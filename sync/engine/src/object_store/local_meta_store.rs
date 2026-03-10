use std::sync::Arc;

use turso_core::{io::FileSyncType, Buffer, Completion, File, OpenFlags, IO};

use super::local_meta::LocalReplicaMetadata;
use crate::Result;

pub const LOCAL_REPLICA_METADATA_SUFFIX: &str = "-s3-page-info";

pub fn local_metadata_path(main_db_path: &str) -> String {
    format!("{main_db_path}{LOCAL_REPLICA_METADATA_SUFFIX}")
}

pub fn load_metadata(io: &Arc<dyn IO>, main_db_path: &str) -> Result<Option<LocalReplicaMetadata>> {
    let path = local_metadata_path(main_db_path);
    let file = match io.open_file(&path, OpenFlags::None, false) {
        Ok(file) => file,
        Err(turso_core::LimboError::CompletionError(turso_core::CompletionError::IOError(
            std::io::ErrorKind::NotFound,
            _,
        ))) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let data = read_file(io, file)?;
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(LocalReplicaMetadata::load(&data)?))
}

pub fn load_or_create_metadata(
    io: &Arc<dyn IO>,
    main_db_path: &str,
    database_id: &str,
    client_id: &str,
) -> Result<LocalReplicaMetadata> {
    if let Some(meta) = load_metadata(io, main_db_path)? {
        return Ok(meta);
    }

    let meta = LocalReplicaMetadata::new(database_id, client_id);
    save_metadata(io, main_db_path, &meta)?;
    Ok(meta)
}

pub fn save_metadata(
    io: &Arc<dyn IO>,
    main_db_path: &str,
    metadata: &LocalReplicaMetadata,
) -> Result<()> {
    let path = local_metadata_path(main_db_path);
    let file = io.open_file(&path, OpenFlags::Create, false)?;
    let data = metadata.to_bytes()?;
    write_file(io, file, data)
}

fn read_file(io: &Arc<dyn IO>, file: Arc<dyn File>) -> Result<Vec<u8>> {
    let size = file.size()? as usize;
    if size == 0 {
        return Ok(Vec::new());
    }

    let buffer = Arc::new(Buffer::new_temporary(size));
    let read_len = Arc::new(std::sync::Mutex::new(0usize));
    let completion = Completion::new_read(buffer.clone(), {
        let read_len = read_len.clone();
        move |result| {
            if let Ok((_, len)) = result {
                *read_len.lock().unwrap() = len as usize;
            }
            None
        }
    });
    let completion = file.pread(0, completion)?;
    io.wait_for_completion(completion)?;
    let read_len = *read_len.lock().unwrap();
    Ok(buffer.as_slice()[0..read_len].to_vec())
}

fn write_file(io: &Arc<dyn IO>, file: Arc<dyn File>, data: Vec<u8>) -> Result<()> {
    let truncate = file.truncate(0, Completion::new_trunc(|_| {}))?;
    io.wait_for_completion(truncate)?;

    let write = file.pwrite(
        0,
        Arc::new(Buffer::new(data)),
        Completion::new_write(|_| {}),
    )?;
    io.wait_for_completion(write)?;

    let sync = file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
    io.wait_for_completion(sync)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{load_metadata, load_or_create_metadata, local_metadata_path, save_metadata};
    use crate::object_store::local_meta::LocalReplicaMetadata;
    use turso_core::{MemoryIO, IO};

    #[test]
    fn creates_and_round_trips_local_metadata_sidecar() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let path = "demo.db";

        let mut meta = load_or_create_metadata(&io, path, "db-1", "client-1").unwrap();
        assert_eq!(meta.database_id, "db-1");
        assert_eq!(local_metadata_path(path), "demo.db-s3-page-info");

        meta.head_commit_id = Some("commit-1".to_string());
        meta.sequence_no = 1;
        save_metadata(&io, path, &meta).unwrap();

        let loaded = load_metadata(&io, path).unwrap().unwrap();
        assert_eq!(loaded.head_commit_id.as_deref(), Some("commit-1"));
        assert_eq!(loaded.sequence_no, 1);
    }

    #[test]
    fn load_metadata_returns_none_for_missing_sidecar() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());

        assert_eq!(load_metadata(&io, "missing.db").unwrap(), None);
    }

    #[test]
    fn save_metadata_overwrites_existing_payload() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let path = "overwrite.db";

        save_metadata(&io, path, &LocalReplicaMetadata::new("db-1", "client-1")).unwrap();
        let mut updated = LocalReplicaMetadata::new("db-1", "client-1");
        updated.head_commit_id = Some("commit-2".to_string());
        updated.sequence_no = 2;
        save_metadata(&io, path, &updated).unwrap();

        let loaded = load_metadata(&io, path).unwrap().unwrap();
        assert_eq!(loaded.head_commit_id.as_deref(), Some("commit-2"));
        assert_eq!(loaded.sequence_no, 2);
    }
}
