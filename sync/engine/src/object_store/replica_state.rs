use std::sync::Arc;

use turso_core::Database;

use super::{
    external_commit::{
        capture_database_external_commit_state, install_object_store_external_commit_coordinator,
        ObjectStoreMetadataEncryption,
    },
    load_metadata, load_or_create_metadata, save_metadata, LocalReplicaMetadata,
};
use crate::Result;

pub struct ObjectStoreReplicaState {
    io: Arc<dyn turso_core::IO>,
    main_db_path: String,
    metadata: LocalReplicaMetadata,
}

impl ObjectStoreReplicaState {
    pub fn open(
        io: Arc<dyn turso_core::IO>,
        main_db_path: impl Into<String>,
        database_id: &str,
        client_id: &str,
    ) -> Result<Self> {
        let main_db_path = main_db_path.into();
        let metadata = load_or_create_metadata(&io, &main_db_path, database_id, client_id)?;
        Ok(Self {
            io,
            main_db_path,
            metadata,
        })
    }

    pub fn metadata(&self) -> &LocalReplicaMetadata {
        &self.metadata
    }

    pub fn main_db_path(&self) -> &str {
        &self.main_db_path
    }

    pub fn install_remote<R>(
        &self,
        database: &Arc<Database>,
        remote: R,
        metadata_encryption: Option<ObjectStoreMetadataEncryption>,
    ) -> Result<()>
    where
        R: super::remote::ObjectStoreRemote + Send + Sync + 'static,
    {
        install_object_store_external_commit_coordinator(
            database,
            self.io.clone(),
            self.main_db_path.clone(),
            remote,
            metadata_encryption,
            &self.metadata,
        )
    }

    pub fn persist(&self) -> Result<()> {
        save_metadata(&self.io, &self.main_db_path, &self.metadata)
    }

    pub fn refresh_from_disk(&mut self) -> Result<bool> {
        let Some(metadata) = load_metadata(&self.io, &self.main_db_path)? else {
            return Ok(false);
        };
        let changed = metadata != self.metadata;
        self.metadata = metadata;
        Ok(changed)
    }

    pub fn capture_and_persist(&mut self, database: &Arc<Database>) -> Result<bool> {
        self.refresh_from_disk()?;
        let changed = capture_database_external_commit_state(database, &mut self.metadata)?;
        if changed {
            self.persist()?;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::ObjectStoreReplicaState;
    use crate::object_store::{
        load_metadata,
        remote::{ObjectStoreRemote, ObjectStoreRemoteError},
    };
    use turso_core::{Database, MemoryIO};

    #[derive(Default)]
    struct RecordingRemote {
        events: Mutex<Vec<String>>,
    }

    impl ObjectStoreRemote for RecordingRemote {
        fn put_immutable(
            &self,
            key: &str,
            _body: &[u8],
        ) -> std::result::Result<(), ObjectStoreRemoteError> {
            self.events.lock().unwrap().push(format!("put:{key}"));
            Ok(())
        }

        fn put_head_if_absent(
            &self,
            _body: &[u8],
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events.lock().unwrap().push("put-head".to_string());
            Ok("etag-1".to_string())
        }

        fn cas_head(
            &self,
            _body: &[u8],
            etag: &str,
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events.lock().unwrap().push(format!("cas:{etag}"));
            Ok("etag-2".to_string())
        }
    }

    #[test]
    fn opens_installs_and_persists_updated_metadata() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let mut state =
            ObjectStoreReplicaState::open(io.clone(), "state.db", "db-1", "client-1").unwrap();
        state
            .install_remote(&db, RecordingRemote::default(), None)
            .unwrap();

        conn.execute("PRAGMA user_version = 123").unwrap();
        assert!(state.capture_and_persist(&db).unwrap());

        let reloaded = load_metadata(&(io as Arc<dyn turso_core::IO>), "state.db")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.head_commit_id.as_deref(), Some("seq-1-client-1-1"));
        assert_eq!(reloaded.sequence_no, 1);
        assert_eq!(reloaded.head_etag.as_deref(), Some("etag-1"));
    }
}
