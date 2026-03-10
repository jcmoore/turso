use std::sync::{Arc, Mutex};

use turso_core::{
    commit_coordinator::{
        CommitPageImage, ExternalCommitCoordinator, ExternalCommitOutcome, ExternalCommitRequest,
        RemoteHeadMetadata,
    },
    CipherMode, Database, EncryptionContext, EncryptionKey,
};

use super::{
    commit_coordinator::{
        ObjectStoreCommitCoordinator, PublishBaseHead, PublishOutcome, PublishRequest,
    },
    local_meta::LocalReplicaMetadata,
    local_meta_store::save_metadata,
    manifest::{CommitManifest, HeadManifest, SegmentRef, SelectorReservation},
    remote::{ImmutableObject, ObjectStoreRemote},
};
use crate::{errors::Error, Result};

const SEGMENT_ENCODING_VERSION: u32 = 1;
const SEGMENT_MAGIC: &[u8; 4] = b"TS3P";

pub struct ObjectStoreExternalCommitCoordinator<R> {
    publisher: ObjectStoreCommitCoordinator<R>,
    metadata_encryption: Option<ObjectStoreMetadataEncryption>,
    pending_publish: Option<PendingPublishState>,
}

#[derive(Clone)]
pub struct ObjectStoreMetadataEncryption {
    cipher_mode: CipherMode,
    key: EncryptionKey,
}

struct PendingPublishState {
    io: Arc<dyn turso_core::IO>,
    main_db_path: String,
    metadata: Mutex<LocalReplicaMetadata>,
}

impl<R> ObjectStoreExternalCommitCoordinator<R> {
    pub fn new(remote: R) -> Self {
        Self {
            publisher: ObjectStoreCommitCoordinator::new(remote),
            metadata_encryption: None,
            pending_publish: None,
        }
    }

    pub fn with_pending_publish_state(
        remote: R,
        metadata_encryption: Option<ObjectStoreMetadataEncryption>,
        io: Arc<dyn turso_core::IO>,
        main_db_path: impl Into<String>,
        metadata: LocalReplicaMetadata,
    ) -> Self {
        Self {
            publisher: ObjectStoreCommitCoordinator::new(remote),
            metadata_encryption,
            pending_publish: Some(PendingPublishState {
                io,
                main_db_path: main_db_path.into(),
                metadata: Mutex::new(metadata),
            }),
        }
    }
}

impl ObjectStoreMetadataEncryption {
    pub fn aegis256(hexkey: &str) -> Result<Self> {
        Ok(Self {
            cipher_mode: CipherMode::Aegis256,
            key: EncryptionKey::from_hex_string(hexkey)?,
        })
    }

    fn encrypt(&self, key_aad: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
        let ctx = EncryptionContext::new(self.cipher_mode, &self.key, 0)?;
        ctx.encrypt_metadata_blob(plaintext, key_aad.as_bytes())
            .map_err(Error::from)
    }
}

impl PendingPublishState {
    fn begin(&self, commit_id: &str) -> Result<()> {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.begin_pending_publish(commit_id.to_string());
        save_metadata(&self.io, &self.main_db_path, &metadata)
    }

    fn clear(&self) -> Result<()> {
        let mut metadata = self.metadata.lock().unwrap();
        if metadata.pending_publish_commit_id.is_none() {
            return Ok(());
        }
        metadata.clear_pending_publish();
        save_metadata(&self.io, &self.main_db_path, &metadata)
    }
}

impl<R> ExternalCommitCoordinator for ObjectStoreExternalCommitCoordinator<R>
where
    R: ObjectStoreRemote + Send + Sync + 'static,
{
    fn precommit(
        &self,
        request: &ExternalCommitRequest,
    ) -> turso_core::Result<ExternalCommitOutcome> {
        let mut publish_request = build_publish_request(request)
            .map_err(|err| turso_core::LimboError::InternalError(err.to_string()))?;
        if let Some(metadata_encryption) = &self.metadata_encryption {
            publish_request.head_bytes = metadata_encryption
                .encrypt("refs/head", &publish_request.head_bytes)
                .map_err(|err| turso_core::LimboError::InternalError(err.to_string()))?;
            publish_request.commit.body = metadata_encryption
                .encrypt(&publish_request.commit.key, &publish_request.commit.body)
                .map_err(|err| turso_core::LimboError::InternalError(err.to_string()))?;
        }
        if let Some(pending_publish) = &self.pending_publish {
            pending_publish
                .begin(&publish_request.next_head.head_commit_id)
                .map_err(|err| turso_core::LimboError::InternalError(err.to_string()))?;
        }
        let outcome = self
            .publisher
            .publish(&publish_request)
            .map_err(|err| {
                if let Some(pending_publish) = &self.pending_publish {
                    let _ = pending_publish.clear();
                }
                turso_core::LimboError::InternalError(err.to_string())
            })?;

        Ok(match outcome {
            PublishOutcome::Published(success) => ExternalCommitOutcome::Published {
                new_head: RemoteHeadMetadata::new(
                    Some(success.head_commit_id),
                    success.sequence_no,
                    Some(success.head_etag),
                ),
            },
            PublishOutcome::Conflict => {
                if let Some(pending_publish) = &self.pending_publish {
                    pending_publish
                        .clear()
                        .map_err(|err| turso_core::LimboError::InternalError(err.to_string()))?;
                }
                ExternalCommitOutcome::Conflict
            }
        })
    }
}

pub fn install_object_store_external_commit_coordinator<R>(
    database: &Arc<Database>,
    io: Arc<dyn turso_core::IO>,
    main_db_path: impl Into<String>,
    remote: R,
    metadata_encryption: Option<ObjectStoreMetadataEncryption>,
    metadata: &LocalReplicaMetadata,
) -> Result<()>
where
    R: ObjectStoreRemote + Send + Sync + 'static,
{
    database
        .install_external_commit_coordinator(
            Arc::new(ObjectStoreExternalCommitCoordinator::with_pending_publish_state(
                remote,
                metadata_encryption,
                io,
                main_db_path,
                metadata.clone(),
            )),
            metadata.to_external_commit_state(),
        )
        .map_err(Error::from)
}

pub fn capture_database_external_commit_state(
    database: &Arc<Database>,
    metadata: &mut LocalReplicaMetadata,
) -> Result<bool> {
    let state = database.external_commit_state().map_err(Error::from)?;
    let Some(state) = state else {
        return Ok(false);
    };
    let previous = metadata.clone();
    metadata.finalize_external_commit_state(&state);
    Ok(previous != *metadata)
}

pub fn build_publish_request(request: &ExternalCommitRequest) -> Result<PublishRequest> {
    let sorted_page_images = sorted_page_images(&request.page_images);
    let changed_page_ids: Vec<u32> = sorted_page_images.iter().map(|page| page.page_id).collect();
    if changed_page_ids != sorted_page_ids(&request.changed_page_ids) {
        return Err(Error::DatabaseSyncEngineError(
            "page_images and changed_page_ids are inconsistent".to_string(),
        ));
    }

    let next_sequence_no = request.base_head.sequence_no + 1;
    let commit_id = format!(
        "seq-{}-{}-{}",
        next_sequence_no,
        sanitize_key_component(&request.client_id),
        sanitize_key_component(&request.transaction_id)
    );
    let segment_id = format!("{commit_id}-segment-0");
    let segment_payload = encode_segment_pages(&sorted_page_images)?;
    let segment_ref = SegmentRef {
        segment_id: segment_id.clone(),
        page_ids: changed_page_ids.clone(),
    };
    let selector = SelectorReservation {
        selector_id: request.selector_id.clone(),
        selector_hash: request.selector_hash.clone(),
        selector_version: request.selector_version.clone(),
    };
    let commit_manifest = CommitManifest::new(
        &request.database_id,
        &commit_id,
        request.base_head.head_commit_id.clone(),
        next_sequence_no,
        request.page_count,
        &request.client_id,
        &request.transaction_id,
        vec![segment_ref],
        selector,
    );
    let head_manifest = HeadManifest::new(&request.database_id, &commit_id, next_sequence_no);

    Ok(PublishRequest {
        base: PublishBaseHead {
            head_commit_id: request.base_head.head_commit_id.clone(),
            sequence_no: request.base_head.sequence_no,
            head_etag: request.base_head.head_etag.clone(),
        },
        next_head: head_manifest.clone(),
        head_bytes: serde_json::to_vec(&head_manifest)?,
        commit: ImmutableObject::new(
            format!("commits/{commit_id}"),
            serde_json::to_vec(&commit_manifest)?,
        ),
        segments: vec![ImmutableObject::new(
            format!("segments/{segment_id}"),
            segment_payload,
        )],
    })
}

pub fn encode_segment_pages(page_images: &[CommitPageImage]) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(SEGMENT_MAGIC);
    payload.extend_from_slice(&SEGMENT_ENCODING_VERSION.to_be_bytes());
    payload.extend_from_slice(&(page_images.len() as u32).to_be_bytes());
    for page in page_images {
        payload.extend_from_slice(&page.page_id.to_be_bytes());
        payload.extend_from_slice(&(page.bytes.len() as u32).to_be_bytes());
        payload.extend_from_slice(&page.bytes);
    }
    Ok(payload)
}

fn sorted_page_images(page_images: &[CommitPageImage]) -> Vec<CommitPageImage> {
    let mut sorted = page_images.to_vec();
    sorted.sort_by_key(|page| page.page_id);
    sorted
}

fn sorted_page_ids(page_ids: &[u32]) -> Vec<u32> {
    let mut sorted = page_ids.to_vec();
    sorted.sort_unstable();
    sorted
}

fn sanitize_key_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use turso_core::commit_coordinator::CommitPageImage;

    use super::{
        build_publish_request, capture_database_external_commit_state, encode_segment_pages,
        install_object_store_external_commit_coordinator, ObjectStoreExternalCommitCoordinator,
    };
    use crate::object_store::{
        local_meta::LocalReplicaMetadata,
        local_meta_store::{load_metadata, save_metadata},
        remote::{ObjectStoreRemote, ObjectStoreRemoteError},
    };
    use turso_core::{
        commit_coordinator::{
            ExternalCommitCoordinator, ExternalCommitRequest, RemoteHeadMetadata,
        },
        Database, MemoryIO,
    };

    #[derive(Default)]
    struct RecordingRemote {
        events: Mutex<Vec<String>>,
        head_conflict: bool,
    }

    impl ObjectStoreRemote for RecordingRemote {
        fn put_immutable(
            &self,
            key: &str,
            body: &[u8],
        ) -> std::result::Result<(), ObjectStoreRemoteError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("put:{key}:{}", body.len()));
            Ok(())
        }

        fn put_head_if_absent(
            &self,
            body: &[u8],
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("put-head:{}", body.len()));
            if self.head_conflict {
                return Err(ObjectStoreRemoteError::Conflict);
            }
            Ok("etag-1".to_string())
        }

        fn cas_head(
            &self,
            body: &[u8],
            etag: &str,
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("cas:{etag}:{}", body.len()));
            if self.head_conflict {
                return Err(ObjectStoreRemoteError::Conflict);
            }
            Ok("etag-2".to_string())
        }
    }

    fn request() -> ExternalCommitRequest {
        ExternalCommitRequest {
            database_id: "db-1".to_string(),
            client_id: "client/a".to_string(),
            transaction_id: "tx/7".to_string(),
            base_head: RemoteHeadMetadata::new(None, 0, None),
            page_count: 4,
            changed_page_ids: vec![2, 1],
            page_images: vec![
                CommitPageImage {
                    page_id: 2,
                    bytes: vec![2, 2],
                },
                CommitPageImage {
                    page_id: 1,
                    bytes: vec![1, 1, 1],
                },
            ],
            selector_id: Some("tenant/main".to_string()),
            selector_hash: Some("hash-1".to_string()),
            selector_version: Some("v1".to_string()),
        }
    }

    #[test]
    fn builds_publish_request_for_initial_head_create() {
        let publish = build_publish_request(&request()).unwrap();

        assert_eq!(publish.base.head_etag, None);
        assert_eq!(publish.next_head.sequence_no, 1);
        assert_eq!(publish.next_head.head_commit_id, "seq-1-client_a-tx_7");
        assert_eq!(publish.commit.key, "commits/seq-1-client_a-tx_7");
        assert_eq!(
            publish.segments[0].key,
            "segments/seq-1-client_a-tx_7-segment-0"
        );
    }

    #[test]
    fn encodes_segment_pages_in_sorted_page_order() {
        let payload = encode_segment_pages(&request().page_images).unwrap();

        assert_eq!(&payload[0..4], b"TS3P");
        assert_eq!(u32::from_be_bytes(payload[4..8].try_into().unwrap()), 1);
        assert_eq!(u32::from_be_bytes(payload[8..12].try_into().unwrap()), 2);
    }

    #[test]
    fn adapter_maps_publish_success_to_new_head_metadata() {
        let remote = RecordingRemote::default();
        let coordinator = ObjectStoreExternalCommitCoordinator::new(remote);

        let outcome = coordinator.precommit(&request()).unwrap();

        assert_eq!(
            outcome,
            turso_core::commit_coordinator::ExternalCommitOutcome::Published {
                new_head: RemoteHeadMetadata::new(
                    Some("seq-1-client_a-tx_7".to_string()),
                    1,
                    Some("etag-1".to_string()),
                ),
            }
        );
    }

    #[test]
    fn install_helper_pushes_state_into_database() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let meta = LocalReplicaMetadata::new("db-1", "client-1");
        install_object_store_external_commit_coordinator(
            &db,
            io.clone(),
            "demo.db",
            RecordingRemote::default(),
            None,
            &meta,
        )
        .unwrap();

        let state = db.external_commit_state().unwrap().unwrap();
        assert_eq!(state.database_id, "db-1");
        assert_eq!(state.client_id, "client-1");
        assert_eq!(state.remote_head.sequence_no, 0);
    }

    #[test]
    fn capture_helper_persists_updated_head_after_commit() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let mut meta = LocalReplicaMetadata::new("db-1", "client-1");
        save_metadata(&(io.clone() as Arc<dyn turso_core::IO>), "demo.db", &meta).unwrap();
        install_object_store_external_commit_coordinator(
            &db,
            io.clone(),
            "demo.db",
            RecordingRemote::default(),
            None,
            &meta,
        )
        .unwrap();

        conn.execute("PRAGMA user_version = 55").unwrap();
        assert!(capture_database_external_commit_state(&db, &mut meta).unwrap());
        save_metadata(&(io.clone() as Arc<dyn turso_core::IO>), "demo.db", &meta).unwrap();

        let persisted = load_metadata(&(io as Arc<dyn turso_core::IO>), "demo.db")
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.head_commit_id.as_deref(),
            Some("seq-1-client-1-1")
        );
        assert_eq!(persisted.sequence_no, 1);
        assert_eq!(persisted.head_etag.as_deref(), Some("etag-1"));
    }

    #[test]
    fn precommit_persists_pending_publish_until_capture_clears_it() {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:").unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

        let mut meta = LocalReplicaMetadata::new("db-1", "client-1");
        save_metadata(&(io.clone() as Arc<dyn turso_core::IO>), "demo.db", &meta).unwrap();
        install_object_store_external_commit_coordinator(
            &db,
            io.clone(),
            "demo.db",
            RecordingRemote::default(),
            None,
            &meta,
        )
        .unwrap();

        conn.execute("PRAGMA user_version = 77").unwrap();

        let pending = load_metadata(&(io.clone() as Arc<dyn turso_core::IO>), "demo.db")
            .unwrap()
            .unwrap();
        assert_eq!(
            pending.pending_publish_commit_id.as_deref(),
            Some("seq-1-client-1-1")
        );
        assert_eq!(pending.head_commit_id, None);

        assert!(capture_database_external_commit_state(&db, &mut meta).unwrap());
        save_metadata(&(io.clone() as Arc<dyn turso_core::IO>), "demo.db", &meta).unwrap();

        let persisted = load_metadata(&(io as Arc<dyn turso_core::IO>), "demo.db")
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.head_commit_id.as_deref(),
            Some("seq-1-client-1-1")
        );
        assert_eq!(persisted.pending_publish_commit_id, None);
    }
}
