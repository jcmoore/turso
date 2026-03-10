use super::{
    manifest::HeadManifest,
    remote::{ImmutableObject, ObjectStoreRemote, ObjectStoreRemoteError},
};
use crate::{errors::Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishBaseHead {
    pub head_commit_id: Option<String>,
    pub sequence_no: u64,
    pub head_etag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishRequest {
    pub base: PublishBaseHead,
    pub next_head: HeadManifest,
    pub head_bytes: Vec<u8>,
    pub commit: ImmutableObject,
    pub segments: Vec<ImmutableObject>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishSuccess {
    pub head_commit_id: String,
    pub sequence_no: u64,
    pub head_etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    Published(PublishSuccess),
    Conflict,
}

pub struct ObjectStoreCommitCoordinator<R> {
    remote: R,
}

impl<R> ObjectStoreCommitCoordinator<R> {
    pub fn new(remote: R) -> Self {
        Self { remote }
    }
}

impl<R: ObjectStoreRemote> ObjectStoreCommitCoordinator<R> {
    pub fn publish(&self, request: &PublishRequest) -> Result<PublishOutcome> {
        for segment in &request.segments {
            self.put_immutable(segment)?;
        }

        self.put_immutable(&request.commit)?;

        let next_etag = match &request.base.head_etag {
            Some(etag) => match self.remote.cas_head(&request.head_bytes, etag) {
                Ok(etag) => etag,
                Err(ObjectStoreRemoteError::Conflict) => return Ok(PublishOutcome::Conflict),
                Err(err) => return Err(map_remote_error(err)),
            },
            None => match self.remote.put_head_if_absent(&request.head_bytes) {
                Ok(etag) => etag,
                Err(ObjectStoreRemoteError::Conflict) => return Ok(PublishOutcome::Conflict),
                Err(err) => return Err(map_remote_error(err)),
            },
        };

        Ok(PublishOutcome::Published(PublishSuccess {
            head_commit_id: request.next_head.head_commit_id.clone(),
            sequence_no: request.next_head.sequence_no,
            head_etag: next_etag,
        }))
    }

    fn put_immutable(&self, object: &ImmutableObject) -> Result<()> {
        match self.remote.put_immutable(&object.key, &object.body) {
            Ok(()) | Err(ObjectStoreRemoteError::AlreadyExists(_)) => Ok(()),
            Err(err) => Err(map_remote_error(err)),
        }
    }
}

fn map_remote_error(err: ObjectStoreRemoteError) -> Error {
    match err {
        ObjectStoreRemoteError::Conflict => {
            Error::DatabaseSyncEngineConflict("object-store head conflict".to_string())
        }
        ObjectStoreRemoteError::AlreadyExists(key) => {
            Error::DatabaseSyncEngineError(format!("immutable object already exists: {key}"))
        }
        ObjectStoreRemoteError::NotFound(key) => {
            Error::DatabaseSyncEngineError(format!("object not found: {key}"))
        }
        ObjectStoreRemoteError::Transport(message) => Error::IoError(message),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        HeadManifest, ObjectStoreCommitCoordinator, PublishBaseHead, PublishOutcome, PublishRequest,
    };
    use crate::object_store::remote::{ImmutableObject, ObjectStoreRemote, ObjectStoreRemoteError};

    #[derive(Default)]
    struct RecordingRemote {
        events: Mutex<Vec<String>>,
        fail_cas: bool,
        fail_put_head: bool,
        existing_keys: Vec<String>,
    }

    impl ObjectStoreRemote for RecordingRemote {
        fn put_immutable(
            &self,
            key: &str,
            _body: &[u8],
        ) -> std::result::Result<(), ObjectStoreRemoteError> {
            self.events.lock().unwrap().push(format!("put:{key}"));
            if self.existing_keys.iter().any(|existing| existing == key) {
                return Err(ObjectStoreRemoteError::AlreadyExists(key.to_string()));
            }
            Ok(())
        }

        fn cas_head(
            &self,
            _body: &[u8],
            etag: &str,
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events.lock().unwrap().push(format!("cas:{etag}"));
            if self.fail_cas {
                return Err(ObjectStoreRemoteError::Conflict);
            }
            Ok("next-etag".to_string())
        }

        fn put_head_if_absent(
            &self,
            _body: &[u8],
        ) -> std::result::Result<String, ObjectStoreRemoteError> {
            self.events.lock().unwrap().push("put-head".to_string());
            if self.fail_put_head {
                return Err(ObjectStoreRemoteError::Conflict);
            }
            Ok("next-etag".to_string())
        }
    }

    fn publish_request() -> PublishRequest {
        PublishRequest {
            base: PublishBaseHead {
                head_commit_id: Some("commit-0".to_string()),
                sequence_no: 10,
                head_etag: Some("etag-0".to_string()),
            },
            next_head: HeadManifest::new("db-1", "commit-1", 11),
            head_bytes: br#"{"head":"commit-1"}"#.to_vec(),
            commit: ImmutableObject::new("commits/commit-1", b"commit".to_vec()),
            segments: vec![
                ImmutableObject::new("segments/seg-a", b"aaa".to_vec()),
                ImmutableObject::new("segments/seg-b", b"bbb".to_vec()),
            ],
        }
    }

    #[test]
    fn publishes_segments_then_commit_then_head() {
        let remote = RecordingRemote::default();
        let coordinator = ObjectStoreCommitCoordinator::new(remote);

        let outcome = coordinator.publish(&publish_request()).unwrap();

        assert_eq!(
            outcome,
            PublishOutcome::Published(super::PublishSuccess {
                head_commit_id: "commit-1".to_string(),
                sequence_no: 11,
                head_etag: "next-etag".to_string(),
            })
        );
        assert_eq!(
            coordinator.remote.events.lock().unwrap().as_slice(),
            [
                "put:segments/seg-a",
                "put:segments/seg-b",
                "put:commits/commit-1",
                "cas:etag-0",
            ]
        );
    }

    #[test]
    fn creates_head_when_no_cached_etag_exists() {
        let remote = RecordingRemote::default();
        let coordinator = ObjectStoreCommitCoordinator::new(remote);
        let mut request = publish_request();
        request.base.head_etag = None;
        request.base.head_commit_id = None;
        request.next_head = HeadManifest::new("db-1", "commit-0", 1);
        request.head_bytes = br#"{"head":"commit-0"}"#.to_vec();
        request.commit = ImmutableObject::new("commits/commit-0", b"commit".to_vec());

        let outcome = coordinator.publish(&request).unwrap();

        assert!(matches!(outcome, PublishOutcome::Published(_)));
        assert_eq!(
            coordinator.remote.events.lock().unwrap().as_slice(),
            [
                "put:segments/seg-a",
                "put:segments/seg-b",
                "put:commits/commit-0",
                "put-head",
            ]
        );
    }

    #[test]
    fn ignores_existing_immutable_objects_but_still_cas_updates_head() {
        let remote = RecordingRemote {
            existing_keys: vec!["segments/seg-a".to_string(), "commits/commit-1".to_string()],
            ..Default::default()
        };
        let coordinator = ObjectStoreCommitCoordinator::new(remote);

        let outcome = coordinator.publish(&publish_request()).unwrap();

        assert!(matches!(outcome, PublishOutcome::Published(_)));
        assert_eq!(
            coordinator.remote.events.lock().unwrap().as_slice(),
            [
                "put:segments/seg-a",
                "put:segments/seg-b",
                "put:commits/commit-1",
                "cas:etag-0",
            ]
        );
    }

    #[test]
    fn surfaces_head_cas_conflict_without_retry() {
        let remote = RecordingRemote {
            fail_cas: true,
            ..Default::default()
        };
        let coordinator = ObjectStoreCommitCoordinator::new(remote);

        let outcome = coordinator.publish(&publish_request()).unwrap();

        assert_eq!(outcome, PublishOutcome::Conflict);
        assert_eq!(
            coordinator.remote.events.lock().unwrap().as_slice(),
            [
                "put:segments/seg-a",
                "put:segments/seg-b",
                "put:commits/commit-1",
                "cas:etag-0",
            ]
        );
    }

    #[test]
    fn surfaces_initial_head_create_conflict_without_retry() {
        let remote = RecordingRemote {
            fail_put_head: true,
            ..Default::default()
        };
        let coordinator = ObjectStoreCommitCoordinator::new(remote);
        let mut request = publish_request();
        request.base.head_etag = None;

        let outcome = coordinator.publish(&request).unwrap();

        assert_eq!(outcome, PublishOutcome::Conflict);
        assert_eq!(
            coordinator.remote.events.lock().unwrap().as_slice(),
            [
                "put:segments/seg-a",
                "put:segments/seg-b",
                "put:commits/commit-1",
                "put-head",
            ]
        );
    }
}
