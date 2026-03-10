use serde::{Deserialize, Serialize};
use turso_core::commit_coordinator::{ExternalCommitState, RemoteHeadMetadata};

use crate::{errors::Error, Result};

pub const LOCAL_REPLICA_METADATA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalReplicaMetadata {
    pub version: u32,
    pub database_id: String,
    pub client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<String>,
    pub sequence_no: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pulled_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_publish_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_version: Option<String>,
}

impl LocalReplicaMetadata {
    pub fn new(database_id: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            version: LOCAL_REPLICA_METADATA_VERSION,
            database_id: database_id.into(),
            client_id: client_id.into(),
            head_commit_id: None,
            sequence_no: 0,
            head_etag: None,
            last_pulled_at: None,
            pending_publish_commit_id: None,
            selector_id: None,
            selector_hash: None,
            selector_version: None,
        }
    }

    pub fn load(data: &[u8]) -> Result<Self> {
        let meta = serde_json::from_slice::<Self>(data).map_err(|err| {
            Error::JsonDecode(format!(
                "unable to parse object-store local metadata: {err}"
            ))
        })?;
        Ok(meta)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn to_external_commit_state(&self) -> ExternalCommitState {
        ExternalCommitState {
            database_id: self.database_id.clone(),
            client_id: self.client_id.clone(),
            remote_head: RemoteHeadMetadata::new(
                self.head_commit_id.clone(),
                self.sequence_no,
                self.head_etag.clone(),
            ),
            selector_id: self.selector_id.clone(),
            selector_hash: self.selector_hash.clone(),
            selector_version: self.selector_version.clone(),
        }
    }

    pub fn apply_published_head(&mut self, head: &RemoteHeadMetadata) {
        self.head_commit_id = head.head_commit_id.clone();
        self.sequence_no = head.sequence_no;
        self.head_etag = head.head_etag.clone();
        self.pending_publish_commit_id = None;
    }

    pub fn apply_external_commit_state(&mut self, state: &ExternalCommitState) {
        self.database_id = state.database_id.clone();
        self.client_id = state.client_id.clone();
        self.selector_id = state.selector_id.clone();
        self.selector_hash = state.selector_hash.clone();
        self.selector_version = state.selector_version.clone();
        self.head_commit_id = state.remote_head.head_commit_id.clone();
        self.sequence_no = state.remote_head.sequence_no;
        self.head_etag = state.remote_head.head_etag.clone();
    }

    pub fn begin_pending_publish(&mut self, commit_id: impl Into<String>) {
        self.pending_publish_commit_id = Some(commit_id.into());
    }

    pub fn clear_pending_publish(&mut self) {
        self.pending_publish_commit_id = None;
    }

    pub fn finalize_external_commit_state(&mut self, state: &ExternalCommitState) {
        self.apply_external_commit_state(state);
        self.clear_pending_publish();
    }
}

#[cfg(test)]
mod tests {
    use super::LocalReplicaMetadata;
    use turso_core::commit_coordinator::RemoteHeadMetadata;

    #[test]
    fn preserves_reserved_selector_fields() {
        let mut meta = LocalReplicaMetadata::new("db-1", "client-1");
        meta.selector_id = Some("tenant/main".to_string());
        meta.selector_hash = Some("hash-1".to_string());
        meta.selector_version = Some("v1".to_string());
        meta.pending_publish_commit_id = Some("commit-42".to_string());

        let bytes = meta.to_bytes().unwrap();
        let decoded = LocalReplicaMetadata::load(&bytes).unwrap();

        assert_eq!(decoded.selector_id.as_deref(), Some("tenant/main"));
        assert_eq!(decoded.selector_hash.as_deref(), Some("hash-1"));
        assert_eq!(decoded.selector_version.as_deref(), Some("v1"));
        assert_eq!(
            decoded.pending_publish_commit_id.as_deref(),
            Some("commit-42")
        );
    }

    #[test]
    fn round_trips_through_external_commit_state_and_published_head() {
        let mut meta = LocalReplicaMetadata::new("db-1", "client-1");
        meta.head_commit_id = Some("commit-0".to_string());
        meta.sequence_no = 7;
        meta.head_etag = Some("etag-0".to_string());
        meta.selector_id = Some("tenant/main".to_string());

        let state = meta.to_external_commit_state();
        assert_eq!(state.database_id, "db-1");
        assert_eq!(state.remote_head.sequence_no, 7);
        assert_eq!(state.selector_id.as_deref(), Some("tenant/main"));

        meta.pending_publish_commit_id = Some("commit-1".to_string());
        meta.apply_published_head(&RemoteHeadMetadata::new(
            Some("commit-1".to_string()),
            8,
            Some("etag-1".to_string()),
        ));
        assert_eq!(meta.head_commit_id.as_deref(), Some("commit-1"));
        assert_eq!(meta.sequence_no, 8);
        assert_eq!(meta.head_etag.as_deref(), Some("etag-1"));
        assert_eq!(meta.pending_publish_commit_id, None);

        meta.apply_external_commit_state(&state);
        assert_eq!(meta.head_commit_id.as_deref(), Some("commit-0"));
        assert_eq!(meta.sequence_no, 7);
    }
}
