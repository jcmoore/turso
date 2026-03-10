use crate::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPageImage {
    pub page_id: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteHeadMetadata {
    pub head_commit_id: Option<String>,
    pub sequence_no: u64,
    pub head_etag: Option<String>,
}

impl RemoteHeadMetadata {
    pub fn new(
        head_commit_id: Option<String>,
        sequence_no: u64,
        head_etag: Option<String>,
    ) -> Self {
        Self {
            head_commit_id,
            sequence_no,
            head_etag,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalCommitState {
    pub database_id: String,
    pub client_id: String,
    pub remote_head: RemoteHeadMetadata,
    pub selector_id: Option<String>,
    pub selector_hash: Option<String>,
    pub selector_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalCommitRequest {
    pub database_id: String,
    pub client_id: String,
    pub transaction_id: String,
    pub base_head: RemoteHeadMetadata,
    pub page_count: u32,
    pub changed_page_ids: Vec<u32>,
    pub page_images: Vec<CommitPageImage>,
    pub selector_id: Option<String>,
    pub selector_hash: Option<String>,
    pub selector_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalCommitOutcome {
    Published { new_head: RemoteHeadMetadata },
    Conflict,
}

pub trait ExternalCommitCoordinator: Send + Sync {
    fn precommit(&self, request: &ExternalCommitRequest) -> Result<ExternalCommitOutcome>;
}
