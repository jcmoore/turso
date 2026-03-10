use serde::{Deserialize, Serialize};

pub const OBJECT_STORE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectorReservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadManifest {
    pub version: u32,
    pub database_id: String,
    pub head_commit_id: String,
    pub sequence_no: u64,
}

impl HeadManifest {
    pub fn new(
        database_id: impl Into<String>,
        head_commit_id: impl Into<String>,
        sequence_no: u64,
    ) -> Self {
        Self {
            version: OBJECT_STORE_SCHEMA_VERSION,
            database_id: database_id.into(),
            head_commit_id: head_commit_id.into(),
            sequence_no,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    pub segment_id: String,
    pub page_ids: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitManifest {
    pub version: u32,
    pub database_id: String,
    pub commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_commit_id: Option<String>,
    pub sequence_no: u64,
    pub page_count: u32,
    pub client_id: String,
    pub transaction_id: String,
    pub segments: Vec<SegmentRef>,
    #[serde(default)]
    pub selector: SelectorReservation,
}

impl CommitManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: impl Into<String>,
        commit_id: impl Into<String>,
        parent_commit_id: Option<String>,
        sequence_no: u64,
        page_count: u32,
        client_id: impl Into<String>,
        transaction_id: impl Into<String>,
        segments: Vec<SegmentRef>,
        selector: SelectorReservation,
    ) -> Self {
        Self {
            version: OBJECT_STORE_SCHEMA_VERSION,
            database_id: database_id.into(),
            commit_id: commit_id.into(),
            parent_commit_id,
            sequence_no,
            page_count,
            client_id: client_id.into(),
            transaction_id: transaction_id.into(),
            segments,
            selector,
        }
    }
}
