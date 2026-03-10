use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: String,
    pub page_ids: Vec<u32>,
    pub payload_len: usize,
}

impl SegmentDescriptor {
    pub fn new(segment_id: impl Into<String>, page_ids: Vec<u32>, payload_len: usize) -> Self {
        Self {
            segment_id: segment_id.into(),
            page_ids,
            payload_len,
        }
    }
}
