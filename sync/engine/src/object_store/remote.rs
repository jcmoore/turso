use super::manifest::HeadManifest;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImmutableObject {
    pub key: String,
    pub body: Vec<u8>,
}

impl ImmutableObject {
    pub fn new(key: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            body,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteHead {
    pub manifest: HeadManifest,
    pub etag: String,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ObjectStoreRemoteError {
    #[error("conflict")]
    Conflict,
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("transport error: {0}")]
    Transport(String),
}

pub trait ObjectStoreRemote {
    fn put_immutable(
        &self,
        key: &str,
        body: &[u8],
    ) -> std::result::Result<(), ObjectStoreRemoteError>;
    fn put_head_if_absent(
        &self,
        body: &[u8],
    ) -> std::result::Result<String, ObjectStoreRemoteError>;
    fn cas_head(
        &self,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<String, ObjectStoreRemoteError>;
}
