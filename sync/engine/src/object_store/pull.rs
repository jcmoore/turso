use std::collections::{HashMap, HashSet};

use super::manifest::CommitManifest;
use crate::{errors::Error, Result};

pub fn collect_missing_commits<'a>(
    local_head_commit_id: Option<&str>,
    remote_head_commit_id: &'a str,
    manifests: &'a HashMap<String, CommitManifest>,
) -> Result<Vec<&'a CommitManifest>> {
    let mut missing = Vec::new();
    let mut seen = HashSet::new();
    let mut current = Some(remote_head_commit_id);

    while let Some(commit_id) = current {
        if Some(commit_id) == local_head_commit_id {
            break;
        }
        if !seen.insert(commit_id.to_string()) {
            return Err(Error::DatabaseSyncEngineError(format!(
                "cycle detected in object-store commit history at {commit_id}"
            )));
        }

        let manifest = manifests.get(commit_id).ok_or_else(|| {
            Error::DatabaseSyncEngineError(format!(
                "missing object-store manifest for commit {commit_id}"
            ))
        })?;
        missing.push(manifest);
        current = manifest.parent_commit_id.as_deref();
    }

    if current.is_none() && local_head_commit_id.is_some() {
        return Err(Error::DatabaseSyncEngineConflict(
            "local head is not an ancestor of remote head".to_string(),
        ));
    }

    missing.reverse();
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::collect_missing_commits;
    use crate::object_store::manifest::{CommitManifest, SegmentRef, SelectorReservation};

    fn manifest(commit_id: &str, parent_commit_id: Option<&str>) -> CommitManifest {
        CommitManifest::new(
            "db-1",
            commit_id,
            parent_commit_id.map(str::to_string),
            1,
            64,
            "client-1",
            "tx-1",
            vec![SegmentRef {
                segment_id: format!("segment-{commit_id}"),
                page_ids: vec![1, 2],
            }],
            SelectorReservation::default(),
        )
    }

    #[test]
    fn returns_missing_commits_oldest_to_newest() {
        let mut manifests = HashMap::new();
        manifests.insert("c1".to_string(), manifest("c1", None));
        manifests.insert("c2".to_string(), manifest("c2", Some("c1")));
        manifests.insert("c3".to_string(), manifest("c3", Some("c2")));

        let missing = collect_missing_commits(Some("c1"), "c3", &manifests).unwrap();
        let ids: Vec<_> = missing
            .iter()
            .map(|manifest| manifest.commit_id.as_str())
            .collect();

        assert_eq!(ids, vec!["c2", "c3"]);
    }

    #[test]
    fn rejects_non_ancestor_local_head() {
        let mut manifests = HashMap::new();
        manifests.insert("c1".to_string(), manifest("c1", None));
        manifests.insert("c2".to_string(), manifest("c2", Some("c1")));

        let err = collect_missing_commits(Some("other"), "c2", &manifests).unwrap_err();

        assert!(err
            .to_string()
            .contains("local head is not an ancestor of remote head"));
    }
}
