//! Kernel-side helpers for catalog-managed Delta tables in Unity Catalog.

mod committer;
mod constants;
mod errors;
mod utils;

pub use committer::UCCommitter;
use constants::UC_TABLE_ID_KEY;
use delta_kernel::snapshot::{IncrementalSnapshotBuilder, SnapshotBuilder, SnapshotRef};
use delta_kernel::{KernelError, KernelResult, LogPath, Snapshot, Version};
use unity_catalog_delta_client_api::{Commit, LoadTableResponse};
use url::Url;
pub use utils::{
    aws_object_store_options, build_uc_create_table_request, get_required_properties_for_disk,
};

/// Convert the inline `Commit`s returned by a UC `load_table` response into
/// kernel `LogPath`s suitable for [`SnapshotBuilder::with_catalog_metadata`].
///
/// Sorts commits by version because they may arrive out of order. Each commit may name either a
/// staged or an already-published commit file. A published filename asserts that its version and
/// all lower versions are already published.
///
/// # Errors
///
/// Returns an error if a commit's `version` or `file_size` is negative, or if
/// [`LogPath::from_commit_filename`] rejects its filename (including a filename whose version
/// differs from the commit's `version`).
pub fn log_tail_from_commits(commits: &[Commit], table_root: Url) -> KernelResult<Vec<LogPath>> {
    let mut sorted: Vec<&Commit> = commits.iter().collect();
    sorted.sort_by_key(|c| c.version);
    sorted
        .into_iter()
        .map(|c| {
            let version = c.version.try_into().map_err(|_| {
                KernelError::generic(format!("commit version {} is negative", c.version))
            })?;
            let file_size = c.file_size.try_into().map_err(|_| {
                KernelError::generic(format!(
                    "commit file_size {} does not fit in FileSize",
                    c.file_size
                ))
            })?;
            LogPath::from_commit_filename(
                table_root.clone(),
                version,
                &c.file_name,
                c.file_modification_timestamp,
                file_size,
            )
        })
        .collect()
}

/// Build a [`SnapshotBuilder`] from a UC `load_table` response.
///
/// Supplies the catalog's latest ratified version and complete retained commit list through
/// [`SnapshotBuilder::with_catalog_metadata`]. The [UC Delta API] guarantees that managed-table
/// responses contain every retained commit, atomically with the metadata, without truncation.
/// Custom clients must preserve that response contract and must not filter `commits`.
///
/// The connector owns the `load_table` call and the final `build(engine)`. Add `at_version(v)` to
/// the returned builder to time-travel while retaining the complete catalog observation.
///
/// [UC Delta API]: https://github.com/unitycatalog/unitycatalog/blob/d31fa8b47251/api/delta.yaml
///
/// # Errors
///
/// Returns an error if the response's location is not a valid URL, if [`log_tail_from_commits`]
/// fails, or if `latest_table_version` is absent or negative. The metadata-changing version is
/// not a substitute for the latest ratified version.
pub fn snapshot_builder_from_load_table(resp: &LoadTableResponse) -> KernelResult<SnapshotBuilder> {
    let (table_root, max_catalog_version, commits) = catalog_metadata_from_load_table(resp)?;
    Ok(Snapshot::builder_for(table_root).with_catalog_metadata(max_catalog_version, commits))
}

/// Builds an incremental snapshot builder from a complete UC `load_table` response.
///
/// Uses the same response contract as [`snapshot_builder_from_load_table`]. Requires matching
/// table locations and a response table UUID equal to the seed's `io.unitycatalog.tableId`
/// property. The connector must also ensure that the response describes the same committed
/// history; these identity checks cannot prove historical equivalence. Build the returned builder
/// to reconcile retired staged references, including when the latest ratified version has not
/// advanced.
///
/// Returns an error for invalid response metadata, a different table location, or a missing or
/// mismatched UC table ID in the seed.
pub fn incremental_snapshot_builder_from_load_table(
    existing_snapshot: SnapshotRef,
    resp: &LoadTableResponse,
) -> KernelResult<IncrementalSnapshotBuilder> {
    let (table_root, max_catalog_version, commits) = catalog_metadata_from_load_table(resp)?;
    if existing_snapshot.table_root() != &table_root {
        return Err(KernelError::generic(
            "catalog response table location does not match the existing snapshot",
        ));
    }
    let configuration = existing_snapshot
        .table_configuration()
        .metadata()
        .configuration();
    if configuration.get(UC_TABLE_ID_KEY) != Some(&resp.metadata.table_uuid) {
        return Err(KernelError::generic(
            "catalog response table UUID does not match the existing snapshot",
        ));
    }
    Ok(Snapshot::builder_from(existing_snapshot)
        .with_catalog_metadata(max_catalog_version, commits))
}

fn catalog_metadata_from_load_table(
    resp: &LoadTableResponse,
) -> KernelResult<(Url, Version, Vec<LogPath>)> {
    let mut table_root = Url::parse(&resp.metadata.location)
        .map_err(|e| KernelError::generic(format!("invalid table location: {e}")))?;
    if !table_root.path().ends_with('/') {
        table_root.set_path(&format!("{}/", table_root.path()));
    }
    let version = resp
        .latest_table_version
        .ok_or_else(|| KernelError::generic("catalog response is missing latest_table_version"))?;
    let max_catalog_version = version
        .try_into()
        .map_err(|_| KernelError::generic("catalog reported a negative table version"))?;
    let commits = log_tail_from_commits(&resp.commits, table_root.clone())?;
    Ok((table_root, max_catalog_version, commits))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::FileMeta;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use unity_catalog_delta_client_api::{Commit, InMemoryUpdateTableClient, TableData};

    use super::*;

    #[tokio::test]
    async fn load_snapshot_errors_on_non_contiguous_commits() {
        let client = InMemoryUpdateTableClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: 3,
                catalog_commits: vec![
                    Commit::new(
                        1,
                        0,
                        "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ),
                    Commit::new(
                        3,
                        0,
                        "00000000000000000003.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
                        100,
                        0,
                    ), // gap: version 2 missing
                ],
            },
        );
        let resp = client
            .load_table_response("test_table", "memory:///")
            .unwrap();

        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let result = snapshot_builder_from_load_table(&resp)
            .unwrap()
            .build(&engine);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Log tail versions 1 and 3 are not contiguous")
        );
    }

    /// Builds a `LoadTableResponse` with no commits at the given location and ratified version.
    fn load_table_response(location: &str, version: i64) -> LoadTableResponse {
        let client = InMemoryUpdateTableClient::new();
        client.insert_table(
            "test_table",
            TableData {
                max_ratified_version: version,
                catalog_commits: vec![],
            },
        );
        client.load_table_response("test_table", location).unwrap()
    }

    #[test]
    fn snapshot_builder_from_load_table_rejects_negative_version() {
        let resp = load_table_response("memory:///my_table", -1);
        let err = snapshot_builder_from_load_table(&resp).unwrap_err();
        assert!(err.to_string().contains("negative table version"));
    }

    #[test]
    fn snapshot_builder_from_load_table_requires_latest_ratified_version() {
        let mut resp = load_table_response("memory:///my_table", 9);
        resp.latest_table_version = None;
        assert_eq!(resp.metadata.last_commit_version, Some(9));
        let err = snapshot_builder_from_load_table(&resp).unwrap_err();
        assert!(err.to_string().contains("missing latest_table_version"));
    }

    #[test]
    fn snapshot_builder_from_load_table_rejects_invalid_location() {
        let resp = load_table_response("not a url", 0);
        let err = snapshot_builder_from_load_table(&resp).unwrap_err();
        assert!(err.to_string().contains("invalid table location"));
    }

    const UUID: &str = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";

    #[rstest]
    fn log_tail_from_commits_sorts_normalizes_and_resolves_commit_forms(
        #[values("memory:///my_table", "memory:///my_table/")] table_root: &str,
    ) {
        let staged_1 = format!("00000000000000000001.{UUID}.json");
        // Out of order, mixing published and staged names.
        let commits = vec![
            Commit::new(2, 0, "00000000000000000002.json", 100, 0),
            Commit::new(0, 0, "00000000000000000000.json", 100, 0),
            Commit::new(1, 0, &staged_1, 100, 0),
        ];

        let log_tail = log_tail_from_commits(&commits, Url::parse(table_root).unwrap()).unwrap();

        let normalized = Url::parse("memory:///my_table/").unwrap();
        let expected: Vec<LogPath> = [
            "_delta_log/00000000000000000000.json".to_string(),
            format!("_delta_log/_staged_commits/{staged_1}"),
            "_delta_log/00000000000000000002.json".to_string(),
        ]
        .iter()
        .map(|rel_path| {
            LogPath::try_new(FileMeta {
                location: normalized.join(rel_path).unwrap(),
                last_modified: 0,
                size: 100,
            })
            .unwrap()
        })
        .collect();
        assert_eq!(log_tail, expected);
    }

    #[test]
    fn log_tail_from_commits_empty_commits_is_empty() {
        let table_root = Url::parse("memory:///my_table/").unwrap();
        assert!(log_tail_from_commits(&[], table_root).unwrap().is_empty());
    }

    #[rstest]
    #[case::negative_file_size(0, "00000000000000000000.json", -1, false)]
    #[case::negative_version(-1, "00000000000000000000.json", 100, false)]
    #[case::version_mismatch(1, "00000000000000000000.json", 100, true)]
    #[case::not_a_commit(0, "00000000000000000000.crc", 100, true)]
    fn log_tail_from_commits_rejects_invalid_commit(
        #[case] version: i64,
        #[case] file_name: &str,
        #[case] file_size: i64,
        #[case] expected_invalid_log_path_error: bool,
    ) {
        let commits = vec![Commit::new(version, 0, file_name, file_size, 0)];
        let table_root = Url::parse("memory:///my_table/").unwrap();

        let err = log_tail_from_commits(&commits, table_root).unwrap_err();

        let expected_kind = if expected_invalid_log_path_error {
            matches!(err, KernelError::InvalidLogPath(_))
        } else {
            matches!(err, KernelError::Generic(_))
        };
        assert!(expected_kind, "{err:?}");
    }
}
