//! Complete catalog observations and retained commit-reference reconciliation.

use std::sync::Arc;

use rstest::rstest;
use serde_json::json;
use test_utils::{
    TestAction, actions_to_string_catalog_managed, add_commit, add_staged_commit,
    delta_path_for_version,
};
use url::Url;

use super::builder::tests::setup_test;
use super::{Snapshot, SnapshotHint, SnapshotRef};
use crate::crc::Crc;
use crate::engine::sync::SyncEngine;
use crate::log_path::LogPath;
use crate::object_store::path::Path;
use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::{
    Engine, EvaluationHandler, FileMeta, JsonHandler, KernelError, KernelResult, ParquetHandler,
    StorageHandler, Version,
};

const ICT_BASE: i64 = 1_587_968_586_154;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Published commits precede `staged_start`; staged commits each carry a distinct transaction.
struct CatalogHistory {
    engine: Arc<SyncEngine>,
    store: Arc<DynObjectStore>,
    table_root: String,
    staged: Vec<LogPath>,
    staged_paths: Vec<Path>,
    staged_start: Version,
    max_version: Version,
}

impl CatalogHistory {
    async fn new(staged_start: Version, max_version: Version) -> TestResult<Self> {
        let (engine, store, table_root) = setup_test();
        let mut staged = Vec::new();
        let mut staged_paths = Vec::new();
        for version in 0..=max_version {
            let mut actions = Vec::new();
            if version == 0 {
                actions.push(actions_to_string_catalog_managed(vec![
                    TestAction::Metadata,
                ]));
            } else {
                actions.push(
                    json!({"commitInfo": {
                        "inCommitTimestamp": ICT_BASE + version as i64,
                        "txnId": format!("txn-{version}"), "operation": "WRITE",
                    }})
                    .to_string(),
                );
            }
            actions.push(
                json!({"txn": {
                    "appId": format!("app-{version}"), "version": version,
                }})
                .to_string(),
            );
            let actions = actions.join("\n");
            if version < staged_start {
                add_commit(&table_root, store.as_ref(), version, actions).await?;
            } else {
                let path = add_staged_commit(&table_root, store.as_ref(), version, actions).await?;
                let metadata = store.head(&path).await?;
                staged.push(LogPath::try_new(FileMeta {
                    location: Url::parse(&table_root)?.join(path.as_ref())?,
                    last_modified: metadata.last_modified.timestamp_millis(),
                    size: metadata.size,
                })?);
                staged_paths.push(path);
            }
        }
        Ok(Self {
            engine,
            store,
            table_root,
            staged,
            staged_paths,
            staged_start,
            max_version,
        })
    }

    fn commits_from(&self, version: Version) -> Vec<LogPath> {
        self.staged[(version - self.staged_start) as usize..].to_vec()
    }

    fn build_at(&self, version: Version) -> KernelResult<SnapshotRef> {
        Snapshot::builder_for(&self.table_root)
            .with_catalog_metadata(self.max_version, self.staged.clone())
            .at_version(version)
            .build(self.engine.as_ref())
    }

    async fn publish(&self, version: Version) -> TestResult {
        self.store
            .copy(
                &self.staged_paths[(version - self.staged_start) as usize],
                &delta_path_for_version(version, "json"),
            )
            .await?;
        Ok(())
    }

    async fn delete_staged(&self, version: Version) -> TestResult {
        self.store
            .delete(&self.staged_paths[(version - self.staged_start) as usize])
            .await?;
        Ok(())
    }
}

/// Any request for an engine handler fails, including metadata probes during path-only repair.
struct NoIoEngine;

impl Engine for NoIoEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        panic!("catalog reference repair requested an evaluation handler")
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        panic!("catalog reference repair requested a storage handler")
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        panic!("catalog reference repair requested a JSON handler")
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        panic!("catalog reference repair requested a Parquet handler")
    }
}

fn selected_commit(snapshot: &Snapshot, version: Version) -> &ParsedLogPath {
    snapshot
        .log_segment
        .listed
        .ascending_commit_files
        .iter()
        .find(|path| path.version == version)
        .unwrap()
}

#[rstest]
#[case::same_version(10, 9)]
#[case::overlapping_tail(13, 10)]
#[case::disjoint_tail(17, 15)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_catalog_repairs_cached_prefix_and_replays_after_staged_deletion(
    #[case] target: Version,
    #[case] first_retained: Version,
    #[values(false, true)] skip_new_checkpoints: bool,
    #[values(None, Some(0), Some(5))] checkpoint: Option<Version>,
) -> TestResult {
    let history = CatalogHistory::new(6, target).await?;
    if let Some(version) = checkpoint {
        history
            .build_at(version)?
            .checkpoint(history.engine.as_ref(), None)?;
    }
    let seed = history.build_at(10)?;
    for version in 6..first_retained {
        history.publish(version).await?;
    }
    let builder = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(target, history.commits_from(first_retained));
    let builder = if skip_new_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    let updated = if target == seed.version() {
        builder.build(&NoIoEngine)?
    } else {
        builder.build(history.engine.as_ref())?
    };
    assert_eq!(updated.version(), target);
    assert!(!Arc::ptr_eq(&seed, &updated));
    for version in 6..=target {
        assert_eq!(
            selected_commit(&updated, version).file_type,
            if version < first_retained {
                LogPathFileType::Commit
            } else {
                LogPathFileType::StagedCommit
            }
        );
    }
    assert_eq!(
        selected_commit(&seed, 6).file_type,
        LogPathFileType::StagedCommit
    );
    for version in 6..first_retained {
        history.delete_staged(version).await?;
    }
    assert_eq!(
        updated
            .reader()
            .get_app_id_version("app-6", history.engine.as_ref())?,
        Some(6)
    );
    assert_eq!(
        updated.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE + target as i64
    );
    Ok(())
}

#[rstest]
#[case::version_zero(0)]
#[case::latest_commit(2)]
#[tokio::test]
async fn complete_empty_catalog_repairs_latest_commit_without_io(
    #[case] target: Version,
) -> TestResult {
    let history = CatalogHistory::new(0, target).await?;
    let seed = history.build_at(target)?;
    let staged_latest = seed.log_segment.listed.latest_commit_file.clone().unwrap();
    for version in 0..=target {
        history.publish(version).await?;
    }
    let updated = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(target, Vec::new())
        .build(&NoIoEngine)?;
    let latest = updated
        .log_segment
        .listed
        .latest_commit_file
        .as_ref()
        .unwrap();
    assert_eq!(latest.file_type, LogPathFileType::Commit);
    assert_eq!(latest.location.size, staged_latest.location.size);
    assert_eq!(
        latest.location.last_modified,
        staged_latest.location.last_modified
    );
    assert_eq!(latest.filename, format!("{target:020}.json"));
    assert_eq!(latest.extension, "json");
    assert_eq!(
        updated.log_segment.listed.max_published_version,
        Some(target)
    );
    for version in 0..=target {
        history.delete_staged(version).await?;
    }
    assert_eq!(
        updated
            .reader()
            .get_app_id_version("app-0", history.engine.as_ref())?,
        Some(0)
    );
    assert_eq!(
        updated.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE + target as i64
    );
    assert_eq!(
        seed.log_segment
            .listed
            .latest_commit_file
            .as_ref()
            .unwrap()
            .file_type,
        LogPathFileType::StagedCommit
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_empty_catalog_repairs_latest_only_reference() -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    history.publish(0).await?;
    history
        .build_at(0)?
        .checkpoint(history.engine.as_ref(), None)?;
    let seed = history.build_at(0)?;
    assert!(seed.log_segment.listed.ascending_commit_files.is_empty());
    assert_eq!(
        seed.log_segment
            .listed
            .latest_commit_file
            .as_ref()
            .unwrap()
            .file_type,
        LogPathFileType::StagedCommit
    );
    let updated = Snapshot::builder_from(seed)
        .with_catalog_metadata(0, Vec::new())
        .build(&NoIoEngine)?;
    history.delete_staged(0).await?;
    assert_eq!(
        updated.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE
    );
    assert_eq!(
        updated
            .reader()
            .get_app_id_version("app-0", history.engine.as_ref())?,
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn catalog_retains_staged_authority_over_published_watermark() -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    for version in 0..=2 {
        history.publish(version).await?;
    }
    let seed = history.build_at(2)?;
    assert_eq!(seed.log_segment.listed.max_published_version, Some(2));
    assert_eq!(
        selected_commit(&seed, 1).file_type,
        LogPathFileType::StagedCommit
    );
    let unchanged = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(2, history.staged.clone())
        .build(&NoIoEngine)?;
    assert!(Arc::ptr_eq(&seed, &unchanged));
    Ok(())
}

#[tokio::test]
async fn complete_catalog_time_travel_clamps_publication_evidence() -> TestResult {
    let history = CatalogHistory::new(0, 4).await?;
    for version in 0..=4 {
        history.publish(version).await?;
    }
    let snapshot = Snapshot::builder_for(&history.table_root)
        .with_catalog_metadata(4, Vec::new())
        .at_version(2)
        .build(history.engine.as_ref())?;
    assert_eq!(snapshot.version(), 2);
    assert_eq!(snapshot.log_segment.listed.max_published_version, Some(2));
    assert!(!snapshot.is_built_as_latest());
    Ok(())
}

#[rstest]
#[case::duplicate(vec![1, 1, 2], 2)]
#[case::unsorted(vec![2, 1], 2)]
#[case::gap(vec![0, 2], 2)]
#[case::above_maximum(vec![3], 2)]
#[case::missing_endpoint(vec![1], 2)]
#[tokio::test]
async fn complete_catalog_validates_full_response_before_time_travel_or_reuse(
    #[case] versions: Vec<Version>,
    #[case] maximum: Version,
    #[values(false, true)] incremental: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, 3).await?;
    let commits = versions
        .iter()
        .map(|v| history.staged[*v as usize].clone())
        .collect();
    let result = if incremental {
        Snapshot::builder_from(history.build_at(0)?)
            .with_catalog_metadata(maximum, commits)
            .at_version(0)
            .build(&NoIoEngine)
    } else {
        Snapshot::builder_for(&history.table_root)
            .with_catalog_metadata(maximum, commits)
            .at_version(0)
            .build(&NoIoEngine)
    };
    assert!(result.is_err());
    Ok(())
}

#[rstest]
#[case::wrong_table("memory:///other/_delta_log/00000000000000000002.json")]
#[case::crc("memory:///_delta_log/00000000000000000002.crc")]
#[case::checkpoint("memory:///_delta_log/00000000000000000002.checkpoint.parquet")]
#[tokio::test]
async fn complete_catalog_rejects_invalid_paths_beyond_time_travel_target(
    #[case] path: &str,
) -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    let path = LogPath::try_new(FileMeta {
        location: Url::parse(path)?,
        last_modified: 0,
        size: 1,
    })?;
    let result = Snapshot::builder_from(history.build_at(0)?)
        .with_catalog_metadata(2, vec![path])
        .at_version(0)
        .build(&NoIoEngine);
    assert!(result.is_err());
    Ok(())
}

#[rstest]
#[case::catalog_behind_seed(1, None)]
#[case::target_behind_seed(2, Some(1))]
#[case::target_above_catalog(2, Some(3))]
#[tokio::test]
async fn complete_catalog_rejects_invalid_incremental_bounds(
    #[case] maximum: Version,
    #[case] target: Option<Version>,
) -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    let builder =
        Snapshot::builder_from(history.build_at(2)?).with_catalog_metadata(maximum, Vec::new());
    let builder = if let Some(target) = target {
        builder.at_version(target)
    } else {
        builder
    };
    assert!(builder.build(&NoIoEngine).is_err());
    Ok(())
}

#[rstest]
#[case::empty_tail(false)]
#[case::bound(true)]
#[tokio::test]
async fn complete_catalog_rejects_mixed_setters_in_either_order(
    #[case] legacy_bound: bool,
    #[values(false, true)] complete_first: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    let mut builder = Snapshot::builder_from(history.build_at(0)?);
    if complete_first {
        builder = builder.with_catalog_metadata(0, Vec::new());
    }
    #[allow(deprecated)]
    let legacy_builder = if legacy_bound {
        builder.with_max_catalog_version(0)
    } else {
        builder.with_log_tail(Vec::new())
    };
    builder = legacy_builder;
    if !complete_first {
        builder = builder.with_catalog_metadata(0, Vec::new());
    }
    assert!(builder.build(&NoIoEngine).is_err());
    Ok(())
}

#[tokio::test]
async fn repeating_complete_catalog_setter_replaces_whole_observation() -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    let seed = history.build_at(2)?;
    let snapshot = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(0, Vec::new())
        .with_catalog_metadata(2, history.staged.clone())
        .build(&NoIoEngine)?;
    assert!(Arc::ptr_eq(&seed, &snapshot));
    Ok(())
}

#[tokio::test]
async fn complete_catalog_rejects_snapshot_hint_even_when_empty() -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    let seed = history.build_at(0)?;
    let hint = SnapshotHint::from_snapshot(&seed)?;
    let result = Snapshot::builder_for(&history.table_root)
        .with_snapshot_hint(hint)
        .with_catalog_metadata(0, Vec::new())
        .build(&NoIoEngine);
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn complete_catalog_rejects_filesystem_managed_table() -> TestResult {
    let (engine, store, table_root) = setup_test();
    add_commit(
        &table_root,
        store.as_ref(),
        0,
        test_utils::actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    let result = Snapshot::builder_for(&table_root)
        .with_catalog_metadata(0, Vec::new())
        .build(engine.as_ref());
    assert!(matches!(result, Err(KernelError::MaxCatalogVersion(_))));
    Ok(())
}

#[tokio::test]
#[allow(deprecated)]
async fn legacy_empty_tail_does_not_assert_catalog_retirement() -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    let seed = history.build_at(2)?;
    let snapshot = Snapshot::builder_from(seed.clone())
        .with_log_tail(Vec::new())
        .with_max_catalog_version(2)
        .build(&NoIoEngine)?;
    assert!(Arc::ptr_eq(&seed, &snapshot));
    assert_eq!(
        selected_commit(&snapshot, 0).file_type,
        LogPathFileType::StagedCommit
    );
    Ok(())
}

#[tokio::test]
async fn catalog_reference_repair_preserves_crc_and_resolved_absence() -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    let original = history.build_at(2)?;
    let crc = Arc::new(Crc {
        version: 2,
        metadata: original.table_configuration.metadata().clone(),
        protocol: original.table_configuration.protocol().clone(),
        in_commit_timestamp_opt: Some(ICT_BASE + 2),
        ..Default::default()
    });
    let mut segment = original.log_segment.clone();
    segment.listed.latest_crc_file = Some(
        ParsedLogPath::new(FileMeta {
            location: Url::parse(&history.table_root)?
                .join("_delta_log/00000000000000000001.crc")?,
            last_modified: 456,
            size: 789,
        })
        .unwrap(),
    );
    let seed = Arc::new(Snapshot::new_with_crc(
        segment,
        original.table_configuration.clone(),
        Some(crc.clone()),
        true,
        false,
    )?);
    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        seed.checkpoint_action.set(None).unwrap();
        assert_eq!(seed.last_manifest_commit.get(), Some(&None));
    }
    let updated = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(2, Vec::new())
        .build(&NoIoEngine)?;
    assert!(Arc::ptr_eq(updated.crc_at_version().unwrap(), &crc));
    assert_eq!(
        updated.log_segment.listed.latest_crc_file,
        seed.log_segment.listed.latest_crc_file
    );
    #[cfg(feature = "adaptive-metadata-in-dev")]
    {
        assert_eq!(updated.checkpoint_action.get(), Some(&None));
        assert_eq!(updated.last_manifest_commit.get(), Some(&None));
    }
    assert_eq!(
        updated.log_segment.last_checkpoint_metadata,
        seed.log_segment.last_checkpoint_metadata
    );
    Ok(())
}

#[tokio::test]
async fn missing_published_replacement_preserves_typed_read_failure() -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    let repaired = Snapshot::builder_from(history.build_at(0)?)
        .with_catalog_metadata(0, Vec::new())
        .build(&NoIoEngine)?;
    let error = repaired
        .reader()
        .get_timestamp(history.engine.as_ref())
        .unwrap_err();
    assert!(matches!(error, KernelError::FileNotFound(_)));
    Ok(())
}

#[tokio::test]
async fn advancing_repair_does_not_probe_known_published_replacements() -> TestResult {
    let history = CatalogHistory::new(0, 3).await?;
    let seed = history.build_at(2)?;
    let updated = Snapshot::builder_from(seed)
        .with_catalog_metadata(3, history.commits_from(3))
        .build(history.engine.as_ref())?;
    assert_eq!(updated.version(), 3);
    for version in 0..=2 {
        assert_eq!(
            selected_commit(&updated, version).file_type,
            LogPathFileType::Commit
        );
    }
    assert_eq!(
        updated.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE + 3
    );
    let error = updated
        .reader()
        .get_app_id_version("app-0", history.engine.as_ref())
        .unwrap_err();
    assert!(matches!(error, KernelError::FileNotFound(_)));
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advancing_catalog_requires_missing_middle_or_covering_checkpoint(
    #[values(false, true)] checkpoint: bool,
) -> TestResult {
    let history = CatalogHistory::new(6, 17).await?;
    let seed = history.build_at(10)?;
    for version in 6..=14 {
        history.publish(version).await?;
    }
    if checkpoint {
        history
            .build_at(14)?
            .checkpoint(history.engine.as_ref(), None)?;
    }
    for version in 11..=14 {
        history
            .store
            .delete(&delta_path_for_version(version, "json"))
            .await?;
    }
    let result = Snapshot::builder_from(seed)
        .with_catalog_metadata(17, history.commits_from(15))
        .build(history.engine.as_ref());
    if checkpoint {
        let snapshot = result?;
        assert_eq!(snapshot.log_segment.checkpoint_version, Some(14));
        assert_eq!(
            snapshot
                .reader()
                .get_app_id_version("app-11", history.engine.as_ref())?,
            Some(11)
        );
    } else {
        assert!(result.is_err());
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn advancing_repair_prefers_observed_published_metadata(
    #[values(false, true)] skip_new_checkpoints: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, 3).await?;
    let original = history.build_at(2)?;
    let mut segment = original.log_segment.clone();
    for path in &mut segment.listed.ascending_commit_files {
        path.location.last_modified = -100;
    }
    let seed = Arc::new(Snapshot::new_with_crc(
        segment,
        original.table_configuration.clone(),
        None,
        false,
        false,
    )?);
    for version in 0..=2 {
        history.publish(version).await?;
    }
    let builder = Snapshot::builder_from(seed).with_catalog_metadata(3, history.commits_from(3));
    let builder = if skip_new_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    let updated = builder.build(history.engine.as_ref())?;
    let repaired = selected_commit(&updated, 1);
    assert_eq!(repaired.file_type, LogPathFileType::Commit);
    if skip_new_checkpoints {
        assert_eq!(repaired.location.last_modified, -100);
    } else {
        let observed = history
            .store
            .head(&delta_path_for_version(1, "json"))
            .await?;
        assert_eq!(
            repaired.location.last_modified,
            observed.last_modified.timestamp_millis()
        );
        assert_eq!(repaired.location.size, observed.size);
    }
    assert_eq!(
        updated.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE + 3
    );
    Ok(())
}

#[rstest]
#[case::mixed(1)]
#[case::published_only(3)]
#[tokio::test]
async fn complete_catalog_accepts_and_prefers_published_entries(
    #[case] published_count: usize,
) -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    let seed = history.build_at(2)?;
    let mut commits = history.staged.clone();
    for (version, entry) in commits.iter_mut().take(published_count).enumerate() {
        history.publish(version as Version).await?;
        let staged: ParsedLogPath = entry.clone().into();
        *entry = LogPath::from_commit_filename(
            Url::parse(&history.table_root)?,
            version as Version,
            &format!("{version:020}.json"),
            123,
            staged.location.size,
        )?;
    }
    let snapshot = Snapshot::builder_from(seed)
        .with_catalog_metadata(2, commits)
        .build(&NoIoEngine)?;
    for version in 0..published_count {
        let selected = selected_commit(&snapshot, version as Version);
        assert_eq!(selected.file_type, LogPathFileType::Commit);
        assert_eq!(selected.location.last_modified, 123);
        history.delete_staged(version as Version).await?;
    }
    assert_eq!(
        snapshot
            .reader()
            .get_app_id_version("app-0", history.engine.as_ref())?,
        Some(0)
    );
    assert_eq!(
        snapshot.reader().get_timestamp(history.engine.as_ref())?,
        ICT_BASE + 2
    );
    Ok(())
}

#[rstest]
#[case::same_version(2, None)]
#[case::same_version_checkpoint_at_conflict(2, Some(1))]
#[case::advancing(3, None)]
#[case::advancing_checkpoint_at_conflict(3, Some(1))]
#[case::advancing_checkpoint_ahead_of_seed(3, Some(3))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_staged_identity_reconstructs_metadata_and_drops_seed_crc(
    #[case] target: Version,
    #[case] checkpoint: Option<Version>,
    #[values(false, true)] skip_new_checkpoints: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, 3).await?;
    let original = history.build_at(2)?;
    let seed = Arc::new(Snapshot::new_with_crc(
        original.log_segment.clone(),
        original.table_configuration.clone(),
        Some(Arc::new(Crc {
            version: 2,
            metadata: original.table_configuration.metadata().clone(),
            protocol: original.table_configuration.protocol().clone(),
            ..Default::default()
        })),
        true,
        false,
    )?);
    let actions = actions_to_string_catalog_managed(vec![TestAction::Metadata]);
    let mut actions: Vec<serde_json::Value> = actions
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    actions
        .iter_mut()
        .find(|action| action.get("metaData").is_some())
        .unwrap()["metaData"]["description"] = json!("authoritative metadata");
    actions.push(json!({"txn": {"appId": "replacement", "version": 77}}));
    let actions = actions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let path = add_staged_commit(&history.table_root, history.store.as_ref(), 1, actions).await?;
    let metadata = history.store.head(&path).await?;
    let replacement = LogPath::try_new(FileMeta {
        location: Url::parse(&history.table_root)?.join(path.as_ref())?,
        last_modified: metadata.last_modified.timestamp_millis(),
        size: metadata.size,
    })?;
    let commits = vec![
        history.staged[0].clone(),
        replacement,
        history.staged[2].clone(),
        history.staged[3].clone(),
    ];
    if let Some(checkpoint) = checkpoint {
        for version in 0..=checkpoint {
            if version == 1 {
                history
                    .store
                    .copy(&path, &delta_path_for_version(1, "json"))
                    .await?;
            } else {
                history.publish(version).await?;
            }
        }
        Snapshot::builder_for(&history.table_root)
            .with_catalog_metadata(3, commits.clone())
            .at_version(checkpoint)
            .build(history.engine.as_ref())?
            .checkpoint(history.engine.as_ref(), None)?;
    }
    let builder = Snapshot::builder_from(seed.clone())
        .with_catalog_metadata(3, commits)
        .at_version(target);
    let builder = if skip_new_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    let snapshot = builder.build(history.engine.as_ref())?;
    assert_eq!(snapshot.version(), target);
    assert_eq!(snapshot.skipped_new_checkpoints, skip_new_checkpoints);
    assert_eq!(snapshot.log_segment.checkpoint_version, None);
    assert_eq!(
        snapshot.table_configuration.metadata().description(),
        Some("authoritative metadata")
    );
    assert!(snapshot.base_crc().is_none());
    assert!(seed.base_crc().is_some());
    assert_eq!(
        snapshot
            .reader()
            .get_app_id_version("replacement", history.engine.as_ref())?,
        Some(77)
    );
    Ok(())
}

#[tokio::test]
async fn conflicting_staged_identity_rejects_different_table_incarnation() -> TestResult {
    let history = CatalogHistory::new(0, 0).await?;
    let seed = history.build_at(0)?;
    let actions = actions_to_string_catalog_managed(vec![TestAction::Metadata]);
    let actions = actions
        .lines()
        .map(|line| {
            let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(metadata) = action.get_mut("metaData") {
                metadata["id"] = json!("different-table-id");
            }
            action.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    let path = add_staged_commit(&history.table_root, history.store.as_ref(), 0, actions).await?;
    let metadata = history.store.head(&path).await?;
    let replacement = LogPath::try_new(FileMeta {
        location: Url::parse(&history.table_root)?.join(path.as_ref())?,
        last_modified: metadata.last_modified.timestamp_millis(),
        size: metadata.size,
    })?;
    let error = Snapshot::builder_from(seed)
        .with_catalog_metadata(0, vec![replacement])
        .build(history.engine.as_ref())
        .unwrap_err();
    assert!(error.to_string().contains("different table incarnation"));
    Ok(())
}

#[rstest]
#[case::latest_only_checkpoint_at_zero(0, vec![0])]
#[case::checkpoint_with_earlier_base(2, vec![0, 2])]
#[case::crc_without_checkpoint(2, vec![])]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_staged_identity_reconstructs_before_retained_checkpoint_and_crc(
    #[case] conflict_version: Version,
    #[case] checkpoints: Vec<Version>,
    #[values(false, true)] disk_crc: bool,
    #[values(false, true)] skip_new_checkpoints: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, conflict_version).await?;
    for version in 0..=conflict_version {
        history.publish(version).await?;
    }
    for checkpoint in &checkpoints {
        history
            .build_at(*checkpoint)?
            .checkpoint(history.engine.as_ref(), None)?;
    }
    let seed = history.build_at(conflict_version)?;
    let seed = if disk_crc {
        seed.write_checksum(history.engine.as_ref())?.1
    } else {
        seed
    };
    let actions = actions_to_string_catalog_managed(vec![TestAction::Metadata]);
    let mut actions: Vec<serde_json::Value> = actions
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    actions
        .iter_mut()
        .find(|action| action.get("metaData").is_some())
        .unwrap()["metaData"]["description"] = json!("replacement history");
    actions.push(json!({"txn": {"appId": "checkpoint-replacement", "version": 77}}));
    let actions = actions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let path = add_staged_commit(
        &history.table_root,
        history.store.as_ref(),
        conflict_version,
        actions,
    )
    .await?;
    let metadata = history.store.head(&path).await?;
    let replacement = LogPath::try_new(FileMeta {
        location: Url::parse(&history.table_root)?.join(path.as_ref())?,
        last_modified: metadata.last_modified.timestamp_millis(),
        size: metadata.size,
    })?;
    let mut commits = history.staged.clone();
    commits[conflict_version as usize] = replacement;
    let builder = Snapshot::builder_from(seed).with_catalog_metadata(conflict_version, commits);
    let builder = if skip_new_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    let snapshot = builder.build(history.engine.as_ref())?;
    assert_eq!(
        snapshot.table_configuration.metadata().description(),
        Some("replacement history")
    );
    assert_eq!(
        snapshot.log_segment.checkpoint_version,
        checkpoints
            .iter()
            .copied()
            .filter(|version| *version < conflict_version)
            .max()
    );
    assert!(
        snapshot
            .base_crc()
            .is_none_or(|crc| crc.version < conflict_version)
    );
    assert_eq!(
        snapshot
            .reader()
            .get_app_id_version("checkpoint-replacement", history.engine.as_ref())?,
        Some(77)
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_staged_identity_rejects_missing_prefix_covered_only_by_unsafe_checkpoint(
    #[values(false, true)] skip_new_checkpoints: bool,
) -> TestResult {
    let history = CatalogHistory::new(0, 2).await?;
    for version in 0..=2 {
        history.publish(version).await?;
    }
    history
        .build_at(2)?
        .checkpoint(history.engine.as_ref(), None)?;
    let seed = history.build_at(2)?;
    let replacement_path = add_staged_commit(
        &history.table_root,
        history.store.as_ref(),
        2,
        actions_to_string_catalog_managed(vec![TestAction::Metadata]),
    )
    .await?;
    let metadata = history.store.head(&replacement_path).await?;
    let replacement = LogPath::try_new(FileMeta {
        location: Url::parse(&history.table_root)?.join(replacement_path.as_ref())?,
        last_modified: metadata.last_modified.timestamp_millis(),
        size: metadata.size,
    })?;
    for version in 0..2 {
        history.delete_staged(version).await?;
        history
            .store
            .delete(&delta_path_for_version(version, "json"))
            .await?;
    }
    let builder = Snapshot::builder_from(seed).with_catalog_metadata(2, vec![replacement]);
    let builder = if skip_new_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    assert!(matches!(
        builder.build(history.engine.as_ref()),
        Err(KernelError::MissingVersion(0))
    ));
    Ok(())
}
