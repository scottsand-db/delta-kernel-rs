//! CRC fuzz stress test: 100 commits with randomized domain metadata operations,
//! file adds/removes, periodic checkpoints, and multi-snapshot CRC verification.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::crc::FileStatsValidity;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Engine};
use rand::prelude::*;
use test_utils::test_table_setup_mt;

// ============================================================================
// Commit helpers
// ============================================================================

/// Write 2 parquet files (1 row each) and add them to the transaction.
async fn add_files_to_txn<S>(
    txn: &mut Transaction<S>,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
    arrow_schema: &ArrowSchema,
    v: usize,
) -> DeltaResult<()> {
    for i in 0..2 {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![(v * 10 + i) as i32]));
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![col])
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
        let write_context = txn.get_write_context();
        let add_files_metadata = engine
            .write_parquet(&ArrowEngineData::new(batch), &write_context, HashMap::new())
            .await?;
        txn.add_files(add_files_metadata);
    }
    Ok(())
}

/// Scan the snapshot, pick 1 random file (via selection vector), and remove it.
fn remove_random_file(
    txn: &mut Transaction,
    snapshot: &Arc<Snapshot>,
    engine: &dyn Engine,
    rng: &mut StdRng,
) -> DeltaResult<()> {
    let scan = snapshot.clone().scan_builder().build()?;
    for sm in scan.scan_metadata(engine)? {
        let sm = sm?;
        let (data, sel) = sm.scan_files.into_parts();
        let num_rows = data.len();
        let active: Vec<usize> = (0..num_rows)
            .filter(|&i| i >= sel.len() || sel[i])
            .collect();
        if active.is_empty() {
            continue;
        }
        let pick = active[rng.gen_range(0..active.len())];
        let mut new_sel = vec![false; num_rows];
        new_sel[pick] = true;
        txn.remove_files(FilteredEngineData::try_new(data, new_sel)?);
        return Ok(());
    }
    panic!("No files found to remove");
}

/// Apply fuzzed domain metadata operations to the transaction.
///
/// Always inserts one new domain. With 70% probability, updates an existing domain.
/// With 50% probability, removes an existing domain (different from the updated one).
/// Only user domains (not `delta.*` system domains) are candidates for update/remove.
/// Returns the transaction back since `with_domain_metadata` / `with_domain_metadata_removed`
/// consume self.
fn apply_domain_ops(
    mut txn: Transaction,
    rng: &mut StdRng,
    v: usize,
    next_domain_id: &mut usize,
    expected_domains: &mut HashMap<String, String>,
) -> Transaction {
    let pre_existing: Vec<String> = expected_domains
        .keys()
        .filter(|k| !k.starts_with("delta."))
        .cloned()
        .collect();

    // Always INSERT a new domain
    let new_name = format!("d_{}", *next_domain_id);
    *next_domain_id += 1;
    let new_val = format!("{v}");
    println!("  domain insert: {new_name}={new_val}");
    txn = txn.with_domain_metadata(new_name.clone(), new_val.clone());
    expected_domains.insert(new_name.clone(), new_val);

    let mut touched = vec![new_name];

    // UPDATE with 70% probability (must pick from pre-existing, not the newly inserted one)
    if pre_existing.len() > 1 && rng.gen_bool(0.7) {
        let idx = rng.gen_range(0..pre_existing.len());
        let key = &pre_existing[idx];
        let val = format!("u{v}");
        println!("  domain update: {key}={val}");
        txn = txn.with_domain_metadata(key.clone(), val.clone());
        expected_domains.insert(key.clone(), val);
        touched.push(key.clone());
    }

    // REMOVE with 50% probability (must not remove a domain touched above, and keep >= 1 user domain)
    let user_domain_count = expected_domains
        .keys()
        .filter(|k| !k.starts_with("delta."))
        .count();
    if user_domain_count > 1 && rng.gen_bool(0.5) {
        let candidates: Vec<String> = expected_domains
            .keys()
            .filter(|k| !k.starts_with("delta.") && !touched.contains(k))
            .cloned()
            .collect();
        if !candidates.is_empty() {
            let idx = rng.gen_range(0..candidates.len());
            let key = &candidates[idx];
            println!("  domain remove: {key}");
            txn = txn.with_domain_metadata_removed(key.clone());
            expected_domains.remove(key);
        }
    }

    txn
}

// ============================================================================
// Validation helpers
// ============================================================================

/// Load a fresh snapshot from disk and validate domain metadata via log replay.
/// Only checks user domains (not `delta.*` system domains) since log replay returns
/// the configuration string, and system domain configs are opaque to this test.
fn validate_fresh_snapshot_no_crc(
    table_path: &str,
    expected_version: u64,
    expected_domains: &HashMap<String, String>,
    engine: &dyn Engine,
) {
    let fresh = Snapshot::builder_for(table_path).build(engine).unwrap();
    assert_eq!(
        fresh.version(),
        expected_version,
        "snapshot version mismatch"
    );
    assert_domains_via_log_replay(&fresh, expected_domains, engine);
}

/// Validate user domain metadata via log replay (works with or without a CRC file on disk).
fn assert_domains_via_log_replay(
    snapshot: &Snapshot,
    expected_domains: &HashMap<String, String>,
    engine: &dyn Engine,
) {
    for (domain, expected_config) in expected_domains {
        if domain.starts_with("delta.") {
            continue;
        }
        let actual = snapshot
            .get_domain_metadata(domain, engine)
            .unwrap()
            .unwrap_or_else(|| panic!("v{}: domain '{}' missing", snapshot.version(), domain));
        assert_eq!(
            actual,
            *expected_config,
            "v{}: domain '{}' config mismatch",
            snapshot.version(),
            domain
        );
    }
}

/// Load a fresh snapshot from disk and validate CRC content (file stats + domain metadata).
/// Also validates file stats via the public `get_file_stats` API.
fn validate_fresh_snapshot_with_crc(
    table_path: &str,
    expected_version: u64,
    expected_num_files: i64,
    expected_domains: &HashMap<String, String>,
    engine: &dyn Engine,
) {
    let fresh = Snapshot::builder_for(table_path).build(engine).unwrap();
    assert_eq!(
        fresh.version(),
        expected_version,
        "snapshot version mismatch"
    );
    validate_in_memory_crc(&fresh, expected_num_files, expected_domains);

    // Also validate file stats via the snapshot's public API.
    let stats = fresh
        .get_file_stats(engine)
        .unwrap_or_else(|| panic!("get_file_stats returned None at v{}", fresh.version()));
    assert_eq!(
        stats.num_files,
        expected_num_files,
        "get_file_stats num_files mismatch at v{}",
        fresh.version()
    );
    assert_eq!(
        stats.table_size_bytes > 0,
        expected_num_files > 0,
        "get_file_stats table_size_bytes inconsistency at v{}",
        fresh.version()
    );
}

/// Validate file stats and domain metadata from a snapshot's in-memory CRC.
fn validate_in_memory_crc(
    snapshot: &Snapshot,
    expected_num_files: i64,
    expected_domains: &HashMap<String, String>,
) {
    let crc = snapshot
        .get_current_crc_if_loaded_for_testing()
        .unwrap_or_else(|| panic!("CRC missing at v{}", snapshot.version()));

    // Verify file stats are valid and match expected counts.
    assert_eq!(
        crc.file_stats_validity,
        FileStatsValidity::Valid,
        "file stats not valid at v{}",
        snapshot.version()
    );
    let stats = crc.file_stats().unwrap();
    assert_eq!(
        stats.num_files,
        expected_num_files,
        "num_files mismatch at v{}",
        snapshot.version()
    );
    if expected_num_files > 0 {
        assert!(
            stats.table_size_bytes > 0,
            "table_size_bytes should be > 0 at v{}",
            snapshot.version()
        );
    } else {
        assert_eq!(
            stats.table_size_bytes,
            0,
            "table_size_bytes should be 0 at v{}",
            snapshot.version()
        );
    }

    // This table only ever has one metadata and one protocol action.
    assert_eq!(
        crc.num_metadata,
        1,
        "num_metadata at v{}",
        snapshot.version()
    );
    assert_eq!(
        crc.num_protocol,
        1,
        "num_protocol at v{}",
        snapshot.version()
    );

    // Verify all domain metadata (user + system) matches our expected tracking map.
    let actual_domains: HashMap<String, String> = crc
        .domain_metadata
        .as_ref()
        .map(|dms| {
            dms.iter()
                .map(|(k, v)| (k.clone(), v.configuration().to_string()))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        actual_domains,
        *expected_domains,
        "CRC domain mismatch at v{}",
        snapshot.version()
    );
}

// ============================================================================
// Test
// ============================================================================

// TODO: Be able to DISABLE and PREVENT the snapshot from loading the CRC file?
//       So we can test pure non-CRC path vs with-CRC path.

/// Deterministic fuzz test (seeded RNG) that builds a 100-commit table history and validates
/// CRC correctness at every version. Uses a fixed seed (`StdRng::seed_from_u64(42)`) so the
/// exact same random choices replay on every run, making failures always reproducible.
///
/// Each commit applies randomized mutations:
/// - Add/remove: +2 files, -1 random file.
/// - Domain metadata: always insert 1 new domain, update existing (70%), remove existing (50%).
/// - TODO: set_txn (application transaction) changes.
/// - TODOO: file size and file size histogram changes
///
/// We write a checksum after each commit. We also write a checkpoint with ~20% probability.
///
/// After each commit, runs validations:
/// 1. Post-commit snapshot (with in-memory CRC)
/// 2. Fresh-from-disk snapshot (no CRC file)
/// 3. [This is when we would write the checkpoint] Fresh-from-disk snapshot (with checkpoint)
/// 4. [This is when we would write the checksum] Fresh-from-disk snapshot (with CRC file written to disk)
#[tokio::test(flavor = "multi_thread")]
async fn test_crc_stress_fuzz_100_commits_all_verify() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let mut rng = StdRng::seed_from_u64(42);

    let schema = Arc::new(StructType::try_new(vec![StructField::new(
        "id",
        DataType::INTEGER,
        false,
    )])?);
    let arrow_schema: ArrowSchema = TryFromKernel::try_from_kernel(schema.as_ref())?;

    // -- v0: CREATE TABLE with 2 files + 3 domains --
    let mut txn = create_table(&table_path, schema, "stress_test/1.0")
        .with_table_properties([("delta.columnMapping.mode", "name")])
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata("d_0".to_string(), "0".to_string())
        .with_domain_metadata("d_1".to_string(), "1".to_string())
        .with_domain_metadata("d_2".to_string(), "2".to_string());
    add_files_to_txn(&mut txn, engine.as_ref(), &arrow_schema, 0).await?;
    let committed = txn.commit(engine.as_ref())?.unwrap_committed();

    let mut snapshot = committed.post_commit_snapshot().unwrap().clone();
    let mut expected_num_files: i64 = 2;

    // Capture the delta.clustering config via log replay so we can track all domains
    // (user + system) in a single expected map.
    let clustering_config = snapshot
        .get_domain_metadata_internal("delta.clustering", engine.as_ref())?
        .expect("delta.clustering domain missing after create_table");
    let mut expected_domains: HashMap<String, String> = HashMap::from([
        ("d_0".to_string(), "0".to_string()),
        ("d_1".to_string(), "1".to_string()),
        ("d_2".to_string(), "2".to_string()),
        ("delta.clustering".to_string(), clustering_config),
    ]);
    let mut next_domain_id: usize = 3;

    println!(
        "v0: committed (num_files={}, domains={})",
        expected_num_files,
        expected_domains.len()
    );
    validate_in_memory_crc(&snapshot, expected_num_files, &expected_domains);

    for v in 1..=100 {
        // ===== Build and commit: fuzzed domain ops + 2 file adds + 1 random file remove =====
        println!("v{v}: starting commit...");
        let txn = snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("WRITE".to_string())
            .with_data_change(true);

        let mut txn =
            apply_domain_ops(txn, &mut rng, v, &mut next_domain_id, &mut expected_domains);
        add_files_to_txn(&mut txn, engine.as_ref(), &arrow_schema, v).await?;
        remove_random_file(&mut txn, &snapshot, engine.as_ref(), &mut rng)?;
        expected_num_files += 1; // +2 adds, -1 remove

        let committed = txn.commit(engine.as_ref())?.unwrap_committed();
        snapshot = committed.post_commit_snapshot().unwrap().clone();
        println!(
            "v{v}: committed (num_files={expected_num_files}, domains={})",
            expected_domains.len()
        );

        // ===== #1: Validate post-commit snapshot (in-memory CRC) =====
        validate_in_memory_crc(&snapshot, expected_num_files, &expected_domains);
        println!("v{v}: #1 post-commit snapshot OK");

        // ===== #2: Validate fresh-from-disk snapshot (no CRC file) =====
        validate_fresh_snapshot_no_crc(&table_path, v as u64, &expected_domains, engine.as_ref());
        println!("v{v}: #2 fresh-from-disk snapshot OK");

        // ===== #3: Checkpoint with ~20% probability, re-validate fresh-from-disk =====
        if rng.gen_bool(0.2) {
            snapshot.clone().checkpoint(engine.as_ref())?;
            validate_fresh_snapshot_no_crc(
                &table_path,
                v as u64,
                &expected_domains,
                engine.as_ref(),
            );
            println!("v{v}: #3 post-checkpoint snapshot OK");
        }

        // ===== #4: Write CRC to disk, reload, validate fresh-from-disk (with CRC) =====
        snapshot.write_checksum(engine.as_ref())?;
        validate_fresh_snapshot_with_crc(
            &table_path,
            v as u64,
            expected_num_files,
            &expected_domains,
            engine.as_ref(),
        );
        println!("v{v}: #4 post-CRC-write snapshot OK");
    }

    Ok(())
}
