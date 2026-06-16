//! End-to-end integration tests for CHECK constraint enforcement on the write path.
//!
//! The connector flow exercised here mirrors the intended design: open a transaction, build a
//! [`ConstraintChecker`] from it, validate the logical batch, and only write + commit if the
//! check passes. A violating batch must error before any data is written.

use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{DeltaResult, Engine, Snapshot};
use serde_json::json;
use test_utils::{add_commit, engine_store_setup};
use url::Url;

fn table_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("age", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

/// Write commit 0: a writer-v7 protocol listing `checkConstraints`, plus metadata whose
/// configuration declares the given `delta.constraints.*` entries. `create_table` helpers cannot
/// inject arbitrary configuration keys, so the initial commit is written directly.
async fn create_constrained_table(
    store: &Arc<dyn delta_kernel::object_store::ObjectStore>,
    location: Url,
    schema: &StructType,
    constraints: &[(&str, &str)],
) -> Result<Url, Box<dyn std::error::Error>> {
    let mut configuration = serde_json::Map::new();
    for (name, sql) in constraints {
        configuration.insert(format!("delta.constraints.{name}"), json!(sql));
    }
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": [],
            "writerFeatures": ["checkConstraints"],
        }
    });
    let metadata = json!({
        "metaData": {
            "id": "test-check-constraints",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": serde_json::to_string(schema)?,
            "partitionColumns": [],
            "configuration": configuration,
            "createdTime": 0,
        }
    });
    add_commit(location.as_str(), store.as_ref(), 0, format!("{protocol}\n{metadata}")).await?;
    Ok(location)
}

fn batch(schema: &StructType, ages: Vec<i32>, names: Vec<&str>) -> DeltaResult<ArrowEngineData> {
    let arrow_schema = Arc::new(schema.try_into_arrow()?);
    let rb = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(ages)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    Ok(ArrowEngineData::new(rb))
}

#[tokio::test]
async fn check_constraints_pass_inserts_and_fail_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = table_schema();
    let (store, engine, location) = engine_store_setup("test_check_constraints", None);
    let table_url = create_constrained_table(
        &store,
        location,
        &schema,
        &[("valid_age", "age = 21"), ("valid_name", "name = 'bob'")],
    )
    .await?;

    // === passing write: every row satisfies both constraints -> check OK -> commit ===
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_operation("WRITE".to_string())
        .with_blind_append();
    let checker = txn.constraint_checker(engine.evaluation_handler().as_ref())?;

    let good = batch(&schema, vec![21, 21], vec!["bob", "bob"])?;
    checker.check(&good)?; // must pass

    let write_context = txn.unpartitioned_write_context()?;
    let add = engine.write_parquet(&good, &write_context).await?;
    txn.add_files(add);
    assert!(
        txn.commit(&engine)?.is_committed(),
        "valid batch must commit",
    );

    // === violating write: one row breaks `valid_name` -> check errors, nothing written ===
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    assert_eq!(snapshot.version(), 1, "passing write should have produced v1");
    let txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_operation("WRITE".to_string())
        .with_blind_append();
    let checker = txn.constraint_checker(engine.evaluation_handler().as_ref())?;

    let bad = batch(&schema, vec![21, 21], vec!["bob", "alice"])?;
    let err = checker
        .check(&bad)
        .expect_err("violating batch must be rejected")
        .to_string();
    assert!(
        err.contains("valid_name"),
        "error must name the violated constraint; got: {err}",
    );

    // The table is untouched: still at v1 (the violating write never reached commit).
    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    assert_eq!(snapshot.version(), 1, "violating write must not commit");

    Ok(())
}
