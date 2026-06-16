//! Write-time enforcement of Delta CHECK constraints.
//!
//! CHECK constraints are boolean SQL expressions stored in table metadata under keys prefixed
//! `delta.constraints.` (e.g. `delta.constraints.age_positive = "age > 0"`). Every row written to
//! the table must satisfy every constraint; a row passes only if the expression evaluates to
//! literally TRUE (both FALSE and NULL are violations, matching Spark).
//!
//! Discovery ([`CheckConstraint`]) exposes the raw SQL and whether kernel could parse it, for
//! connectors that evaluate the SQL with their own engine. Evaluation ([`ConstraintChecker`])
//! parses every constraint, AND-fuses them into a single predicate, and validates a batch in one
//! pass using the engine's [`EvaluationHandler`]. The checker evaluates against the logical batch:
//! constraints reference logical column names, so the caller must run the check before any
//! logical-to-physical transform (column mapping, partition-column dropping).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use super::{ExistingTable, Transaction};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::sql::parse_predicate;
use crate::expressions::Predicate;
use crate::schema::{
    column_name, ColumnName, ColumnNamesAndTypes, DataType, SchemaRef, StructType,
};
use crate::{DeltaResult, EngineData, Error, EvaluationHandler, PredicateEvaluator};

const DELTA_CONSTRAINT_PREFIX: &str = "delta.constraints.";

/// A single CHECK constraint discovered from table metadata.
///
/// Carries the constraint name, its raw SQL, and the parsed [`Predicate`] when kernel's SQL parser
/// could handle it. A connector with its own SQL engine can read [`raw_sql`](Self::raw_sql) and
/// evaluate it itself; a connector relying on kernel uses [`Transaction::constraint_checker`].
pub struct CheckConstraint {
    name: String,
    raw_sql: String,
    parsed: Option<Predicate>,
}

impl CheckConstraint {
    /// Build a constraint from its name and raw SQL, eagerly attempting to parse the SQL into a
    /// predicate against `schema`. Unparsable SQL leaves
    /// [`is_kernel_parsable`](Self::is_kernel_parsable) false.
    fn new(name: String, raw_sql: String, schema: &StructType) -> Self {
        let parsed = parse_predicate(&raw_sql, schema).ok();
        Self {
            name,
            raw_sql,
            parsed,
        }
    }

    /// Extract every `delta.constraints.<name>` entry from a table configuration map. The key
    /// prefix is matched case-insensitively (matching Spark); the name keeps its original case.
    fn from_configuration(
        cfg: &HashMap<String, String>,
        schema: &StructType,
    ) -> Vec<CheckConstraint> {
        cfg.iter()
            .filter(|(key, _)| key.to_ascii_lowercase().starts_with(DELTA_CONSTRAINT_PREFIX))
            .map(|(key, sql)| {
                let name = key[DELTA_CONSTRAINT_PREFIX.len()..].to_string();
                CheckConstraint::new(name, sql.clone(), schema)
            })
            .collect()
    }

    /// The constraint name (the part after `delta.constraints.`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The raw SQL expression string, for connectors that evaluate constraints themselves.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }

    /// Whether kernel parsed this constraint into a predicate it can evaluate.
    pub fn is_kernel_parsable(&self) -> bool {
        self.parsed.is_some()
    }
}

/// A ready-to-run checker over all of a table's CHECK constraints.
///
/// All constraints are AND-fused into one predicate, so [`check`](Self::check) is a single
/// evaluation per batch regardless of how many constraints the table has. The per-constraint
/// evaluators are used only to name the offending constraint when a violation occurs.
pub struct ConstraintChecker {
    combined: Arc<dyn PredicateEvaluator>,
    individuals: Vec<(String, Arc<dyn PredicateEvaluator>)>,
}

impl ConstraintChecker {
    /// Parse and AND-fuse `constraints`, building evaluators with `handler`. Returns an error if
    /// any constraint is not kernel-parsable: a writer that cannot evaluate a declared constraint
    /// must refuse the write rather than skip enforcement.
    fn try_new(
        constraints: Vec<CheckConstraint>,
        schema: SchemaRef,
        handler: &dyn EvaluationHandler,
    ) -> DeltaResult<Self> {
        let mut named = Vec::with_capacity(constraints.len());
        for c in constraints {
            let predicate = c.parsed.ok_or_else(|| {
                Error::unsupported(format!(
                    "CHECK constraint '{}' is not supported by kernel's SQL parser: {}",
                    c.name, c.raw_sql
                ))
            })?;
            named.push((c.name, predicate));
        }
        let conjunction = Predicate::and_from(named.iter().map(|(_, p)| p.clone()));
        let combined = handler.new_predicate_evaluator(schema.clone(), Arc::new(conjunction))?;
        let individuals = named
            .into_iter()
            .map(|(name, p)| {
                Ok((
                    name,
                    handler.new_predicate_evaluator(schema.clone(), Arc::new(p))?,
                ))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Self {
            combined,
            individuals,
        })
    }

    /// Validate a logical `batch`. Returns `Ok(())` if every row satisfies every constraint, or an
    /// error naming the first violated constraint and the offending row index (NULL and FALSE both
    /// count as violations).
    pub fn check(&self, batch: &dyn EngineData) -> DeltaResult<()> {
        // Hot path: one evaluation of the fused conjunction.
        if first_violation(self.combined.evaluate(batch)?.as_ref())?.is_none() {
            return Ok(());
        }
        // Cold path: identify which constraint failed (the write is aborting regardless).
        for (name, evaluator) in &self.individuals {
            if let Some(row) = first_violation(evaluator.evaluate(batch)?.as_ref())? {
                return Err(Error::generic(format!(
                    "CHECK constraint '{name}' violated at row {row}"
                )));
            }
        }
        Ok(())
    }
}

/// Return the index of the first row whose `"output"` boolean column is not literally TRUE (i.e.
/// FALSE or NULL), or `None` if every row passes. Reads the column via the visitor pattern with a
/// nullable getter, so NULL is treated as a violation rather than an error.
fn first_violation(result: &dyn EngineData) -> DeltaResult<Option<usize>> {
    let mut visitor = ConstraintResultVisitor::default();
    visitor.visit_rows_of(result)?;
    Ok(visitor.first_violation)
}

#[derive(Default)]
struct ConstraintResultVisitor {
    rows_seen: usize,
    first_violation: Option<usize>,
}

impl RowVisitor for ConstraintResultVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("output")], vec![DataType::BOOLEAN]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let passes: Option<bool> = getters[0].get_opt(i, "constraint.output")?;
            // A row passes only if the predicate is literally TRUE; NULL and FALSE both fail.
            if passes != Some(true) && self.first_violation.is_none() {
                self.first_violation = Some(self.rows_seen + i);
            }
        }
        self.rows_seen += row_count;
        Ok(())
    }
}

impl Transaction<ExistingTable> {
    /// Discover all CHECK constraints declared on the table being written. Each is eagerly parsed;
    /// constraints kernel cannot parse have [`CheckConstraint::is_kernel_parsable`] false.
    pub fn check_constraints(&self) -> DeltaResult<Vec<CheckConstraint>> {
        let snapshot = self.read_snapshot()?;
        Ok(CheckConstraint::from_configuration(
            snapshot.metadata_configuration(),
            snapshot.schema().as_ref(),
        ))
    }

    /// Build a [`ConstraintChecker`] over the table's CHECK constraints using `handler` for
    /// evaluation. Returns an error if any constraint cannot be parsed by kernel.
    pub fn constraint_checker(
        &self,
        handler: &dyn EvaluationHandler,
    ) -> DeltaResult<ConstraintChecker> {
        let schema = self.read_snapshot()?.schema();
        ConstraintChecker::try_new(self.check_constraints()?, schema, handler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::BooleanArray;
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::schema::StructField;

    fn schema() -> StructType {
        StructType::new_unchecked([StructField::nullable("age", DataType::INTEGER)])
    }

    fn config(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn from_configuration_extracts_only_constraint_keys_case_insensitively() {
        let cfg = config(&[
            ("delta.constraints.age_eq", "age = 1"),
            ("Delta.Constraints.Mixed", "age = 2"), // prefix matched case-insensitively
            ("delta.appendOnly", "true"),           // not a constraint
        ]);
        let mut found = CheckConstraint::from_configuration(&cfg, &schema());
        found.sort_by(|a, b| a.name.cmp(&b.name));
        let names: Vec<_> = found.iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["Mixed", "age_eq"]);
    }

    #[test]
    fn is_kernel_parsable_reflects_parser_support() {
        let parsable = CheckConstraint::new("a".into(), "age = 1".into(), &schema());
        let not_parsable = CheckConstraint::new("a".into(), "age > 1".into(), &schema());
        assert!(parsable.is_kernel_parsable());
        assert!(!not_parsable.is_kernel_parsable());
    }

    fn bool_batch(values: Vec<Option<bool>>) -> Box<dyn EngineData> {
        let array = BooleanArray::from(values);
        let arrow_schema =
            ArrowSchema::new(vec![Field::new("output", ArrowDataType::Boolean, true)]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(array)]).unwrap();
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn first_violation_passes_when_all_true() {
        let batch = bool_batch(vec![Some(true), Some(true), Some(true)]);
        assert_eq!(first_violation(batch.as_ref()).unwrap(), None);
    }

    #[test]
    fn first_violation_flags_first_false_row() {
        let batch = bool_batch(vec![Some(true), Some(false), Some(false)]);
        assert_eq!(first_violation(batch.as_ref()).unwrap(), Some(1));
    }

    #[test]
    fn first_violation_treats_null_as_violation() {
        let batch = bool_batch(vec![Some(true), None, Some(true)]);
        assert_eq!(first_violation(batch.as_ref()).unwrap(), Some(1));
    }
}
