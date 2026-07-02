//! Query GNU recutils `.rec` files with SQL via Apache DataFusion.
//!
//! [`RecTableProvider`] exposes one record set as a `TableProvider`. The rec
//! file's source is held inside the provider so each scan can re-parse it
//! and apply a selection-expression filter at the librec layer when
//! DataFusion pushes predicates down.
//!
//! Use [`RecTableProvider::open_all`] to surface every record set in a file
//! as a separate provider; the source text is shared cheaply via `Arc<str>`
//! across providers built from the same file. Anonymous record sets (no
//! `%rec:` descriptor — e.g. files produced by converting a CSV) are still
//! reachable: they're named `rec` (or `rec_<index>` when there are several
//! or the name would clash).
//!
//! Filter pushdown is best-effort: predicates that fully translate to a
//! selection expression are reported as `Exact` (librec evaluates them and
//! DataFusion does not re-check); predicates whose top-level conjunction
//! has *some* translatable conjuncts are reported as `Inexact` (we push
//! the relaxation, DataFusion re-checks the original); everything else is
//! `Unsupported`.

pub mod format;
mod pushdown;
pub mod sink;

pub use format::{RecFileFormat, RecFileFormatFactory};
pub use sink::RecSink;

use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use recutils_rs::Db;
use recutils_rs::SelectionExpression;
use recutils_rs::arrow::{rec_to_filtered_batch_from_rset, rec_to_record_batch_from_rset};

use crate::pushdown::pushdown_for;

#[derive(Debug)]
pub struct RecTableProvider {
    source: Arc<str>,
    rset_index: usize,
    schema: SchemaRef,
    cached: RecordBatch,
}

impl RecTableProvider {
    pub fn open<P: AsRef<Path>>(
        path: P,
        record_type: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let source: Arc<str> = Arc::from(fs::read_to_string(path.as_ref())?);
        let idx = enumerate_rsets(&source)?
            .into_iter()
            .find(|(name, _)| name == record_type)
            .map(|(_, idx)| idx)
            .ok_or_else(|| format!("no record set named {record_type:?}"))?;
        Self::from_source(source, idx)
    }

    /// Open every record set in the file as its own provider. Named record
    /// sets use the `%rec:` type name; anonymous record sets (no descriptor,
    /// as produced by e.g. converting a CSV) are named `rec`, with a
    /// `rec_<index>` fallback when that would collide or several anonymous
    /// rsets exist.
    pub fn open_all<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<(String, RecTableProvider)>, Box<dyn std::error::Error>> {
        let source: Arc<str> = Arc::from(fs::read_to_string(path.as_ref())?);
        let entries = enumerate_rsets(&source)?;
        let mut out = Vec::with_capacity(entries.len());
        for (name, idx) in entries {
            let provider = Self::from_source(Arc::clone(&source), idx)?;
            out.push((name, provider));
        }
        Ok(out)
    }

    fn from_source(
        source: Arc<str>,
        rset_index: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut db = Db::parse_str(&source)?;
        let rset = db
            .rset_at(rset_index)
            .ok_or_else(|| format!("rset index {rset_index} out of range"))?;
        let (schema, cached) = rec_to_record_batch_from_rset(&rset)?;
        Ok(Self {
            source,
            rset_index,
            schema,
            cached,
        })
    }

    fn batches_for(&self, filters: &[Expr]) -> DfResult<Vec<RecordBatch>> {
        let clauses: Vec<String> = filters
            .iter()
            .filter_map(|f| pushdown_for(f, self.schema.as_ref()).map(|(s, _)| s))
            .collect();

        if clauses.is_empty() {
            return Ok(vec![self.cached.clone()]);
        }

        let combined = if clauses.len() == 1 {
            clauses.into_iter().next().unwrap()
        } else {
            clauses
                .into_iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" && ")
        };

        let selection_expression = match SelectionExpression::compile(&combined, false) {
            Ok(s) => {
                log::debug!("pushed selection expression to librec: {combined}");
                s
            }
            Err(e) => {
                log::warn!(
                    "selection expression compile failed for pushdown expression {combined:?}: \
                     {e}; falling back to unfiltered scan"
                );
                return Ok(vec![self.cached.clone()]);
            }
        };

        let mut db = Db::parse_str(&self.source)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let rset = db.rset_at(self.rset_index).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "rset index {} disappeared on re-parse",
                self.rset_index
            ))
        })?;
        let batch =
            rec_to_filtered_batch_from_rset(&rset, &self.schema, &selection_expression)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(vec![batch])
    }
}

#[async_trait]
impl TableProvider for RecTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match pushdown_for(f, self.schema.as_ref()) {
                Some((_, true)) => TableProviderFilterPushDown::Exact,
                Some((_, false)) => TableProviderFilterPushDown::Inexact,
                None => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let batches = self.batches_for(filters)?;
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}

/// A single SQL table backed by the *same* record set drawn from several
/// files. Each child [`RecTableProvider`] becomes one execution partition,
/// so DataFusion scans the files concurrently and the rows read as one
/// `UNION ALL`.
///
/// The children need not agree on their column sets — records omit fields,
/// so two files of the same `%rec:` type can produce different schemas.
/// [`MultiRecTableProvider::new`] computes a merged schema (union of field
/// names in first-appearance order); at scan time each partition is
/// projected onto it, null-filling absent columns and casting columns whose
/// type was widened during the merge (numeric ⊕ numeric → `Float64`, any
/// other conflict → `Utf8`).
///
/// Filter pushdown is the *weakest* verdict across children: a predicate is
/// `Exact` only if every child can push it, else it degrades to `Inexact` /
/// `Unsupported` and DataFusion re-checks above the provider.
#[derive(Debug)]
pub struct MultiRecTableProvider {
    children: Vec<RecTableProvider>,
    schema: SchemaRef,
}

impl MultiRecTableProvider {
    /// Build a partitioned table from ≥1 providers reading the same record
    /// set across files. Errors if `children` is empty.
    pub fn new(children: Vec<RecTableProvider>) -> Result<Self, Box<dyn std::error::Error>> {
        if children.is_empty() {
            return Err("MultiRecTableProvider requires at least one child".into());
        }
        let schema = merge_schemas(children.iter().map(|c| c.schema.as_ref()));
        Ok(Self { children, schema })
    }
}

#[async_trait]
impl TableProvider for MultiRecTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                // Weakest verdict across children: a child that lacks the
                // filtered column (or can't translate it) drags the whole
                // table down, forcing DataFusion to re-check.
                self.children
                    .iter()
                    .map(|c| match pushdown_for(f, c.schema.as_ref()) {
                        Some((_, true)) => TableProviderFilterPushDown::Exact,
                        Some((_, false)) => TableProviderFilterPushDown::Inexact,
                        None => TableProviderFilterPushDown::Unsupported,
                    })
                    .min_by_key(pushdown_rank)
                    .unwrap_or(TableProviderFilterPushDown::Unsupported)
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let projected = child
                .batches_for(filters)?
                .into_iter()
                .map(|b| project_batch(&b, &self.schema))
                .collect::<DfResult<Vec<_>>>()?;
            partitions.push(projected);
        }
        let mem = MemTable::try_new(Arc::clone(&self.schema), partitions)?;
        mem.scan(state, projection, filters, limit).await
    }
}

/// Rank for `min_by_key` on pushdown verdicts: lower = weaker.
fn pushdown_rank(v: &TableProviderFilterPushDown) -> u8 {
    match v {
        TableProviderFilterPushDown::Unsupported => 0,
        TableProviderFilterPushDown::Inexact => 1,
        TableProviderFilterPushDown::Exact => 2,
    }
}

/// Union the fields of several schemas by name, keeping first-appearance
/// order. All merged fields are nullable (a file may lack any field). When
/// the same name appears with differing types, widen: numeric ⊕ numeric →
/// `Float64`, any other conflict → `Utf8`.
fn merge_schemas<'a>(schemas: impl Iterator<Item = &'a Schema>) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for schema in schemas {
        for f in schema.fields() {
            match seen.get(f.name()) {
                None => {
                    seen.insert(f.name().clone(), fields.len());
                    fields.push(Field::new(f.name(), f.data_type().clone(), true));
                }
                Some(&idx) => {
                    let widened = widen(fields[idx].data_type(), f.data_type());
                    if &widened != fields[idx].data_type() {
                        fields[idx] = Field::new(f.name(), widened, true);
                    }
                }
            }
        }
    }
    Arc::new(Schema::new(fields))
}

fn widen(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    let numeric = |t: &DataType| matches!(t, DataType::Int64 | DataType::Float64);
    if numeric(a) && numeric(b) {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

/// Reproject `batch` onto `target`: reorder columns to match, cast columns
/// whose type was widened, and null-fill fields the batch lacks.
fn project_batch(batch: &RecordBatch, target: &SchemaRef) -> DfResult<RecordBatch> {
    let src = batch.schema();
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        match src.index_of(field.name()) {
            Ok(i) => {
                let col = batch.column(i);
                if col.data_type() == field.data_type() {
                    columns.push(Arc::clone(col));
                } else {
                    columns.push(cast(col, field.data_type()).map_err(DataFusionError::from)?);
                }
            }
            Err(_) => columns.push(new_null_array(field.data_type(), batch.num_rows())),
        }
    }
    RecordBatch::try_new(Arc::clone(target), columns).map_err(DataFusionError::from)
}

/// Walk every rset in the file and return `(table_name, rset_index)` for
/// each. Anonymous rsets (no `%rec:` descriptor — the shape produced by
/// `csv2rec` and friends) are surfaced as `rec`, falling back to
/// `rec_<index>` if there are several or the simple name would clash with
/// an explicitly named rset.
fn enumerate_rsets(
    source: &str,
) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
    let mut db = Db::parse_str(source)?;
    let n = db.num_rsets();

    let mut entries: Vec<(Option<String>, usize)> = Vec::with_capacity(n);
    for i in 0..n {
        let Some(rset) = db.rset_at(i) else { continue };
        // %rec value can be `<name>` or `<name> <key>`; take the first token.
        let name = rset.descriptor().and_then(|desc| {
            desc.fields()
                .find(|f| f.name() == "%rec")
                .and_then(|f| f.value().split_whitespace().next().map(str::to_string))
                .filter(|s| !s.is_empty())
        });
        entries.push((name, i));
    }

    let anon_count = entries.iter().filter(|(n, _)| n.is_none()).count();
    let named: std::collections::HashSet<String> =
        entries.iter().filter_map(|(n, _)| n.clone()).collect();

    let mut out = Vec::with_capacity(entries.len());
    for (name, idx) in entries {
        let resolved = match name {
            Some(n) => n,
            None => {
                if anon_count == 1 && !named.contains("rec") {
                    "rec".to_string()
                } else {
                    let candidate = format!("rec_{idx}");
                    log::info!(
                        "anonymous record set at index {idx} surfaced as table {candidate:?}"
                    );
                    candidate
                }
            }
        };
        out.push((resolved, idx));
    }
    Ok(out)
}
