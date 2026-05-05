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

mod pushdown;

use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
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
