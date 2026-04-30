//! Query GNU recutils `.rec` files with SQL via Apache DataFusion.
//!
//! [`RecTableProvider`] exposes one record set as a `TableProvider`. The rec
//! file's source is held inside the provider so each scan can re-parse it
//! and apply a selection-expression filter at the librec layer when
//! DataFusion pushes predicates down.
//!
//! Use [`RecTableProvider::open_all`] to surface every `%rec:` record set in
//! a file as a separate provider; the source text is shared cheaply via
//! `Arc<str>` across providers built from the same file.
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
use recutils_rs::arrow::{rec_to_filtered_batch, rec_to_record_batch};

use crate::pushdown::pushdown_for;

#[derive(Debug)]
pub struct RecTableProvider {
    source: Arc<str>,
    record_type: String,
    schema: SchemaRef,
    cached: RecordBatch,
}

impl RecTableProvider {
    pub fn open<P: AsRef<Path>>(
        path: P,
        record_type: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let source: Arc<str> = Arc::from(fs::read_to_string(path.as_ref())?);
        Self::from_source(source, record_type)
    }

    /// Open every record set in the file as its own provider, keyed by the
    /// `%rec:` type name. Anonymous record sets (no descriptor) and
    /// descriptors with an empty `%rec` are skipped with a warning.
    pub fn open_all<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<(String, RecTableProvider)>, Box<dyn std::error::Error>> {
        let source: Arc<str> = Arc::from(fs::read_to_string(path.as_ref())?);
        let names = enumerate_types(&source)?;
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let provider = Self::from_source(Arc::clone(&source), &name)?;
            out.push((name, provider));
        }
        Ok(out)
    }

    fn from_source(
        source: Arc<str>,
        record_type: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut db = Db::parse_str(&source)?;
        let (schema, cached) = rec_to_record_batch(&mut db, record_type)?;
        Ok(Self {
            source,
            record_type: record_type.to_string(),
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
        let batch = rec_to_filtered_batch(
            &mut db,
            &self.record_type,
            &self.schema,
            &selection_expression,
        )
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

fn enumerate_types(source: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut db = Db::parse_str(source)?;
    let n = db.num_rsets();
    let mut names = Vec::with_capacity(n);
    for i in 0..n {
        let Some(rset) = db.rset_at(i) else { continue };
        let Some(descriptor) = rset.descriptor() else {
            log::warn!("rset at index {i} has no descriptor; skipping (no table name)");
            continue;
        };
        // %rec value can be `<name>` or `<name> <key>`; take the first token.
        let name = descriptor
            .fields()
            .find(|f| f.name() == "%rec")
            .and_then(|f| f.value().split_whitespace().next().map(str::to_string));
        match name {
            Some(n) if !n.is_empty() => names.push(n),
            _ => log::warn!("rset at index {i} has no %rec name; skipping"),
        }
    }
    Ok(names)
}
