//! [`FileFormat`] / [`FileFormatFactory`] for `.rec` files.
//!
//! Wires `recutils-rs`'s Arrow→rec writer into DataFusion's `COPY TO`
//! statement. Reads are intentionally **not** supported through this
//! `FileFormat` — use [`crate::RecTableProvider`] directly for that. The
//! split keeps the read path's filter-pushdown plumbing out of the write
//! path's `FileFormat::create_physical_plan`, which has a different shape.
//!
//! Usage from SQL:
//!
//! ```sql
//! COPY (SELECT title, year FROM book WHERE year > 2000)
//! TO '/tmp/recent.rec'
//! STORED AS REC
//! OPTIONS ('record_type' 'Book');
//! ```
//!
//! Options:
//! - `record_type` — required; the `%rec:` type name of the emitted rset.
//!
//! Overwrite semantics follow DataFusion's `InsertOp`: plain `COPY` (Append)
//! refuses to overwrite an existing file; `COPY OVERWRITE` replaces it.

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Statistics, not_impl_err, plan_err};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result as DfResult;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;

use crate::sink::RecSink;

const REC_EXT: &str = "rec";
const OPT_RECORD_TYPE: &str = "record_type";

#[derive(Debug, Default)]
pub struct RecFileFormatFactory;

impl RecFileFormatFactory {
    pub fn new() -> Self {
        Self
    }
}

impl GetExt for RecFileFormatFactory {
    fn get_ext(&self) -> String {
        REC_EXT.to_string()
    }
}

impl FileFormatFactory for RecFileFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> DfResult<Arc<dyn FileFormat>> {
        let mut record_type: Option<String> = None;
        for (k, v) in format_options {
            // DataFusion's SQL planner prefixes COPY options with `format.`
            // before handing them to the file format factory. Accept either
            // shape so callers can write OPTIONS ('record_type' 'X').
            let key = k
                .strip_prefix("format.")
                .unwrap_or(k.as_str())
                .to_lowercase();
            match key.as_str() {
                OPT_RECORD_TYPE => record_type = Some(v.clone()),
                other => {
                    return plan_err!(
                        "unknown rec format option {other:?}; supported: {OPT_RECORD_TYPE:?}"
                    );
                }
            }
        }
        Ok(Arc::new(RecFileFormat { record_type }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(RecFileFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct RecFileFormat {
    /// Required for writes; resolved from `OPTIONS ('record_type' '<name>')`.
    /// `None` when the format is built via [`FileFormatFactory::default`] —
    /// `create_writer_physical_plan` rejects writes in that case.
    record_type: Option<String>,
}

#[async_trait]
impl FileFormat for RecFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        REC_EXT.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> DfResult<String> {
        Ok(REC_EXT.to_string())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> DfResult<SchemaRef> {
        not_impl_err!(
            "rec FileFormat is write-only; register a RecTableProvider per rset for reads"
        )
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> DfResult<Statistics> {
        not_impl_err!(
            "rec FileFormat is write-only; register a RecTableProvider per rset for reads"
        )
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "rec FileFormat is write-only; register a RecTableProvider per rset for reads"
        )
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let record_type = self.record_type.clone().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "COPY to .rec requires OPTIONS ('{OPT_RECORD_TYPE}' '<rec-type-name>')"
            ))
        })?;
        let path = resolve_local_path(&conf)?;
        let schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(RecSink::new(path, record_type, schema, conf.insert_op));
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self, _table_schema: TableSchema) -> Arc<dyn FileSource> {
        // Intentionally fail loudly: this FileFormat is write-only. Reads
        // belong in RecTableProvider, which exposes per-rset providers with
        // selection-expression pushdown. If you see this panic, you've
        // routed a SELECT through the COPY-targeted format by mistake.
        panic!(
            "RecFileFormat::file_source called: rec format is write-only via DataFusion's COPY. \
             Use RecTableProvider for reads."
        )
    }
}

/// Pull a local filesystem path out of a [`FileSinkConfig`]. Rejects any URL
/// with a non-`file` scheme — object_store-backed writes are out of scope.
fn resolve_local_path(conf: &FileSinkConfig) -> DfResult<PathBuf> {
    let raw = conf.original_url.as_str();
    if let Some(rest) = raw.strip_prefix("file://") {
        return Ok(PathBuf::from(rest));
    }
    if let Some(idx) = raw.find("://") {
        let scheme = &raw[..idx];
        return not_impl_err!(
            "rec writer only supports local filesystem paths; got scheme {scheme:?}"
        );
    }
    Ok(PathBuf::from(raw))
}
