//! [`DataSink`] implementation that writes a query's output to a `.rec` file.
//!
//! Constructed by [`crate::format::RecFileFormat::create_writer_physical_plan`]
//! when DataFusion plans a `COPY ... TO '<path>.rec'` statement. The sink
//! drains the input stream, hands the buffered batches to
//! `recutils_rs::arrow::record_batches_to_rec_string` (which emits the
//! `%rec:` / `%type:` / `%mandatory:` descriptor block), and writes the
//! resulting text to the local filesystem in one shot.

use std::any::Any;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{exec_err, not_impl_err};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use recutils_rs::arrow::record_batches_to_rec_string;

#[derive(Debug)]
pub struct RecSink {
    path: PathBuf,
    record_type: String,
    schema: SchemaRef,
    insert_op: InsertOp,
}

impl RecSink {
    pub fn new(
        path: PathBuf,
        record_type: String,
        schema: SchemaRef,
        insert_op: InsertOp,
    ) -> Self {
        Self {
            path,
            record_type,
            schema,
            insert_op,
        }
    }
}

impl DisplayAs for RecSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RecSink(path={}, record_type={}, insert_op={:?})",
                    self.path.display(),
                    self.record_type,
                    self.insert_op
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: rec")?;
                writeln!(f, "record_type: {}", self.record_type)?;
                write!(f, "file={}", self.path.display())
            }
        }
    }
}

#[async_trait]
impl DataSink for RecSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        match self.insert_op {
            InsertOp::Append => {
                // DataFusion's COPY currently always sets InsertOp::Append
                // regardless of user syntax (see physical_planner.rs in DF
                // 53). Treating Append as "create new file" — refuse to
                // clobber. Users who want to replace should remove the file
                // first.
                if self.path.exists() {
                    return exec_err!(
                        "rec writer: refusing to write {:?} (file exists)",
                        self.path
                    );
                }
            }
            InsertOp::Overwrite => {}
            InsertOp::Replace => {
                return not_impl_err!("rec writer does not implement REPLACE INTO");
            }
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows: u64 = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            total_rows += batch.num_rows() as u64;
            batches.push(batch);
        }

        let text = record_batches_to_rec_string(&self.record_type, &self.schema, &batches)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "rec serialization failed: {e}"
                ))
            })?;

        fs::write(&self.path, text).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "writing {:?} failed: {e}",
                self.path
            ))
        })?;

        Ok(total_rows)
    }
}
