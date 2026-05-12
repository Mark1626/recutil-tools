//! Convert rec records into Apache Arrow `RecordBatch`es.
//!
//! Gated behind the `arrow` cargo feature. Honors `%type:` declarations
//! from the rset descriptor; untyped fields fall back to `Utf8`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::rset::Rset;
use crate::{Db, OwnedRset, Record, SelectionExpression};

pub fn rec_to_record_batch(
    db: &mut Db,
    record_type: &str,
) -> Result<(Arc<Schema>, RecordBatch), Box<dyn std::error::Error>> {
    let rset = db
        .rset_by_type(record_type)
        .ok_or_else(|| format!("no record set of type {record_type:?}"))?;
    rec_to_record_batch_from_rset(&rset)
}

/// Build the `(schema, batch)` for an arbitrary [`Rset`], including
/// anonymous record sets that have no `%rec:` descriptor (so they can't be
/// looked up by [`Db::rset_by_type`]).
pub fn rec_to_record_batch_from_rset(
    rset: &Rset<'_>,
) -> Result<(Arc<Schema>, RecordBatch), Box<dyn std::error::Error>> {
    let mut declared_types: HashMap<String, String> = HashMap::new();
    if let Some(desc) = rset.descriptor() {
        for f in desc.fields() {
            if f.name() == "%type" {
                if let Some((field, ty)) = split_type_decl(&f.value()) {
                    declared_types.insert(field, ty);
                }
            }
        }
    }

    let (column_order, rows) = collect_rows_from_rset(rset)?;
    let schema = build_schema(&column_order, &declared_types);
    let columns = build_columns(&schema, &rows);
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    Ok((schema, batch))
}

/// Build a [`RecordBatch`] for the records of `record_type` that match the
/// given selection expression, using the caller-provided `schema` (so the
/// column set stays stable even when the filter excludes every record that
/// has a particular field).
pub fn rec_to_filtered_batch(
    db: &mut Db,
    record_type: &str,
    schema: &Arc<Schema>,
    selection_expression: &SelectionExpression,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let rset = db
        .rset_by_type(record_type)
        .ok_or_else(|| format!("no record set of type {record_type:?}"))?;
    rec_to_filtered_batch_from_rset(&rset, schema, selection_expression)
}

/// Same as [`rec_to_filtered_batch`] but for an arbitrary [`Rset`].
pub fn rec_to_filtered_batch_from_rset(
    rset: &Rset<'_>,
    schema: &Arc<Schema>,
    selection_expression: &SelectionExpression,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut rows: Vec<HashMap<String, String>> = Vec::new();
    for (i, record) in rset.records().enumerate() {
        if !selection_expression.matches(&record) {
            continue;
        }
        let mut row: HashMap<String, String> = HashMap::new();
        for f in record.fields() {
            let name = f.name();
            if name.starts_with('%') {
                continue;
            }
            if row.contains_key(&name) {
                return Err(format!(
                    "field {:?} repeated in record {} (1-based); use a List<T> mapping (not yet supported) or remove the repeat",
                    name,
                    i + 1
                )
                .into());
            }
            row.insert(name.clone(), f.value());
        }
        rows.push(row);
    }
    let columns = build_columns(schema, &rows);
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

pub fn split_type_decl(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim();
    let (name, rest) = trimmed.split_once(char::is_whitespace)?;
    Some((name.trim().to_string(), rest.trim().to_string()))
}

pub fn collect_rows(
    db: &mut Db,
    record_type: &str,
) -> Result<(Vec<String>, Vec<HashMap<String, String>>), Box<dyn std::error::Error>> {
    let rset = db
        .rset_by_type(record_type)
        .ok_or_else(|| format!("no record set of type {record_type:?}"))?;
    collect_rows_from_rset(&rset)
}

pub fn collect_rows_from_rset(
    rset: &Rset<'_>,
) -> Result<(Vec<String>, Vec<HashMap<String, String>>), Box<dyn std::error::Error>> {
    let mut column_order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut rows: Vec<HashMap<String, String>> = Vec::new();

    for (i, record) in rset.records().enumerate() {
        let mut row: HashMap<String, String> = HashMap::new();
        for f in record.fields() {
            let name = f.name();
            if name.starts_with('%') {
                continue;
            }
            if row.contains_key(&name) {
                return Err(format!(
                    "field {:?} repeated in record {} (1-based); use a List<T> mapping (not yet supported) or remove the repeat",
                    name,
                    i + 1
                )
                .into());
            }
            row.insert(name.clone(), f.value());
            if seen.insert(name.clone()) {
                column_order.push(name);
            }
        }
        rows.push(row);
    }
    Ok((column_order, rows))
}

pub fn build_schema(
    column_order: &[String],
    declared: &HashMap<String, String>,
) -> Arc<Schema> {
    let fields: Vec<Field> = column_order
        .iter()
        .map(|name| {
            let dt = match declared.get(name) {
                Some(t) => map_rec_type(t),
                None => {
                    log::info!("no %type for field {name:?}; falling back to Utf8");
                    DataType::Utf8
                }
            };
            Field::new(name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

pub fn map_rec_type(t: &str) -> DataType {
    match t.split_whitespace().next().unwrap_or("") {
        "int" | "range" => DataType::Int64,
        "real" => DataType::Float64,
        "bool" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

pub fn build_columns(schema: &Schema, rows: &[HashMap<String, String>]) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| build_column(f, rows))
        .collect()
}

pub fn build_column(field: &Field, rows: &[HashMap<String, String>]) -> ArrayRef {
    let name = field.name();
    match field.data_type() {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(name).map(|s| s.trim()) {
                    Some(s) if s.is_empty() => b.append_null(),
                    Some(s) => match s.parse::<i64>() {
                        Ok(v) => b.append_value(v),
                        Err(_) => {
                            log::warn!("field {name:?}: cannot parse {s:?} as int; nulled");
                            b.append_null();
                        }
                    },
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.get(name).map(|s| s.trim()) {
                    Some(s) if s.is_empty() => b.append_null(),
                    Some(s) => match s.parse::<f64>() {
                        Ok(v) => b.append_value(v),
                        Err(_) => {
                            log::warn!("field {name:?}: cannot parse {s:?} as real; nulled");
                            b.append_null();
                        }
                    },
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get(name).map(|s| s.trim()) {
                    Some(s) if s.is_empty() => b.append_null(),
                    Some(s) => match parse_rec_bool(s) {
                        Some(v) => b.append_value(v),
                        None => {
                            log::warn!("field {name:?}: cannot parse {s:?} as bool; nulled");
                            b.append_null();
                        }
                    },
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.get(name) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        other => panic!("unsupported arrow type {other:?}"),
    }
}

pub fn parse_rec_bool(s: &str) -> Option<bool> {
    match s {
        "yes" | "true" | "1" => Some(true),
        "no" | "false" | "0" => Some(false),
        _ => None,
    }
}

/// Serialize `batches` as a `.rec` file body containing a single record set
/// of type `record_type`. The descriptor block carries `%rec:`, one `%type:`
/// line per non-Utf8 column, and one `%mandatory:` line per non-nullable
/// Arrow field. Null values are omitted from the produced records (rec
/// convention: absent field == null).
///
/// Each batch's column count and layout must match `schema`. Unsupported
/// Arrow types (anything beyond Int64 / Float64 / Boolean / Utf8) return an
/// error rather than producing a lossy serialization.
pub fn record_batches_to_rec_string(
    record_type: &str,
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error>> {
    if record_type.is_empty() {
        return Err("record_type must be a non-empty rec type name".into());
    }

    let mut db = Db::new();
    let mut rset = OwnedRset::new();
    rset.set_descriptor(build_descriptor(record_type, schema)?);

    for batch in batches {
        if batch.num_columns() != schema.fields().len() {
            return Err(format!(
                "batch has {} columns but schema has {}",
                batch.num_columns(),
                schema.fields().len()
            )
            .into());
        }
        for row in 0..batch.num_rows() {
            let mut record = Record::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let array = batch.column(col_idx).as_ref();
                if array.is_null(row) {
                    continue;
                }
                let value = format_arrow_value(field, array, row)?;
                record.append_field(field.name(), &value)?;
            }
            rset.append_record(record)?;
        }
    }

    db.append_rset(rset)?;
    Ok(db.to_rec_string()?)
}

fn build_descriptor(
    record_type: &str,
    schema: &Schema,
) -> Result<Record, Box<dyn std::error::Error>> {
    let mut desc = Record::new();
    desc.append_field("%rec", record_type)?;
    for field in schema.fields() {
        if let Some(rec_ty) = map_arrow_to_rec_type(field.data_type())? {
            desc.append_field("%type", &format!("{} {}", field.name(), rec_ty))?;
        }
    }
    for field in schema.fields() {
        if !field.is_nullable() {
            desc.append_field("%mandatory", field.name())?;
        }
    }
    Ok(desc)
}

/// Inverse of [`map_rec_type`]. Returns `Ok(None)` for `Utf8`, since rec's
/// untyped default is string and emitting `%type: <name> string` would be
/// noise. Returns `Err` for Arrow types we don't know how to round-trip.
pub fn map_arrow_to_rec_type(
    dt: &DataType,
) -> Result<Option<&'static str>, Box<dyn std::error::Error>> {
    Ok(match dt {
        DataType::Int64 => Some("int"),
        DataType::Float64 => Some("real"),
        DataType::Boolean => Some("bool"),
        DataType::Utf8 => None,
        other => {
            return Err(format!("unsupported arrow type {other:?} for rec output").into());
        }
    })
}

pub fn format_arrow_value(
    field: &Field,
    array: &dyn Array,
    row: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    match field.data_type() {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected Int64Array")?;
            Ok(a.value(row).to_string())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("expected Float64Array")?;
            Ok(format_rec_float(a.value(row)))
        }
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("expected BooleanArray")?;
            Ok(if a.value(row) { "yes" } else { "no" }.to_string())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("expected StringArray")?;
            Ok(a.value(row).to_string())
        }
        other => Err(format!("unsupported arrow type {other:?} for rec output").into()),
    }
}

/// Format an `f64` so integer-valued finite floats serialize as `"1.0"`
/// rather than `"1"`. Keeps round-trips stable when the file is read back
/// without `%type: real` (e.g. by a human-trimmed descriptor).
fn format_rec_float(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 {
        format!("{f:.1}")
    } else {
        f.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Title", DataType::Utf8, false),
            Field::new("Year", DataType::Int64, true),
            Field::new("Price", DataType::Float64, true),
            Field::new("InPrint", DataType::Boolean, true),
        ]))
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        let titles: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Refactoring"),
            Some("TDD"),
        ]));
        let years: ArrayRef = Arc::new(Int64Array::from(vec![Some(1999), None]));
        let prices: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(42.0), Some(19.95)]));
        let in_print: ArrayRef =
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)]));
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![titles, years, prices, in_print],
        )
        .unwrap()
    }

    #[test]
    fn descriptor_carries_types_and_mandatory() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        let text =
            record_batches_to_rec_string("Book", &schema, std::slice::from_ref(&batch))
                .unwrap();
        assert!(text.contains("%rec: Book"));
        assert!(text.contains("%type: Year int"));
        assert!(text.contains("%type: Price real"));
        assert!(text.contains("%type: InPrint bool"));
        // Utf8 columns get no %type: line.
        assert!(!text.contains("%type: Title"));
        // Only the non-nullable Arrow field becomes %mandatory.
        assert!(text.contains("%mandatory: Title"));
        assert!(!text.contains("%mandatory: Year"));
    }

    #[test]
    fn integer_valued_float_keeps_decimal() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        let text =
            record_batches_to_rec_string("Book", &schema, std::slice::from_ref(&batch))
                .unwrap();
        assert!(text.contains("Price: 42.0"));
        assert!(text.contains("Price: 19.95"));
    }

    #[test]
    fn bool_writes_yes_no() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        let text =
            record_batches_to_rec_string("Book", &schema, std::slice::from_ref(&batch))
                .unwrap();
        assert!(text.contains("InPrint: yes"));
        assert!(text.contains("InPrint: no"));
    }

    #[test]
    fn null_field_is_omitted() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        let text =
            record_batches_to_rec_string("Book", &schema, std::slice::from_ref(&batch))
                .unwrap();
        // The second record has Year=null; it should not emit a Year field.
        // Anchor on the unique Title "TDD" to find the second record block.
        let tdd_idx = text.find("Title: TDD").expect("TDD record present");
        let tdd_block = &text[tdd_idx..];
        // Stop at the next blank-line-prefixed record or EOF.
        let block_end = tdd_block.find("\n\n").unwrap_or(tdd_block.len());
        let block = &tdd_block[..block_end];
        assert!(!block.contains("Year:"), "Year should be omitted: {block:?}");
    }

    #[test]
    fn round_trip_through_librec_parser() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        let text =
            record_batches_to_rec_string("Book", &schema, std::slice::from_ref(&batch))
                .unwrap();

        let mut db = Db::parse_str(&text).unwrap();
        let (schema2, batch2) = rec_to_record_batch(&mut db, "Book").unwrap();

        // Same column set in the same order.
        let names: Vec<&str> =
            schema2.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["Title", "Year", "Price", "InPrint"]);
        // Types survive the round-trip.
        assert_eq!(schema2.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema2.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema2.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema2.field(3).data_type(), &DataType::Boolean);
        // Row count is preserved.
        assert_eq!(batch2.num_rows(), batch.num_rows());
    }

    #[test]
    fn empty_record_type_rejected() {
        let schema = sample_schema();
        let batch = sample_batch(&schema);
        assert!(
            record_batches_to_rec_string("", &schema, std::slice::from_ref(&batch))
                .is_err()
        );
    }

    #[test]
    fn unsupported_arrow_type_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Stamp",
            DataType::Int32,
            true,
        )]));
        let arr: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![Some(1)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr]).unwrap();
        assert!(
            record_batches_to_rec_string("T", &schema, std::slice::from_ref(&batch))
                .is_err()
        );
    }
}
