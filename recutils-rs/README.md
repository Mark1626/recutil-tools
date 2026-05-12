# recutils-rs

Rust FFI bindings to [GNU recutils](https://www.gnu.org/software/recutils/) (`librec`).

The crate has two layers:

- A raw `unsafe` FFI under `recutils_rs::ffi`, generated at build time by
  `bindgen` from `<rec.h>` — covers the full `rec_*` / `REC_*` / `MSET_*`
  surface.
- A small safe wrapper at the crate root: `Db` (parse + construct empty), `Rset` (borrowed from a parsed `Db`), `OwnedRset` (freshly built, transferred into a `Db` via `Db::append_rset`), `Record`, `SelectionExpression`, etc.

The optional `arrow` feature covers both directions: `rec_to_record_batch` reads an rset into Arrow, and `record_batches_to_rec_string` serializes Arrow batches back to rec text with `%rec:` / `%type:` / `%mandatory:` declarations derived from the schema.

## Status

The raw FFI is complete; the safe layer is intentionally minimal to what's needed in `recsql` and `rec2parquet`.
