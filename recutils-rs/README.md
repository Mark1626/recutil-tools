# recutils-rs

Rust FFI bindings to [GNU recutils](https://www.gnu.org/software/recutils/) (`librec`).

The crate has two layers:

- A raw `unsafe` FFI under `recutils_rs::ffi`, generated at build time by
  `bindgen` from `<rec.h>` — covers the full `rec_*` / `REC_*` / `MSET_*`
  surface.
- A small safe wrapper at the crate root (`Db`, `Rset`, `Record`, `SelectionExpression`, etc)

## Status

The raw FFI is complete; the safe layer is intentionally minimal to what's needed in `recsql` and `rec2parquet`.
