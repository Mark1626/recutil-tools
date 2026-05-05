# recutil-tools

A set of tools around [GNU recutils](https://www.gnu.org/software/recutils/).

## Crates

### [`recutils-rs`](./recutils-rs)

Rust FFI bindings to `librec` with a safe wrapper to contain unsafe FFI calls.
Also has an optional `arrow` feature to read recfiles into Arrow `RecordBatch`es

### [`rec2parquet`](./rec2parquet)

CLI binary that converts `.rec` files to Apache Parquet

```bash
rec2parquet <INPUT> <OUTPUT> -t <TYPE> [-c <COMPRESSION>] [--max-row-group-size N] [-p] [-n]
```

### [`recsql`](./recsql)

CLI tool to query `.rec` files with SQL via [Apache DataFusion](https://datafusion.apache.org/). Every `%rec:` record set is registered as a SQL table named after its type. Anonymous record sets — files with no `%rec:` descriptor, such as the output of `csv2rec` — are exposed as the table `rec` (or `rec_<index>` when there's more than one or the simple name would collide).

```bash
recsql <INPUT> -q '<SQL>'

recsql library.rec -q 'SHOW TABLES'

recsql library.rec -q '
  SELECT b."Title", b."Year", a."Country"
  FROM book b JOIN author a ON b."Author" = a."Name"
  ORDER BY b."Year"'

# Query a CSV-derived recfile (no %rec: descriptor)
csv2rec books.csv > books.rec
recsql books.rec -q 'SELECT "Title", "Year" FROM rec ORDER BY "Year"'
```

## Build

```bash
cargo build --workspace --all-targets
```

## librec discovery

`recutils-rs/build.rs` searches for librec in this order:

1. `RECUTILS_PREFIX` (expects `include/` and `lib/` underneath)
2. `RECUTILS_INCLUDE_DIR` and/or `RECUTILS_LIB_DIR`
3. `brew --prefix recutils` (macOS Homebrew)
4. Compiler defaults

`bindgen` requires `libclang` (Xcode CLT on macOS, `libclang-dev` on Linux).
On Linux: `apt install recutils libgnurec-dev`.

## License

`GPL-3.0-or-later`
