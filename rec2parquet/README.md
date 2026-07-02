# rec2parquet

CLI that converts [GNU recutils](https://www.gnu.org/software/recutils/) `.rec` files to Apache Parquet, modelled on [`csv2parquet`](https://github.com/domoritz/arrow-tools/tree/main/crates/csv2parquet).

```bash
rec2parquet <INPUT> <OUTPUT> -t <TYPE> [-c <COMPRESSION>] [--max-row-group-size N] [-p] [-n]
```

Honors `%type:` declarations from the rec descriptor; fields without a declared type fall back to `Utf8`

## Install

```bash
cargo install rec2parquet
```

Requires GNU `recutils` (provides `librec`) installed on the build host. On macOS: `brew install recutils`. On Debian/Ubuntu: `apt install recutils libgnurec-dev`.
