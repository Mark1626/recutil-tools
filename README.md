# recutil-tools

A set of tools around [GNU recutils](https://www.gnu.org/software/recutils/).

## Crates

### [`recutils-rs`](./recutils-rs)

Rust FFI bindings to `librec` with a safe wrapper to contain unsafe FFI calls.
Also has an optional `arrow` feature to read recfiles into Arrow `RecordBatch`es and to serialize `RecordBatch`es back out as `.rec` text (with `%rec:` / `%type:` / `%mandatory:` declarations).

### [`rec2parquet`](./rec2parquet)

CLI binary that converts `.rec` files to Apache Parquet

```bash
rec2parquet <INPUT> <OUTPUT> -t <TYPE> [-c <COMPRESSION>] [--max-row-group-size N] [-p] [-n]
```

### [`recsql`](./recsql)

CLI tool to query `.rec` files with SQL via [Apache DataFusion](https://datafusion.apache.org/).

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

# Emit results as a .rec record stream instead of the default pretty table
recsql library.rec -q 'SELECT "Title", "Year" FROM book' -f rec -t Book

# Write query results to a new .rec file with %type: / %mandatory: from the schema
recsql library.rec -q "
  COPY (SELECT \"Title\", \"Year\", \"Pages\" FROM book WHERE \"Year\" > 1990)
  TO '/tmp/recent.rec' STORED AS REC
  OPTIONS ('record_type' 'Book')"
```

## Build

```bash
cargo build --workspace --all-targets
```

## Other Tools

### rec.k

This repo also has a K recfile parser and a relational query engine in the folder [k-rec](./k-rec/README.md)

## librec discovery

`recutils-rs/build.rs` searches for librec in this order:

1. `RECUTILS_PREFIX` (expects `include/` and `lib/` underneath)
2. `RECUTILS_INCLUDE_DIR` and/or `RECUTILS_LIB_DIR`
3. `brew --prefix recutils` (macOS Homebrew)
4. Compiler defaults

`bindgen` requires `libclang` (Xcode CLT on macOS, `libclang-dev` on Linux).
On Linux: `apt install recutils libgnurec-dev`.

## Claude Code skill

A [Claude Code](https://claude.com/claude-code) skill teaching `recsql` usage lives at [`.claude/skills/recsql/SKILL.md`](./.claude/skills/recsql/SKILL.md). It covers the table-naming rules (named `%rec:` types vs. anonymous rsets), DataFusion identifier-quoting, declared-vs-inferred column types, and filter pushdown.

- **Inside this repo**: nothing to do — Claude Code auto-loads project-level skills from `.claude/skills/`.
- **Anywhere else**: copy it once into your user skills directory.

```bash
mkdir -p ~/.claude/skills/recsql
curl -fsSL https://raw.githubusercontent.com/Mark1626/recutil-tools/main/.claude/skills/recsql/SKILL.md \
  -o ~/.claude/skills/recsql/SKILL.md
```

## License

`GPL-3.0-or-later`
