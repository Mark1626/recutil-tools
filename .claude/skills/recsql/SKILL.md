---
name: recsql
description: Query GNU recutils .rec files with SQL via the recsql CLI. Use when the user wants to filter, project, join, or aggregate over a .rec file, when reaching for awk/grep/recsel-pipelines on rec data would be clumsy, or when the user wants to COPY query results out to a fresh .rec file with %type:/%mandatory: declarations preserved. Covers the table-naming rules (named %rec: types vs. anonymous rsets like csv2rec output), DataFusion identifier-quoting, declared-vs-inferred column types, filter pushdown, and the COPY write path.
---

# recsql

`recsql` is a CLI that exposes a GNU recutils `.rec` file as SQL tables and runs queries against them via Apache DataFusion. Reach for it instead of `recsel | awk` when the user wants joins, aggregates, ordering, projection of specific columns, or anything more structured than "match a selection expression and print."

## Invocation

```bash
recsql <INPUT.rec> -q '<SQL>'
```

Single statement, single file. The result is printed as a pretty-formatted table.

## Table-naming rules

Each record set in the file becomes one SQL table. The naming depends on whether the rset has a `%rec:` descriptor:

- **Named rset** (file contains `%rec: Book`): table is `book`. The first whitespace-separated token after `%rec:` is the type name; key suffixes (`%rec: Book Id`) are stripped.
- **Anonymous rset** (no `%rec:` descriptor — the shape produced by `csv2rec`): table is `rec`.
- **Multiple anonymous rsets, or anonymous + a named rset called `rec`**: anonymous ones become `rec_<index>` where `<index>` is the rset's 0-based position in the file.

Use `recsql file.rec -q 'SHOW TABLES'` to list what's available — `information_schema` is enabled by default.

## Identifier quoting (the #1 footgun)

DataFusion case-folds unquoted SQL identifiers to lowercase, but rec field names usually preserve case (`Title`, `Author`, `Year`). So:

- **Table names**: register lowercased (`book`, not `Book`), so unquoted is fine: `FROM book`.
- **Column names with mixed case**: must be double-quoted: `SELECT "Title", "Year" FROM book` — *not* `SELECT Title, Year`.
- **All-lowercase rec field names**: don't need quoting.

When a query fails with `No field named title. Valid fields are book."Title", ...`, that's the case-folding bite. Add quotes.

## Examples

```bash
# List tables in a file
recsql library.rec -q 'SHOW TABLES'

# Project columns from a named rset
recsql library.rec -q 'SELECT "Title", "Year" FROM book WHERE "Year" > 2000 ORDER BY "Year"'

# Join two named rsets
recsql library.rec -q '
  SELECT b."Title", b."Year", a."Country"
  FROM book b JOIN author a ON b."Author" = a."Name"
  ORDER BY b."Year"'

# Aggregate
recsql library.rec -q '
  SELECT "Author", count(*) as books, min("Year") as first_year
  FROM book
  GROUP BY "Author"
  ORDER BY books DESC'

# Query a CSV-derived rec file (no %rec: descriptor)
csv2rec books.csv > books.rec
recsql books.rec -q 'SELECT "Title", "Year" FROM rec ORDER BY "Year"'
```

## Column types

`%type:` declarations from the rset descriptor are honored:

- `int` / `range` → `Int64`
- `real` → `Float64`
- `bool` → `Boolean` (accepts `yes`/`no`/`true`/`false`/`1`/`0`)
- everything else → `Utf8`

Untyped fields fall back to `Utf8` and a `log::info!` line is printed per column. Files produced by `csv2rec` typically have no `%type:` declarations, so every column comes through as `Utf8` — cast in SQL if you need numeric ordering: `ORDER BY CAST("Year" AS INT)`.

## Filter pushdown

Predicates that translate cleanly into a recutils selection-expression are pushed down to librec and reported as `Exact` (DataFusion does not re-check). Top-level conjunctions where only some conjuncts translate are pushed as a relaxation and reported `Inexact` (DataFusion re-checks the original). Anything else is evaluated entirely in DataFusion. This means: simple `WHERE Year > 2000` filters scan less data; complex `WHERE` clauses still work, just without the librec pre-filter.

Run with `RUST_LOG=debug recsql ...` to see what got pushed.

## Writing: `COPY` to a fresh `.rec` file

`COPY (SELECT ...) TO '<path>' STORED AS REC OPTIONS ('record_type' '<Name>')` writes the query results as a fresh `.rec` file. The `%rec:` / `%type:` / `%mandatory:` descriptor is built from the Arrow schema of the SELECT.

```bash
recsql library.rec -q "
  COPY (SELECT \"Title\", \"Year\", \"Pages\" FROM book WHERE \"Year\" > 1990)
  TO '/tmp/recent.rec' STORED AS REC
  OPTIONS ('record_type' 'Book')"
```

Produces:

```
%rec: Book
%type: Year int
%type: Pages int

Title: The Practice of Programming
Year: 1999
Pages: 267
```

Rules and footguns:

- **`record_type` is required.** Missing it → plan-time error. There's no implicit default.
- **No overwrite.** If the target path already exists, the write is refused before any I/O. The user must delete the file first. (DataFusion 53 doesn't expose `COPY OVERWRITE` syntax; this isn't a recsql gap.)
- **Local filesystem only.** `s3://`, `gs://`, etc. return `not_impl_err`.
- **Type info round-trips.** Int64 → `%type: <field> int`, Float64 → `real`, Boolean → `bool`, Utf8 → no `%type:` (rec's default is string). Non-nullable Arrow fields produce `%mandatory:` lines.
- **Nulls become absent fields.** A null value in the input batch omits that field from the emitted record — matches the read side's "missing field == null" convention.

## DML against existing rsets

**Not supported.** `INSERT`, `UPDATE`, `DELETE` against a registered rec table all return `not_impl_err`. There's an unimplemented plan in the repo (`INSERT_PLAN.md`) for `INSERT INTO existing_rset` semantics; until it lands, the only write surface is `COPY ... TO` to a new file.

## Reading via `CREATE EXTERNAL TABLE` / path-as-table

Not supported either. `CREATE EXTERNAL TABLE foo STORED AS REC LOCATION '...'` and `SELECT * FROM '/some/file.rec'` both return `not_impl_err` — the rec `FileFormat` is write-only. Reads are intentionally routed through the per-rset `RecTableProvider` that the CLI registers at startup, because a single `.rec` file can contain multiple `%rec:` blocks with distinct schemas and the `FileFormat` model can only return one schema per file. To query a file, point the CLI at it: `recsql <file.rec> -q '...'`.

## Limitations to surface

- One file per invocation; no multi-file `JOIN` across files.
- Repeated field names within one record (`Author: Foo` then `Author: Bar` in the same record) are an error in the Arrow conversion — not yet mapped to `List<T>`.
- DataFusion 53 is the pinned version.

## When NOT to suggest recsql

- Single-field substring search on a known type → `recsel -e '...'` is shorter.
- Mutating an existing rec file in place → use `recins` / `recset` / `recdel` directly. recsql's `COPY` writes a *new* file; it can't append to or edit an existing one yet.
- Streaming/very-large files → recsql parses the whole file into memory.
