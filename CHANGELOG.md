# Changelog

All notable changes to this workspace are documented here. The crates
(`recutils-rs`, `rec2parquet`, `recsql`) share a single workspace version.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **recsql:** Allow ability to query multiple files recfiles together. Supports both same record 
  set spread across files and anonymous record sets
- **recsql:** `-f/--format <table|rec>` flag to choose the query output format.
  `table` (the default) keeps the existing DataFusion pretty table; `rec` emits
  a GNU `.rec` record stream that round-trips back into `recsql`, reusing the
  same serializer as `COPY ... STORED AS REC`.
- **recsql:** `-t/--record-type <NAME>` flag (default `Record`) to set the
  `%rec:` type name stamped on `--format rec` output.
- **recsql:** `.format [table|rec [TypeName]]` REPL meta-command to toggle the
  output format live. `.tables` and `.schema` always render as tables.
- **recsql:** interactive REPL (`--features repl`) with `;`-terminated
  multi-line statements, persistent history, and `.help` / `.quit` / `.tables`
  / `.schema` / `.read` meta-commands.

## [0.1.1] - 2024

### Changed

- Enable building the crate documentation on docs.rs.

## [0.1.0] - 2024

### Added

- **recutils-rs:** Rust FFI bindings to `librec` with a safe wrapper (`Db`,
  `Rset`, `Record`, `SelectionExpression`), plus an optional `arrow` feature to
  read `.rec` files into Arrow `RecordBatch`es and serialize them back out as
  `.rec` text with `%rec:` / `%type:` / `%mandatory:` declarations.
- **recutils-rs:** write support — append records to an rset and write into a
  new `.rec` file.
- **rec2parquet:** CLI to convert `.rec` files to Apache Parquet.
- **recsql:** CLI to query `.rec` files with SQL via Apache DataFusion. Each
  `%rec:` record set is registered as a SQL table (named after its type;
  anonymous rsets such as `csv2rec` output are exposed as `rec` / `rec_<index>`),
  with selection-expression filter pushdown and a `COPY ... STORED AS REC` write
  path to a fresh `.rec` file.
- Claude Code skill documenting `recsql` usage.

[Unreleased]: https://github.com/Mark1626/recutil-tools/compare/0.1.1...HEAD
[0.1.1]: https://github.com/Mark1626/recutil-tools/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/Mark1626/recutil-tools/releases/tag/0.1.0
