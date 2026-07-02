# recsql

CLI that runs SQL queries against [GNU recutils](https://www.gnu.org/software/recutils/) `.rec` files via [Apache DataFusion](https://datafusion.apache.org/). Every `%rec:` record set in the input becomes its own SQL table, named after its type. Anonymous record sets — files with no `%rec:` descriptor, such as the output of `csv2rec` — are exposed as the table `rec` (or `rec_<index>` when there's more than one anonymous rset, or the simple name would collide with an explicitly named one).

```bash
recsql <INPUT> -q '<SQL>'
```

```bash
recsql library.rec -q 'SHOW TABLES'

recsql library.rec -q '
  SELECT b."Title", b."Year", a."Country"
  FROM book b JOIN author a ON b."Author" = a."Name"
  ORDER BY b."Year"'

# Files produced by csv2rec have no %rec: descriptor — query them via 'rec'
csv2rec books.csv > books.rec
recsql books.rec -q 'SELECT "Title", "Year" FROM rec ORDER BY "Year"'
```

`SHOW TABLES` and the rest of `information_schema` are enabled. SQL identifiers are case-folded by default — quote rec field names that use mixed case (e.g. `"Year"`).

## Output format

Query results default to a DataFusion pretty table. Pass `-f/--format rec` to emit a GNU `.rec` record stream instead, which round-trips straight back into `recsql`:

```bash
recsql library.rec -q 'SELECT "Title", "Year" FROM book LIMIT 1' -f rec -t Book
# %rec: Book
# %type: Year int
#
# Title: Refactoring
# Year: 1999
```

`-t/--record-type NAME` sets the `%rec:` type name stamped on rec output (it can't be inferred from an arbitrary `SELECT`); it defaults to `Record`. The serializer is the same one behind `COPY ... STORED AS REC` — Int64 → `%type: <field> int`, Float64 → `real`, Boolean → `bool`, Utf8 → no `%type:`, and nulls are omitted from each record. An empty result set still emits a valid descriptor-only file.

Filter pushdown to recutils' selection-expression engine is best-effort: predicates that translate fully are reported as `Exact` (librec evaluates them and DataFusion does not re-check); a partial conjunction is `Inexact` (we push a relaxation, DataFusion re-checks); anything else stays in DataFusion.

## Writing results to a new `.rec` file

`COPY (SELECT ...) TO '<path>' STORED AS REC OPTIONS ('record_type' '<Name>')` writes the query output as a fresh `.rec` file. The descriptor block (`%rec:`, `%type:`, `%mandatory:`) is derived from the SELECT's Arrow schema — Int64 → `int`, Float64 → `real`, Boolean → `bool`, Utf8 → no `%type:` (rec's default is string), and any non-nullable column produces a `%mandatory:` line. Nulls are omitted from the emitted record (rec's "missing field == null" convention).

```bash
recsql library.rec -q "
  COPY (SELECT \"Title\", \"Year\", \"Pages\" FROM book WHERE \"Year\" > 1990)
  TO '/tmp/recent.rec' STORED AS REC
  OPTIONS ('record_type' 'Book')"

cat /tmp/recent.rec
# %rec: Book
# %type: Year int
# %type: Pages int
#
# Title: The Practice of Programming
# Year: 1999
# Pages: 267
```

`record_type` is required. The write refuses to overwrite an existing path (delete it first) and only supports local-filesystem targets. In-place `INSERT`/`UPDATE`/`DELETE` against an existing rset are still unsupported (see `INSERT_PLAN.md`); `COPY ... TO` is the only write surface today.

## Interactive mode

Omit `-q` to drop into a SQL REPL. Statements are `;`-terminated and may span multiple lines; Ctrl-C clears the current input, Ctrl-D exits. History persists across sessions under the platform's data dir (e.g. `~/Library/Application Support/recsql/history` on macOS, `~/.local/share/recsql/history` on Linux).

```bash
recsql library.rec
recsql> .tables
recsql> SELECT "Title", "Year" FROM book
   ...> ORDER BY "Year" LIMIT 3;
```

Meta-commands (sqlite-style):

| command          | effect                                  |
|------------------|-----------------------------------------|
| `.help`          | show help                               |
| `.quit`, `.exit` | leave the REPL (or Ctrl-D)              |
| `.tables`        | list registered tables                  |
| `.schema [TBL]`  | show columns for `TBL`, or all          |
| `.format [table\|rec [T]]` | show or set output format; `rec [T]` optionally sets the `%rec:` type |
| `.read <PATH>`   | run `;`-terminated statements from file |

The REPL honors `-f/--format` and `-t/--record-type` as its startup defaults, and `.format` toggles the output format live; `.tables` and `.schema` always render as tables regardless of the active format.

The REPL is gated behind the `repl` cargo feature so the headless install stays lean. Without the feature, invoking `recsql` without `-q` errors out.

## Install

```bash
cargo install recsql                  # headless only
cargo install recsql --features repl  # with the interactive REPL
```

Requires GNU `recutils` (provides `librec`) installed on the build host. On macOS: `brew install recutils`. On Debian/Ubuntu: `apt install recutils libgnurec-dev`.
