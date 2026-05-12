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

## Install

```bash
cargo install recsql
```

Requires GNU `recutils` (provides `librec`) installed on the build host. On macOS: `brew install recutils`. On Debian/Ubuntu: `apt install recutils libgnurec-dev`.
