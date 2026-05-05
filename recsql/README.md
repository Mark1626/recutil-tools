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

## Install

```bash
cargo install recsql
```

Requires GNU `recutils` (provides `librec`) installed on the build host. On macOS: `brew install recutils`. On Debian/Ubuntu: `apt install recutils libgnurec-dev`.
