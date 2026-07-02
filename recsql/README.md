# recsql

recsql allows running SQL queries against [GNU recutils](https://www.gnu.org/software/recutils/) `.rec` files via [Apache DataFusion](https://datafusion.apache.org/). Every `%rec:` record set in the input becomes its own SQL table, named after its type. Anonymous record sets — files with no `%rec:` descriptor are exposed as the table `rec` or `rec_<index>` when there's more than one anonymous rset.

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

`SHOW TABLES` and the rest of `information_schema` are enabled. SQL identifiers are case-folded by default, quote rec field names that use mixed case (e.g. `"Year"`).

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

`-t/--record-type NAME` sets the `%rec:` (defaults to `Record`)

## Writing results to a new `.rec` file

`COPY (SELECT ...) TO '<path>' STORED AS REC OPTIONS ('record_type' '<Name>')` writes the query output as a fresh `.rec` file. This also includes the Arrow schema in the block description `%rec:`

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

`record_type` is required.

## Interactive mode (needs repl feature)

Omit `-q` to drop into a SQL REPL.

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

## Install

```bash
cargo install recsql                  # headless only
cargo install recsql --features repl  # with the interactive REPL
```

Requires GNU `recutils` (provides `librec`) installed on the build host. On macOS: `brew install recutils`. On Debian/Ubuntu: `apt install recutils libgnurec-dev`.
