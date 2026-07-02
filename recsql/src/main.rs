use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use datafusion::prelude::{SessionConfig, SessionContext};
use recsql::{MultiRecTableProvider, RecFileFormatFactory, RecTableProvider};

#[cfg(feature = "repl")]
mod repl;

/// Output format for query results.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Pretty box-drawing table (default).
    Table,
    /// GNU recutils .rec record stream.
    Rec,
}

#[derive(Parser, Debug)]
#[command(
    name = "recsql",
    about = "Query a GNU recutils .rec file with SQL",
    version
)]
struct Opts {
    /// Input .rec file(s). Every record set is registered as a SQL table
    /// named after its `%rec:` type, or as `rec` (or `rec_<index>`) for
    /// anonymous rsets with no descriptor (e.g. files produced by csv2rec).
    ///
    /// Pass several files to query across them. Give a file an explicit
    /// table name with `alias=path` (e.g. `sales=q1.rec`). When two inputs
    /// resolve to the *same* table name — same `%rec:` type, or the same
    /// alias — their record sets are unioned into one table, each file
    /// becoming a separate scan partition.
    #[arg(required = true, num_args = 1..)]
    inputs: Vec<String>,
    /// SQL query to run. Omit to drop into an interactive REPL (requires the
    /// `repl` feature; rebuild with `--features repl`).
    #[arg(short = 'q', long)]
    query: Option<String>,
    /// Output format for query results.
    #[arg(short = 'f', long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    /// `%rec:` type name to stamp on `--format rec` output.
    #[arg(short = 't', long, default_value = "Record")]
    record_type: String,
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();

    let opts = Opts::parse();
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(opts)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(opts: Opts) -> Result<(), Box<dyn std::error::Error>> {
    let specs: Vec<InputSpec> = opts.inputs.iter().map(|s| InputSpec::parse(s)).collect();
    let ctx = build_context(&specs)?;
    match opts.query {
        Some(query) => run_query(&ctx, &query, opts.format, &opts.record_type).await,
        None => run_interactive(ctx, opts.format, opts.record_type).await,
    }
}

/// One CLI input: an optional table alias plus a path. `alias=path` sets the
/// alias; a bare path leaves it unnamed (tables take their `%rec:` names).
struct InputSpec {
    alias: Option<String>,
    path: PathBuf,
}

impl InputSpec {
    fn parse(raw: &str) -> Self {
        // Treat `alias=path` as an alias only when the left side looks like a
        // bare table name (no path separators) — otherwise it's a filename
        // that merely contains '='.
        if let Some((alias, path)) = raw.split_once('=') {
            let looks_like_alias = !alias.is_empty()
                && !alias.contains('/')
                && !alias.contains(std::path::MAIN_SEPARATOR);
            if looks_like_alias {
                return InputSpec {
                    alias: Some(alias.to_string()),
                    path: PathBuf::from(path),
                };
            }
        }
        InputSpec {
            alias: None,
            path: PathBuf::from(raw),
        }
    }
}

fn build_context(specs: &[InputSpec]) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // Group providers by resolved table name, preserving first-seen order so
    // registration and partition order are deterministic. Same name from
    // multiple files → one partitioned table.
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<RecTableProvider>> = HashMap::new();
    for spec in specs {
        let rsets = RecTableProvider::open_all(&spec.path)?;
        let multi_rset = rsets.len() > 1;
        for (name, provider) in rsets {
            let table = match &spec.alias {
                // A file with several rsets can't collapse to one alias, so the
                // alias namespaces each rset instead of replacing it.
                Some(a) if multi_rset => format!("{a}_{name}"),
                Some(a) => a.clone(),
                None => name,
            };
            if !groups.contains_key(&table) {
                order.push(table.clone());
            }
            groups.entry(table).or_default().push(provider);
        }
    }
    if order.is_empty() {
        return Err("no record sets found in the given input(s)".into());
    }

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    ctx.state_ref()
        .write()
        .register_file_format(Arc::new(RecFileFormatFactory::new()), false)?;
    for name in order {
        let mut providers = groups.remove(&name).unwrap();
        if providers.len() == 1 {
            ctx.register_table(name.as_str(), Arc::new(providers.pop().unwrap()))?;
        } else {
            log::info!(
                "table {name:?} unions {} files as {} partitions",
                providers.len(),
                providers.len()
            );
            ctx.register_table(name.as_str(), Arc::new(MultiRecTableProvider::new(providers)?))?;
        }
    }
    Ok(ctx)
}

async fn run_query(
    ctx: &SessionContext,
    query: &str,
    format: OutputFormat,
    record_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx.sql(query).await?;
    // Capture the arrow schema before `collect()` consumes the DataFrame; the
    // rec serializer needs it even when the result has zero batches.
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;
    match format {
        OutputFormat::Table => println!("{}", pretty_format_batches(&batches)?),
        OutputFormat::Rec => {
            let s = recutils_rs::arrow::record_batches_to_rec_string(
                record_type,
                &schema,
                &batches,
            )?;
            print!("{s}");
        }
    }
    Ok(())
}

#[cfg(feature = "repl")]
async fn run_interactive(
    ctx: SessionContext,
    format: OutputFormat,
    record_type: String,
) -> Result<(), Box<dyn std::error::Error>> {
    repl::run(ctx, format, record_type).await
}

#[cfg(not(feature = "repl"))]
async fn run_interactive(
    _ctx: SessionContext,
    _format: OutputFormat,
    _record_type: String,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("no query supplied; rebuild with `--features repl` for interactive mode".into())
}
