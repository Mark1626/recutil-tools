use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use datafusion::prelude::{SessionConfig, SessionContext};
use recsql::{RecFileFormatFactory, RecTableProvider};

#[cfg(feature = "repl")]
mod repl;

#[derive(Parser, Debug)]
#[command(
    name = "recsql",
    about = "Query a GNU recutils .rec file with SQL",
    version
)]
struct Opts {
    /// Input .rec file. Every record set is registered as a SQL table named
    /// after its `%rec:` type, or as `rec` (or `rec_<index>`) for anonymous
    /// rsets with no descriptor (e.g. files produced by csv2rec).
    input: PathBuf,
    /// SQL query to run. Omit to drop into an interactive REPL (requires the
    /// `repl` feature; rebuild with `--features repl`).
    #[arg(short = 'q', long)]
    query: Option<String>,
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
    let ctx = build_context(&opts.input)?;
    match opts.query {
        Some(query) => run_query(&ctx, &query).await,
        None => run_interactive(ctx).await,
    }
}

fn build_context(input: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let providers = RecTableProvider::open_all(input)?;
    if providers.is_empty() {
        return Err(format!("no record sets found in {}", input.display()).into());
    }
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    ctx.state_ref()
        .write()
        .register_file_format(Arc::new(RecFileFormatFactory::new()), false)?;
    for (name, provider) in providers {
        ctx.register_table(name.as_str(), Arc::new(provider))?;
    }
    Ok(ctx)
}

async fn run_query(ctx: &SessionContext, query: &str) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx.sql(query).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}

#[cfg(feature = "repl")]
async fn run_interactive(ctx: SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    repl::run(ctx).await
}

#[cfg(not(feature = "repl"))]
async fn run_interactive(_ctx: SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    Err("no query supplied; rebuild with `--features repl` for interactive mode".into())
}
