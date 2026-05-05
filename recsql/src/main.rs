use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use datafusion::prelude::{SessionConfig, SessionContext};
use recsql::RecTableProvider;

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
    /// SQL query to run
    #[arg(short = 'q', long)]
    query: String,
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
    let providers = RecTableProvider::open_all(&opts.input)?;
    if providers.is_empty() {
        return Err(format!("no record sets found in {}", opts.input.display()).into());
    }
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    for (name, provider) in providers {
        ctx.register_table(name.as_str(), Arc::new(provider))?;
    }
    let df = ctx.sql(&opts.query).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
