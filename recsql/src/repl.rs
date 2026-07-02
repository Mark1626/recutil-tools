use std::error::Error;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::OutputFormat;

const PROMPT: &str = "recsql> ";
const CONTINUATION: &str = "   ...> ";

const HELP: &str = "\
recsql REPL commands:
  .help                  show this help
  .quit, .exit           leave the REPL
  .tables                list tables (SHOW TABLES)
  .schema [TABLE]        show schema for TABLE, or all columns
  .format [table|rec [T]] show or set output format (rec optionally sets type T)
  .read <PATH>           run ;-terminated statements from a file
SQL statements end with ';' (multi-line supported). Ctrl-C clears the
current input; Ctrl-D exits.";

enum Action {
    Continue,
    Quit,
}

pub async fn run(
    ctx: SessionContext,
    format: OutputFormat,
    record_type: String,
) -> Result<(), Box<dyn Error>> {
    let mut rl = DefaultEditor::new()?;
    let history = history_path();
    if let Some(path) = history.as_deref() {
        let _ = rl.load_history(path);
    }
    let result = run_loop(&ctx, &mut rl, format, record_type).await;
    if let Some(path) = history.as_deref() {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.save_history(path);
    }
    result
}

fn history_path() -> Option<PathBuf> {
    dirs::data_dir().map(|d| d.join("recsql").join("history"))
}

async fn run_loop(
    ctx: &SessionContext,
    rl: &mut DefaultEditor,
    mut format: OutputFormat,
    mut record_type: String,
) -> Result<(), Box<dyn Error>> {
    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() { PROMPT } else { CONTINUATION };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if buffer.is_empty() && trimmed.is_empty() {
                    continue;
                }
                if buffer.is_empty() && trimmed.starts_with('.') {
                    let _ = rl.add_history_entry(trimmed);
                    match handle_meta(ctx, trimmed, &mut format, &mut record_type).await {
                        Ok(Action::Continue) => {}
                        Ok(Action::Quit) => break,
                        Err(e) => eprintln!("error: {e}"),
                    }
                    continue;
                }
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);
                if !buffer.trim_end().ends_with(';') {
                    continue;
                }
                let _ = rl.add_history_entry(buffer.as_str());
                let stmt = buffer.trim().trim_end_matches(';').trim_end();
                if !stmt.is_empty() {
                    if let Err(e) = execute(ctx, stmt, format, &record_type).await {
                        eprintln!("error: {e}");
                    }
                }
                buffer.clear();
            }
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(Box::new(e)),
        }
    }
    Ok(())
}

async fn execute(
    ctx: &SessionContext,
    sql: &str,
    format: OutputFormat,
    record_type: &str,
) -> Result<(), Box<dyn Error>> {
    let df = ctx.sql(sql).await?;
    // Capture the arrow schema before `collect()` consumes the DataFrame; the
    // rec serializer needs it even when the result has zero batches.
    let schema = df.schema().as_arrow().clone();
    let batches: Vec<RecordBatch> = df.collect().await?;
    match format {
        OutputFormat::Table => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("{}", pretty_format_batches(&batches)?);
            println!("({} row{})", rows, if rows == 1 { "" } else { "s" });
        }
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

async fn handle_meta(
    ctx: &SessionContext,
    line: &str,
    format: &mut OutputFormat,
    record_type: &mut String,
) -> Result<Action, Box<dyn Error>> {
    let mut parts = line.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or("");
    let arg = parts.next().map(str::trim).unwrap_or("");
    match cmd {
        ".help" => {
            println!("{HELP}");
            Ok(Action::Continue)
        }
        ".quit" | ".exit" => Ok(Action::Quit),
        ".tables" => {
            // Listings/schemas are always tabular for readability, regardless
            // of the active output format.
            execute(ctx, "SHOW TABLES", OutputFormat::Table, record_type).await?;
            Ok(Action::Continue)
        }
        ".schema" => {
            let sql = if arg.is_empty() {
                "SELECT table_name, column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema NOT IN ('information_schema') \
                 ORDER BY table_name, ordinal_position"
                    .to_string()
            } else {
                format!("DESCRIBE {arg}")
            };
            execute(ctx, &sql, OutputFormat::Table, record_type).await?;
            Ok(Action::Continue)
        }
        ".format" => {
            handle_format(arg, format, record_type);
            Ok(Action::Continue)
        }
        ".read" => {
            if arg.is_empty() {
                return Err("usage: .read <path>".into());
            }
            let contents = std::fs::read_to_string(arg)?;
            for stmt in contents.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                if let Err(e) = execute(ctx, stmt, *format, record_type).await {
                    eprintln!("error: {e}");
                }
            }
            Ok(Action::Continue)
        }
        _ => Err(format!("unknown command: {cmd} (try .help)").into()),
    }
}

/// Handle the `.format` meta-command: with no argument, report the current
/// setting; `table` / `rec [TypeName]` switch the active output format.
fn handle_format(arg: &str, format: &mut OutputFormat, record_type: &mut String) {
    let mut words = arg.split_whitespace();
    match words.next() {
        None => match format {
            OutputFormat::Table => println!("format: table"),
            OutputFormat::Rec => println!("format: rec (type {record_type})"),
        },
        Some("table") => {
            *format = OutputFormat::Table;
            println!("format: table");
        }
        Some("rec") => {
            *format = OutputFormat::Rec;
            if let Some(name) = words.next() {
                *record_type = name.to_string();
            }
            println!("format: rec (type {record_type})");
        }
        Some(other) => {
            eprintln!("error: unknown format {other:?}; expected `table` or `rec`");
        }
    }
}
