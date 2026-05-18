use std::error::Error;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

const PROMPT: &str = "recsql> ";
const CONTINUATION: &str = "   ...> ";

const HELP: &str = "\
recsql REPL commands:
  .help              show this help
  .quit, .exit       leave the REPL
  .tables            list tables (SHOW TABLES)
  .schema [TABLE]    show schema for TABLE, or all columns
  .read <PATH>       run ;-terminated statements from a file
SQL statements end with ';' (multi-line supported). Ctrl-C clears the
current input; Ctrl-D exits.";

enum Action {
    Continue,
    Quit,
}

pub async fn run(ctx: SessionContext) -> Result<(), Box<dyn Error>> {
    let mut rl = DefaultEditor::new()?;
    let history = history_path();
    if let Some(path) = history.as_deref() {
        let _ = rl.load_history(path);
    }
    let result = run_loop(&ctx, &mut rl).await;
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
                    match handle_meta(ctx, trimmed).await {
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
                    if let Err(e) = execute(ctx, stmt).await {
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

async fn execute(ctx: &SessionContext, sql: &str) -> Result<(), Box<dyn Error>> {
    let df = ctx.sql(sql).await?;
    let batches: Vec<RecordBatch> = df.collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("{}", pretty_format_batches(&batches)?);
    println!("({} row{})", rows, if rows == 1 { "" } else { "s" });
    Ok(())
}

async fn handle_meta(ctx: &SessionContext, line: &str) -> Result<Action, Box<dyn Error>> {
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
            execute(ctx, "SHOW TABLES").await?;
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
            execute(ctx, &sql).await?;
            Ok(Action::Continue)
        }
        ".read" => {
            if arg.is_empty() {
                return Err("usage: .read <path>".into());
            }
            let contents = std::fs::read_to_string(arg)?;
            for stmt in contents.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                if let Err(e) = execute(ctx, stmt).await {
                    eprintln!("error: {e}");
                }
            }
            Ok(Action::Continue)
        }
        _ => Err(format!("unknown command: {cmd} (try .help)").into()),
    }
}
