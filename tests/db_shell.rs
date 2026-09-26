use std::io::{BufReader, Cursor};
use std::time::Duration;

use deepseek_cli::inspection::{DbShellLimits, InspectionError, ReadonlyDbShell};
use deepseek_cli::store::Store;

fn setup() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.sqlite");
    let store = Store::open(&path).unwrap();
    store.create_dialog("one").unwrap();
    (dir, path)
}

fn run(shell: &ReadonlyDbShell, sql: &str) -> Result<String, InspectionError> {
    let mut output = Vec::new();
    shell.run(BufReader::new(Cursor::new(sql.as_bytes())), &mut output)?;
    Ok(String::from_utf8(output).unwrap())
}

#[test]
fn accepts_parameter_free_select_with_explain_and_allowlisted_pragmas() {
    let (_dir, path) = setup();
    let shell = ReadonlyDbShell::new(path);
    let output = run(
        &shell,
        "SELECT id,title FROM dialogs ORDER BY id;\nWITH n(v) AS (SELECT 7) SELECT v FROM n;\nEXPLAIN QUERY PLAN SELECT * FROM dialogs;\nPRAGMA table_info(dialogs);\nPRAGMA schema_version;\n",
    )
    .unwrap();
    assert!(output.contains("one"));
    assert!(output.contains("dialogs"));
    assert!(output.contains("schema_version"));
}

#[test]
fn rejects_mutation_attachment_extensions_parameters_and_shell_escapes() {
    let (_dir, path) = setup();
    let shell = ReadonlyDbShell::new(&path);
    for sql in [
        "INSERT INTO dialogs(title) VALUES('x')\n",
        "UPDATE dialogs SET title='x'\n",
        "DELETE FROM dialogs\n",
        "ATTACH DATABASE '/tmp/other.sqlite' AS other\n",
        "PRAGMA journal_mode=WAL\n",
        "PRAGMA writable_schema=ON\n",
        "SELECT load_extension('/tmp/evil')\n",
        "SELECT * FROM dialogs; DELETE FROM dialogs\n",
        "SELECT * FROM dialogs -- hidden\n",
        "SELECT /* hidden */ * FROM dialogs\n",
        "SELECT ?1\n",
        ".shell id\n",
        ".read /tmp/commands.sql\n",
    ] {
        let error = run(&shell, sql).unwrap_err();
        assert!(
            matches!(
                error,
                InspectionError::RejectedQuery
                    | InspectionError::MultipleStatements
                    | InspectionError::ParametersNotAllowed
                    | InspectionError::Query
            ),
            "unexpected error {error:?} for {sql:?}"
        );
    }
    let verify = rusqlite::Connection::open(&path).unwrap();
    let titles: Vec<String> = verify
        .prepare("SELECT title FROM dialogs ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(titles, vec!["one"]);
}

#[test]
fn output_rows_cells_input_and_execution_are_bounded() {
    let (_dir, path) = setup();
    let shell = ReadonlyDbShell::with_limits(
        path,
        DbShellLimits {
            max_sql_bytes: 256,
            max_rows: 3,
            max_columns: 8,
            max_cell_bytes: 16,
            max_output_bytes: 4_096,
            max_query_time: Duration::from_millis(10),
        },
    )
    .unwrap();
    let output = run(
        &shell,
        "WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<10) SELECT v FROM n;\n",
    )
    .unwrap();
    assert_eq!(
        output
            .lines()
            .filter(|line| line.contains("\"row\""))
            .count(),
        3
    );
    assert!(output.contains("\"truncated\":true"));

    assert_eq!(
        run(&shell, "SELECT printf('%020d', 1)\n").unwrap_err(),
        InspectionError::CellTooLarge
    );
    assert_eq!(
        run(&shell, &format!("SELECT '{}'\n", "x".repeat(300))).unwrap_err(),
        InspectionError::InputTooLarge
    );

    let timeout = run(
        &shell,
        "WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n) SELECT sum(v) FROM n;\n",
    )
    .unwrap_err();
    assert_eq!(timeout, InspectionError::QueryTimedOut);
}

#[test]
fn native_value_limit_and_aggregate_output_budget_fail_closed() {
    let (_dir, path) = setup();
    let native_limited = ReadonlyDbShell::with_limits(
        &path,
        DbShellLimits {
            max_sql_bytes: 512,
            max_rows: 10,
            max_columns: 8,
            max_cell_bytes: 128,
            max_output_bytes: 4_096,
            max_query_time: Duration::from_millis(100),
        },
    )
    .unwrap();
    assert_eq!(
        run(&native_limited, "SELECT zeroblob(129)\n").unwrap_err(),
        InspectionError::ResourceLimit
    );

    let output_limited = ReadonlyDbShell::with_limits(
        path,
        DbShellLimits {
            max_sql_bytes: 512,
            max_rows: 100,
            max_columns: 8,
            max_cell_bytes: 128,
            max_output_bytes: 120,
            max_query_time: Duration::from_millis(100),
        },
    )
    .unwrap();
    assert_eq!(
        run(
            &output_limited,
            "WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<20) SELECT printf('%016d',v) FROM n;\n"
        )
        .unwrap_err(),
        InspectionError::OutputTooLarge
    );
}

#[test]
fn errors_are_safe_machine_codes() {
    let (_dir, path) = setup();
    let shell = ReadonlyDbShell::new(path);
    let error = run(
        &shell,
        "SELECT definitely_missing_secret_column FROM dialogs\n",
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "query_error");
    assert!(
        !error
            .to_string()
            .contains("definitely_missing_secret_column")
    );
}
