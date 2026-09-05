//! The command-line surface a person meets before they meet the API: `--help`, `--version`, and
//! what an unrecognized command does.
//!
//! The last one is the contract worth pinning. Scripts read the machine-readable `error_code` from
//! stdout and the exit code; people need to be told what the accepted commands are. Both happen,
//! in that order, on separate streams.

use std::process::Command;

const COMMANDS: [&str; 5] = [
    "serve",
    "verify",
    "backup create",
    "backup verify",
    "backup restore",
];

#[test]
fn help_prints_usage_to_stdout_and_exits_zero() {
    for flag in ["--help", "-h", "help"] {
        let output = Command::new(env!("CARGO_BIN_EXE_patwari-server"))
            .arg(flag)
            .env("RUST_LOG", "error")
            .output()
            .expect("the archive binary starts");
        assert_eq!(output.status.code(), Some(0), "`{flag}` exits 0");
        let stdout = String::from_utf8(output.stdout).expect("usage is UTF-8");
        assert!(
            stdout.contains("Usage: patwari-server"),
            "`{flag}` prints a usage line: {stdout}"
        );
        for command in COMMANDS {
            assert!(
                stdout.contains(command),
                "`{flag}` usage names `{command}`: {stdout}"
            );
        }
        assert!(
            stdout.contains("--output") && stdout.contains("--data-dir"),
            "`{flag}` usage names the flags the backup commands require: {stdout}"
        );
    }
}

#[test]
fn version_prints_the_crate_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_patwari-server"))
        .arg("--version")
        .env("RUST_LOG", "error")
        .output()
        .expect("the archive binary starts");
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("version is UTF-8");
    assert_eq!(
        stdout.trim(),
        format!("patwari-server {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn an_unknown_command_keeps_its_machine_readable_stdout_and_adds_usage_on_stderr() {
    let output = Command::new(env!("CARGO_BIN_EXE_patwari-server"))
        .args(["backup", "delete"])
        .env("RUST_LOG", "error")
        .output()
        .expect("the archive binary starts");
    assert_eq!(output.status.code(), Some(2), "scripts still see exit 2");

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let first_line = stdout.lines().next().expect("stdout has a first line");
    let error: serde_json::Value =
        serde_json::from_str(first_line).expect("the first stdout line is still the JSON error");
    assert_eq!(error["error_code"], "invalid_command");
    assert!(
        !stdout.contains("Usage: patwari-server"),
        "usage never pollutes the machine-readable stream: {stdout}"
    );

    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("Usage: patwari-server"),
        "a person is told the accepted commands: {stderr}"
    );
    for command in COMMANDS {
        assert!(stderr.contains(command), "stderr usage names `{command}`");
    }
}
