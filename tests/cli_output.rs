use std::process::Command;

use serde_json::Value;

#[test]
fn json_output_mode_writes_machine_readable_error_to_stdout() {
    let output = Command::new(env!("CARGO_BIN_EXE_gmail-auto-label"))
        .args(["--max-labels", "1", "--output", "json"])
        .output()
        .expect("failed to run gmail-auto-label binary");

    assert!(
        !output.status.success(),
        "expected non-zero exit for invalid --max-labels"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    let payload: Value = serde_json::from_str(stdout.trim()).expect("stdout should be valid json");

    assert_eq!(payload.get("ok").and_then(Value::as_bool), Some(false));
    assert_eq!(
        payload.get("code").and_then(Value::as_str),
        Some("config_error")
    );
    assert!(
        payload
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|msg| msg.contains("--max-labels must be at least 2")),
        "unexpected message payload: {payload}"
    );
}
