use std::io::Read;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use wait_timeout::ChildExt;

fn spawn_output_reader<R: Read + Send + 'static>(
    mut reader: R,
) -> thread::JoinHandle<Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    })
}

fn join_output_reader(
    handle: thread::JoinHandle<Result<Vec<u8>>>,
    stream_name: &str,
) -> Result<Vec<u8>> {
    handle
        .join()
        .map_err(|_| anyhow!("{stream_name} reader thread exited unexpectedly"))?
        .with_context(|| format!("Failed to read {stream_name} output"))
}

fn run_cmd_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<(i32, String, String)> {
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    let stdout = child
        .stdout
        .take()
        .context("Failed to capture child process stdout pipe")?;
    let stderr = child
        .stderr
        .take()
        .context("Failed to capture child process stderr pipe")?;
    let stdout_handle = spawn_output_reader(stdout);
    let stderr_handle = spawn_output_reader(stderr);

    let timeout = Duration::from_secs(timeout_secs);
    let status = match child.wait_timeout(timeout)? {
        Some(status) => status,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            let _ = stdout_handle.join();
            let _ = stderr_handle.join();
            bail!("Command timed out ({}s)", timeout_secs);
        }
    };

    let stdout = join_output_reader(stdout_handle, "stdout")?;
    let stderr = join_output_reader(stderr_handle, "stderr")?;
    let code = status.code().unwrap_or(-1);
    Ok((
        code,
        String::from_utf8_lossy(&stdout).to_string(),
        String::from_utf8_lossy(&stderr).to_string(),
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn test_run_cmd_with_timeout_handles_large_output_without_deadlock() {
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "dd if=/dev/zero bs=65536 count=4 2>/dev/null; dd if=/dev/zero bs=65536 count=4 1>&2 2>/dev/null",
        ]);

        let (code, stdout, stderr) = run_cmd_with_timeout(cmd, 5).expect("command should complete");

        assert_eq!(code, 0);
        assert!(stdout.len() >= 4 * 65536);
        assert!(stderr.len() >= 4 * 65536);
    }

    #[test]
    fn test_run_cmd_with_timeout_returns_timeout_error() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 2"]);

        let err = run_cmd_with_timeout(cmd, 1).expect_err("command should time out");
        assert!(err.to_string().contains("Command timed out"));
    }
}

pub(crate) trait CommandRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        timeout_secs: u64,
    ) -> Result<(i32, String, String)>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        timeout_secs: u64,
    ) -> Result<(i32, String, String)> {
        let mut cmd = Command::new(program);
        for arg in args {
            cmd.arg(arg);
        }
        run_cmd_with_timeout(cmd, timeout_secs)
    }
}
