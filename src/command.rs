use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Result, bail};
use wait_timeout::ChildExt;

fn run_cmd_with_timeout(mut cmd: Command, timeout_secs: u64) -> Result<(i32, String, String)> {
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;

    let timeout = Duration::from_secs(timeout_secs);
    let status_opt = child.wait_timeout(timeout)?;
    if status_opt.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        bail!("命令超时（{}s）", timeout_secs);
    }

    let output = child.wait_with_output()?;
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    Ok((code, stdout, stderr))
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
