// SPDX-License-Identifier: AGPL-3.0-only

//! Network namespace helpers for netem chaos tests.
//!
//! Creates an isolated veth pair + tc qdisc for traffic shaping.
//! Requires Linux + root + RK_CHAOS=1. Skips gracefully otherwise.

use std::process::Command;

/// Check if netem tests can run (Linux + root + ip command + RK_CHAOS=1).
pub fn can_run_netem() -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    if std::env::var("RK_CHAOS").map_or(true, |v| v != "1") {
        return false;
    }
    if !is_root() {
        return false;
    }
    Command::new("ip").arg("netns").arg("list").output().is_ok()
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .is_some_and(|s| s.trim() == "0")
    }
    #[cfg(not(unix))]
    {
        false
    }
}

pub struct NetnsGuard {
    pub ns_name: String,
    pub veth_host: String,
    pub host_addr: String,
    pub ns_addr: String,
}

impl NetnsGuard {
    /// Create an isolated network namespace with a veth pair.
    pub fn create(name: &str) -> std::io::Result<Self> {
        let ns_name = format!("rk-chaos-{name}");
        let veth_host = format!("veth-h-{name}");
        let veth_ns = format!("veth-n-{name}");
        let host_addr = "10.99.0.1";
        let ns_addr = "10.99.0.2";

        // Create namespace
        run_cmd("ip", &["netns", "add", &ns_name])?;
        // Create veth pair
        run_cmd(
            "ip",
            &[
                "link", "add", &veth_host, "type", "veth", "peer", "name", &veth_ns,
            ],
        )?;
        // Move one end into namespace
        run_cmd("ip", &["link", "set", &veth_ns, "netns", &ns_name])?;
        // Assign addresses
        run_cmd(
            "ip",
            &["addr", "add", &format!("{host_addr}/24"), "dev", &veth_host],
        )?;
        run_cmd(
            "ip",
            &[
                "netns",
                "exec",
                &ns_name,
                "ip",
                "addr",
                "add",
                &format!("{ns_addr}/24"),
                "dev",
                &veth_ns,
            ],
        )?;
        // Bring up
        run_cmd("ip", &["link", "set", &veth_host, "up"])?;
        run_cmd(
            "ip",
            &[
                "netns", "exec", &ns_name, "ip", "link", "set", &veth_ns, "up",
            ],
        )?;
        run_cmd(
            "ip",
            &["netns", "exec", &ns_name, "ip", "link", "set", "lo", "up"],
        )?;

        Ok(Self {
            ns_name,
            veth_host,
            host_addr: host_addr.into(),
            ns_addr: ns_addr.into(),
        })
    }

    /// Apply tc netem rules to the host-side veth.
    pub fn apply_netem(&self, args: &[&str]) -> std::io::Result<()> {
        let mut cmd_args = vec!["qdisc", "add", "dev", &self.veth_host, "root", "netem"];
        cmd_args.extend(args);
        run_cmd("tc", &cmd_args)
    }

    /// Remove and re-apply netem rules (for mid-test changes).
    pub fn reset_netem(&self, args: &[&str]) -> std::io::Result<()> {
        let _ = run_cmd("tc", &["qdisc", "del", "dev", &self.veth_host, "root"]);
        self.apply_netem(args)
    }
}

impl Drop for NetnsGuard {
    fn drop(&mut self) {
        let _ = run_cmd("ip", &["link", "del", &self.veth_host]);
        let _ = run_cmd("ip", &["netns", "del", &self.ns_name]);
    }
}

fn run_cmd(program: &str, args: &[&str]) -> std::io::Result<()> {
    let status = Command::new(program).args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "{program} {args:?} failed with {status}"
        )))
    }
}
