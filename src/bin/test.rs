extern crate gazpacho;
use gazpacho::daemon::config::Configuration;
use uclicious::{Priority, DEFAULT_DUPLICATE_STRATEGY};

unsafe fn check_root() {
    let uid = libc::getuid();
    if uid != 0 {
        panic!("Not running as root")
    }
}
fn main() {
    //unsafe { check_root() }
    gazpacho::daemon::start_daemon();
}
