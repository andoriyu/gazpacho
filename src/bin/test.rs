extern crate gazpacho;

#[allow(dead_code)]
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
