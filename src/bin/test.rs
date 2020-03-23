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
    unsafe { check_root() }
    let input = r#"
        destination "fulcrum" {
            ssh {
                username = "andoriyu",
                identity_file = "/home/andoriyu/.ssh/id_rsa",
                folder = "/mnt/eden/backups/gazpacho/nimble",
                host = "192.168.86.13:22"
            }
        }
        task "test" {
            destination = "fulcrum",
            full_replication {
                zpool = "z",
                filter = "z\/usr\/ports$"
            }
            compression {
                zstd {
                    level = 3,
                    workers = 2,
                }
            }
        }
    "#;
    let mut builder = Configuration::builder().unwrap();
    builder
        .add_chunk_full(input, Priority::default(), DEFAULT_DUPLICATE_STRATEGY)
        .unwrap();
    let conf = builder.build().unwrap();

    let mut ctx = conf.get_execution_context_for_task("test").unwrap();
    dbg!(&ctx);

    let result = ctx.execute();
    dbg!(result);
}
