use refinery::{Migration, Runner};
use refinery_migrations::MigrationPrefix;

mod v1_create_task_log;
mod v2_create_step_log;

// A hack because intellij-rust doesn't like working on modules that isn't clearly declared.
pub fn runner() -> Runner {
    let migrations = vec![
        Migration {
            name: "create_task_log".to_string(),
            version: 1,
            prefix: MigrationPrefix::Versioned,
            sql: v1_create_task_log::migration(),
        },
        Migration {
            name: "create_tasK_log".to_string(),
            version: 2,
            prefix: MigrationPrefix::Versioned,
            sql: v2_create_step_log::migration(),
        },
    ];

    Runner::new(&migrations)
}
