use refinery::{Migration, Runner};
use refinery_migrations::MigrationPrefix;

mod v1_create_task_log;
mod v2_create_step_log;
mod v3_reset_count;

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
            name: "create_step_log".to_string(),
            version: 2,
            prefix: MigrationPrefix::Versioned,
            sql: v2_create_step_log::migration(),
        },
        Migration {
            name: "create_reset_count".to_string(),
            version: 3,
            prefix: MigrationPrefix::Versioned,
            sql: v3_reset_count::migration(),
        },
    ];

    Runner::new(&migrations)
}
