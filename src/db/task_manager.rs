use refinery::{Migration, Runner};
use refinery_migrations::MigrationPrefix;

mod v1_create_snapshots;

// A hack because intellij-rust doesn't like working on modules that isn't clearly declared.
pub fn runner() -> Runner {
    let migrations = vec![Migration {
        name: "v1_create_snapshots".to_string(),
        version: 1,
        prefix: MigrationPrefix::Versioned,
        sql: v1_create_snapshots::migration(),
    }];

    Runner::new(&migrations)
}
