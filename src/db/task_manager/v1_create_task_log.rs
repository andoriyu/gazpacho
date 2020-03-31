use barrel::{backend::Sqlite, types, Migration};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("task_log", |t| {
        t.add_column("id", types::primary().increments(true));
        t.add_column("task", types::text().nullable(false));
        t.add_column("state", types::text().nullable(false));
        t.add_column("started_at", types::text().nullable(false));
        t.add_column("completed_at", types::text().nullable(true));

        t.add_index(
            "idx_task_log_task",
            types::index(vec!["task"])
                .unique(false)
                .nullable(false),
        )
    });

    m.make::<Sqlite>()
}
