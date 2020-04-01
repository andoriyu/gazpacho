use barrel::{backend::Sqlite, types, Migration};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("reset_count", |t| {
        t.add_column("task", types::text().nullable(false).unique(true));
        t.add_column("count", types::integer().nullable(false));
        t.add_column("reset_at", types::text().nullable(false));

        t.add_index(
            "idx_reset_count_task",
            types::index(vec!["task"])
        );
    });

    m.make::<Sqlite>()
}
