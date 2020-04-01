use barrel::{backend::Sqlite, types, Migration};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("step_log", |t| {
        t.add_column("id", types::primary().increments(true));
        t.add_column(
            "run_id",
            types::foreign("task_log", vec![String::from("id")]),
        );
        t.add_column("state", types::text().nullable(false));
        t.add_column("task", types::text().nullable(false));
        t.add_column("pool", types::text().nullable(false));
        t.add_column("dataset", types::text().nullable(false));
        t.add_column("snapshot", types::text().nullable(false));
        t.add_column("started_at", types::text().nullable(false));
        t.add_column("completed_at", types::text().nullable(true));
    });

    m.make::<Sqlite>()
}