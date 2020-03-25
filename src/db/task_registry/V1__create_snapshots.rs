use barrel::{types, Migration, backend::Sqlite};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("snapshots", |t| {
        t.add_column("id", types::primary().increments(true));
        t.add_column("task", types::text().nullable(false));
        t.add_column("pool", types::text().nullable(false));
        t.add_column("dataset", types::text().nullable(false));
        t.add_column("snapshot", types::text().nullable(false));
        t.add_column("started_at", types::text().nullable(false));
        t.add_column("completed_at", types::text().nullable(true));

        t.add_index("idx_pool_dataset_task",
                    types::index(vec![
                        "pool",
                        "dataset",
                        "task",
                    ])
                        .unique(true)
                        .nullable(false))
    });

    m.make::<Sqlite>()
}