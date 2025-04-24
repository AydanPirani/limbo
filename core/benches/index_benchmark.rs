use criterion::{black_box, criterion_group, criterion_main, Criterion};
use limbo_core::{Database, OwnedValue, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::{rc::Rc, sync::Arc};

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/testing.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn setup_test_data(limbo_conn: Rc<limbo_core::Connection>, io: &Arc<PlatformIO>) {
    // Create tables and indexes
    let queries = [
        "DROP TABLE IF EXISTS idx_bench_users",
        "DROP TABLE IF EXISTS idx_bench_posts",
        "DROP TABLE IF EXISTS idx_bench_tags",
        "CREATE TABLE idx_bench_users (id INTEGER PRIMARY KEY, username TEXT, email TEXT, created_at TEXT)",
        "CREATE TABLE idx_bench_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, content TEXT, published_at TEXT)",
        "CREATE TABLE idx_bench_tags (id INTEGER PRIMARY KEY, post_id INTEGER, name TEXT)",
        "CREATE INDEX idx_users_username ON idx_bench_users(username)",
        "CREATE INDEX idx_users_email ON idx_bench_users(email)",
        "CREATE INDEX idx_posts_user_id ON idx_bench_posts(user_id)",
        "CREATE INDEX idx_posts_published_at ON idx_bench_posts(published_at)",
        "CREATE INDEX idx_tags_post_id ON idx_bench_tags(post_id)",
        "CREATE INDEX idx_tags_name ON idx_bench_tags(name)",
    ];

    for query in queries.iter() {
        let mut stmt = limbo_conn.prepare(query).unwrap();
        loop {
            match stmt.step().unwrap() {
                limbo_core::StepResult::Row => {}
                limbo_core::StepResult::IO => {
                    let _ = io.run_once();
                }
                limbo_core::StepResult::Done => {
                    break;
                }
                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                    unreachable!();
                }
            }
        }
    }

    // Insert test data into users table
    let mut stmt = limbo_conn
        .prepare(
            "INSERT INTO idx_bench_users (id, username, email, created_at) VALUES (?, ?, ?, ?)",
        )
        .unwrap();
    for i in 1..=1000 {
        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(
            2.try_into().unwrap(),
            OwnedValue::Text(format!("user{}", i).into()),
        );
        stmt.bind_at(
            3.try_into().unwrap(),
            OwnedValue::Text(format!("user{}@example.com", i).into()),
        );
        stmt.bind_at(
            4.try_into().unwrap(),
            OwnedValue::Text("2023-01-01".to_string().into()),
        );

        loop {
            match stmt.step().unwrap() {
                limbo_core::StepResult::Row => {}
                limbo_core::StepResult::IO => {
                    let _ = io.run_once();
                }
                limbo_core::StepResult::Done => {
                    break;
                }
                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                    unreachable!();
                }
            }
        }
        stmt.reset();
    }

    // Insert test data into posts table
    let mut stmt = limbo_conn.prepare("INSERT INTO idx_bench_posts (id, user_id, title, content, published_at) VALUES (?, ?, ?, ?, ?)").unwrap();
    for i in 1..=5000 {
        let user_id = (i % 1000) + 1;
        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(2.try_into().unwrap(), OwnedValue::Integer(user_id));
        stmt.bind_at(
            3.try_into().unwrap(),
            OwnedValue::Text(format!("Post title {}", i).into()),
        );
        stmt.bind_at(
            4.try_into().unwrap(),
            OwnedValue::Text(format!("Post content {}", i).into()),
        );
        stmt.bind_at(
            5.try_into().unwrap(),
            OwnedValue::Text("2023-01-01".to_string().into()),
        );

        loop {
            match stmt.step().unwrap() {
                limbo_core::StepResult::Row => {}
                limbo_core::StepResult::IO => {
                    let _ = io.run_once();
                }
                limbo_core::StepResult::Done => {
                    break;
                }
                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                    unreachable!();
                }
            }
        }
        stmt.reset();
    }

    // Insert test data into tags table
    let mut stmt = limbo_conn
        .prepare("INSERT INTO idx_bench_tags (id, post_id, name) VALUES (?, ?, ?)")
        .unwrap();
    for i in 1..=10000 {
        let post_id = (i % 5000) + 1;
        let tag_name = match i % 10 {
            0 => "technology",
            1 => "programming",
            2 => "rust",
            3 => "database",
            4 => "sql",
            5 => "limbo",
            6 => "sqlite",
            7 => "performance",
            8 => "benchmark",
            _ => "other",
        };

        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(2.try_into().unwrap(), OwnedValue::Integer(post_id));
        stmt.bind_at(
            3.try_into().unwrap(),
            OwnedValue::Text(tag_name.to_string().into()),
        );

        loop {
            match stmt.step().unwrap() {
                limbo_core::StepResult::Row => {}
                limbo_core::StepResult::IO => {
                    let _ = io.run_once();
                }
                limbo_core::StepResult::Done => {
                    break;
                }
                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                    unreachable!();
                }
            }
        }
        stmt.reset();
    }
}

fn bench_index_queries(criterion: &mut Criterion) {
    // Flag to disable rusqlite benchmarks if needed
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    // Define the queries to benchmark
    let queries = [
        (
            "Simple Index Lookup",
            "SELECT * FROM idx_bench_users WHERE username = 'user500'"
        ),
        (
            "Complex JOIN with Index",
            "SELECT u.username, p.title, t.name FROM idx_bench_users u 
             JOIN idx_bench_posts p ON u.id = p.user_id 
             JOIN idx_bench_tags t ON p.id = t.post_id 
             WHERE u.id = 500 AND t.name = 'rust'"
        ),
        (
            "Range Query with Index",
            "SELECT * FROM idx_bench_posts WHERE user_id BETWEEN 100 AND 200 ORDER BY user_id"
        ),
        (
            "Count Aggregation with Index",
            "SELECT user_id, COUNT(*) FROM idx_bench_posts GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10"
        ),
        (
            "Compound Filter with Indexes",
            "SELECT p.id, p.title, u.username FROM idx_bench_posts p 
             JOIN idx_bench_users u ON p.user_id = u.id 
             WHERE p.user_id > 500 AND p.title LIKE 'Post title 1%' LIMIT 20"
        ),
    ];

    for (name, query) in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Index Query - {}", name));

        group.bench_function("Limbo", |b| {
            let mut stmt = limbo_conn.prepare(query).unwrap();
            let io = io.clone();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {
                            black_box(stmt.row());
                        }
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset();
            });
        });

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            // Execute the setup queries on SQLite as well
            let setup_queries = [
                "DROP TABLE IF EXISTS idx_bench_users",
                "DROP TABLE IF EXISTS idx_bench_posts",
                "DROP TABLE IF EXISTS idx_bench_tags",
                "CREATE TABLE idx_bench_users (id INTEGER PRIMARY KEY, username TEXT, email TEXT, created_at TEXT)",
                "CREATE TABLE idx_bench_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, content TEXT, published_at TEXT)",
                "CREATE TABLE idx_bench_tags (id INTEGER PRIMARY KEY, post_id INTEGER, name TEXT)",
                "CREATE INDEX idx_users_username ON idx_bench_users(username)",
                "CREATE INDEX idx_users_email ON idx_bench_users(email)",
                "CREATE INDEX idx_posts_user_id ON idx_bench_posts(user_id)",
                "CREATE INDEX idx_posts_published_at ON idx_bench_posts(published_at)",
                "CREATE INDEX idx_tags_post_id ON idx_bench_tags(post_id)",
                "CREATE INDEX idx_tags_name ON idx_bench_tags(name)",
            ];

            for setup_query in setup_queries.iter() {
                sqlite_conn.execute(setup_query, []).unwrap();
            }

            // Insert test data into users table
            let mut stmt = sqlite_conn.prepare("INSERT INTO idx_bench_users (id, username, email, created_at) VALUES (?, ?, ?, ?)").unwrap();
            for i in 1..=1000 {
                stmt.execute((
                    &i,
                    format!("user{}", i),
                    format!("user{}@example.com", i),
                    "2023-01-01",
                ))
                .unwrap();
            }

            // Insert test data into posts table
            let mut stmt = sqlite_conn.prepare("INSERT INTO idx_bench_posts (id, user_id, title, content, published_at) VALUES (?, ?, ?, ?, ?)").unwrap();
            for i in 1..=5000 {
                let user_id = (i % 1000) + 1;
                stmt.execute((
                    &i,
                    &user_id,
                    format!("Post title {}", i),
                    format!("Post content {}", i),
                    "2023-01-01",
                ))
                .unwrap();
            }

            // Insert test data into tags table
            let mut stmt = sqlite_conn
                .prepare("INSERT INTO idx_bench_tags (id, post_id, name) VALUES (?, ?, ?)")
                .unwrap();
            for i in 1..=10000 {
                let post_id = (i % 5000) + 1;
                let tag_name = match i % 10 {
                    0 => "technology",
                    1 => "programming",
                    2 => "rust",
                    3 => "database",
                    4 => "sql",
                    5 => "limbo",
                    6 => "sqlite",
                    7 => "performance",
                    8 => "benchmark",
                    _ => "other",
                };

                stmt.execute((&i, &post_id, tag_name)).unwrap();
            }

            group.bench_function("Sqlite3", |b| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });
        }

        group.finish();
    }
}

fn bench_index_creation(criterion: &mut Criterion) {
    // Flag to disable rusqlite benchmarks if needed
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define the index creation statements to benchmark
    let index_creation_statements = [
        (
            "Simple Index",
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)",
            "CREATE INDEX idx_test_value ON test_table(value)"
        ),
        (
            "Composite Index",
            "CREATE TABLE IF NOT EXISTS test_composite (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c REAL)",
            "CREATE INDEX idx_test_composite ON test_composite(a, b, c)"
        ),
        (
            "Unique Index",
            "CREATE TABLE IF NOT EXISTS test_unique (id INTEGER PRIMARY KEY, code TEXT)",
            "CREATE UNIQUE INDEX idx_test_unique ON test_unique(code)"
        ),
    ];

    for (name, create_table, create_index) in index_creation_statements.iter() {
        let mut group = criterion.benchmark_group(format!("Index Creation - {}", name));

        group.bench_function("Limbo", |b| {
            b.iter(|| {
                // First drop the table and index to ensure clean benchmark
                let drop_query = format!(
                    "DROP TABLE IF EXISTS {}",
                    create_table
                        .split_whitespace()
                        .nth(2)
                        .unwrap()
                        .trim_end_matches(" (")
                );

                let mut stmt = limbo_conn.prepare(&drop_query).unwrap();
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {}
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }

                // Create the table
                let mut stmt = limbo_conn.prepare(create_table).unwrap();
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {}
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }

                // Insert some data
                let table_name = create_table
                    .split_whitespace()
                    .nth(2)
                    .unwrap()
                    .trim_end_matches(" (");
                let insert_query = match table_name {
                    "test_table" => format!("INSERT INTO {} (id, value) VALUES (?, ?)", table_name),
                    "test_composite" => format!(
                        "INSERT INTO {} (id, a, b, c) VALUES (?, ?, ?, ?)",
                        table_name
                    ),
                    "test_unique" => format!("INSERT INTO {} (id, code) VALUES (?, ?)", table_name),
                    _ => panic!("Unknown table name"),
                };

                let mut stmt = limbo_conn.prepare(&insert_query).unwrap();

                // Insert 1000 rows
                for i in 1..=1000 {
                    stmt.reset();

                    // Bind parameters based on table type
                    match table_name {
                        "test_table" => {
                            stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
                            stmt.bind_at(
                                2.try_into().unwrap(),
                                OwnedValue::Text(format!("value{}", i).into()),
                            );
                        }
                        "test_composite" => {
                            stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
                            stmt.bind_at(2.try_into().unwrap(), OwnedValue::Integer(i % 100));
                            stmt.bind_at(
                                3.try_into().unwrap(),
                                OwnedValue::Text(format!("text{}", i).into()),
                            );
                            stmt.bind_at(
                                4.try_into().unwrap(),
                                OwnedValue::Float((i as f64) / 100.0),
                            );
                        }
                        "test_unique" => {
                            stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
                            stmt.bind_at(
                                2.try_into().unwrap(),
                                OwnedValue::Text(format!("CODE{:05}", i).into()),
                            );
                        }
                        _ => panic!("Unknown table name"),
                    }

                    loop {
                        match stmt.step().unwrap() {
                            limbo_core::StepResult::Row => {}
                            limbo_core::StepResult::IO => {
                                let _ = io.run_once();
                            }
                            limbo_core::StepResult::Done => {
                                break;
                            }
                            limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                                unreachable!();
                            }
                        }
                    }
                }

                // Now create the index (this is what we're actually benchmarking)
                let mut stmt = limbo_conn.prepare(create_index).unwrap();
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {}
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
            });
        });

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_function("Sqlite3", |b| {
                b.iter(|| {
                    // First drop the table and index to ensure clean benchmark
                    let drop_query = format!(
                        "DROP TABLE IF EXISTS {}",
                        create_table
                            .split_whitespace()
                            .nth(2)
                            .unwrap()
                            .trim_end_matches(" (")
                    );
                    sqlite_conn.execute(&drop_query, []).unwrap();

                    // Create the table
                    sqlite_conn.execute(create_table, []).unwrap();

                    // Insert some data
                    let table_name = create_table
                        .split_whitespace()
                        .nth(2)
                        .unwrap()
                        .trim_end_matches(" (");

                    // Insert 1000 rows
                    match table_name {
                        "test_table" => {
                            let mut stmt = sqlite_conn
                                .prepare(&format!(
                                    "INSERT INTO {} (id, value) VALUES (?, ?)",
                                    table_name
                                ))
                                .unwrap();
                            for i in 1..=1000 {
                                stmt.execute((&i, format!("value{}", i))).unwrap();
                            }
                        }
                        "test_composite" => {
                            let mut stmt = sqlite_conn
                                .prepare(&format!(
                                    "INSERT INTO {} (id, a, b, c) VALUES (?, ?, ?, ?)",
                                    table_name
                                ))
                                .unwrap();
                            for i in 1..=1000 {
                                stmt.execute((
                                    &i,
                                    &(i % 100),
                                    format!("text{}", i),
                                    (i as f64) / 100.0,
                                ))
                                .unwrap();
                            }
                        }
                        "test_unique" => {
                            let mut stmt = sqlite_conn
                                .prepare(&format!(
                                    "INSERT INTO {} (id, code) VALUES (?, ?)",
                                    table_name
                                ))
                                .unwrap();
                            for i in 1..=1000 {
                                stmt.execute((&i, format!("CODE{:05}", i))).unwrap();
                            }
                        }
                        _ => panic!("Unknown table name"),
                    }

                    // Now create the index (this is what we're actually benchmarking)
                    sqlite_conn.execute(create_index, []).unwrap();
                });
            });
        }

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_index_queries, bench_index_creation
}
criterion_main!(benches);
