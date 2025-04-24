use criterion::{criterion_group, criterion_main, Criterion};
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
    // Create tables
    let queries = [
        "DROP TABLE IF EXISTS tx_bench_accounts",
        "CREATE TABLE tx_bench_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
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

    // Insert 100 accounts with $1000 balance each
    let mut stmt = limbo_conn
        .prepare("INSERT INTO tx_bench_accounts (id, name, balance) VALUES (?, ?, ?)")
        .unwrap();
    for i in 1..=100 {
        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(
            2.try_into().unwrap(),
            OwnedValue::Text(format!("Account {}", i).into()),
        );
        stmt.bind_at(3.try_into().unwrap(), OwnedValue::Float(1000.0));

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

fn bench_simple_transaction(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Simple Transaction");

    // Benchmark simple transaction with a single update
    group.bench_function("Limbo - Single Update", |b| {
        b.iter(|| {
            // Begin transaction
            let mut stmt = limbo_conn.prepare("BEGIN TRANSACTION").unwrap();
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

            // Execute single update
            let mut stmt = limbo_conn
                .prepare("UPDATE tx_bench_accounts SET balance = 1100.0 WHERE id = 1")
                .unwrap();
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

            // Commit transaction
            let mut stmt = limbo_conn.prepare("COMMIT").unwrap();
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

        // Setup test data for SQLite
        sqlite_conn
            .execute("DROP TABLE IF EXISTS tx_bench_accounts", [])
            .unwrap();
        sqlite_conn
            .execute(
                "CREATE TABLE tx_bench_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
                [],
            )
            .unwrap();

        let mut stmt = sqlite_conn
            .prepare("INSERT INTO tx_bench_accounts (id, name, balance) VALUES (?, ?, ?)")
            .unwrap();
        for i in 1..=100 {
            stmt.execute((&i, format!("Account {}", i), 1000.0))
                .unwrap();
        }

        group.bench_function("Sqlite3 - Single Update", |b| {
            b.iter(|| {
                sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
                sqlite_conn
                    .execute(
                        "UPDATE tx_bench_accounts SET balance = 1100.0 WHERE id = 1",
                        [],
                    )
                    .unwrap();
                sqlite_conn.execute("COMMIT", []).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_complex_transaction(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Complex Transaction");

    // Benchmark a money transfer between accounts (two updates in one transaction)
    group.bench_function("Limbo - Money Transfer", |b| {
        b.iter(|| {
            // Begin transaction
            let mut stmt = limbo_conn.prepare("BEGIN TRANSACTION").unwrap();
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

            // Decrement balance from first account
            let mut stmt = limbo_conn
                .prepare("UPDATE tx_bench_accounts SET balance = balance - 100.0 WHERE id = 1")
                .unwrap();
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

            // Increment balance of second account
            let mut stmt = limbo_conn
                .prepare("UPDATE tx_bench_accounts SET balance = balance + 100.0 WHERE id = 2")
                .unwrap();
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

            // Commit transaction
            let mut stmt = limbo_conn.prepare("COMMIT").unwrap();
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

        // Setup test data for SQLite
        sqlite_conn
            .execute("DROP TABLE IF EXISTS tx_bench_accounts", [])
            .unwrap();
        sqlite_conn
            .execute(
                "CREATE TABLE tx_bench_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
                [],
            )
            .unwrap();

        let mut stmt = sqlite_conn
            .prepare("INSERT INTO tx_bench_accounts (id, name, balance) VALUES (?, ?, ?)")
            .unwrap();
        for i in 1..=100 {
            stmt.execute((&i, format!("Account {}", i), 1000.0))
                .unwrap();
        }

        group.bench_function("Sqlite3 - Money Transfer", |b| {
            b.iter(|| {
                sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
                sqlite_conn
                    .execute(
                        "UPDATE tx_bench_accounts SET balance = balance - 100.0 WHERE id = 1",
                        [],
                    )
                    .unwrap();
                sqlite_conn
                    .execute(
                        "UPDATE tx_bench_accounts SET balance = balance + 100.0 WHERE id = 2",
                        [],
                    )
                    .unwrap();
                sqlite_conn.execute("COMMIT", []).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_transaction_rollback(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Transaction Rollback");

    // Benchmark transaction rollback
    group.bench_function("Limbo - Rollback", |b| {
        b.iter(|| {
            // Begin transaction
            let mut stmt = limbo_conn.prepare("BEGIN TRANSACTION").unwrap();
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

            // Execute a modification that will be rolled back
            let mut stmt = limbo_conn
                .prepare("UPDATE tx_bench_accounts SET balance = 0.0 WHERE id <= 50")
                .unwrap();
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

            // Roll back the transaction
            let mut stmt = limbo_conn.prepare("ROLLBACK").unwrap();
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

        // Setup test data for SQLite
        sqlite_conn
            .execute("DROP TABLE IF EXISTS tx_bench_accounts", [])
            .unwrap();
        sqlite_conn
            .execute(
                "CREATE TABLE tx_bench_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
                [],
            )
            .unwrap();

        let mut stmt = sqlite_conn
            .prepare("INSERT INTO tx_bench_accounts (id, name, balance) VALUES (?, ?, ?)")
            .unwrap();
        for i in 1..=100 {
            stmt.execute((&i, format!("Account {}", i), 1000.0))
                .unwrap();
        }

        group.bench_function("Sqlite3 - Rollback", |b| {
            b.iter(|| {
                sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
                sqlite_conn
                    .execute(
                        "UPDATE tx_bench_accounts SET balance = 0.0 WHERE id <= 50",
                        [],
                    )
                    .unwrap();
                sqlite_conn.execute("ROLLBACK", []).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_bulk_transaction(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Bulk Transaction");

    group.bench_function("Limbo - Bulk Update", |b| {
        b.iter(|| {
            // Begin transaction
            let mut stmt = limbo_conn.prepare("BEGIN TRANSACTION").unwrap();
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

            // Execute 50 updates in a single transaction
            for i in 1..=50 {
                let mut stmt = limbo_conn
                    .prepare(format!(
                        "UPDATE tx_bench_accounts SET balance = {} WHERE id = {}",
                        1000.0 + (i as f64),
                        i
                    ))
                    .unwrap();
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

            // Commit transaction
            let mut stmt = limbo_conn.prepare("COMMIT").unwrap();
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

        // Setup test data for SQLite
        sqlite_conn
            .execute("DROP TABLE IF EXISTS tx_bench_accounts", [])
            .unwrap();
        sqlite_conn
            .execute(
                "CREATE TABLE tx_bench_accounts (id INTEGER PRIMARY KEY, name TEXT, balance REAL)",
                [],
            )
            .unwrap();

        let mut stmt = sqlite_conn
            .prepare("INSERT INTO tx_bench_accounts (id, name, balance) VALUES (?, ?, ?)")
            .unwrap();
        for i in 1..=100 {
            stmt.execute((&i, format!("Account {}", i), 1000.0))
                .unwrap();
        }

        group.bench_function("Sqlite3 - Bulk Update", |b| {
            b.iter(|| {
                sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
                for i in 1..=50 {
                    sqlite_conn
                        .execute(
                            &format!(
                                "UPDATE tx_bench_accounts SET balance = {} WHERE id = {}",
                                1000.0 + (i as f64),
                                i
                            ),
                            [],
                        )
                        .unwrap();
                }
                sqlite_conn.execute("COMMIT", []).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_simple_transaction, bench_complex_transaction, bench_transaction_rollback, bench_bulk_transaction
}
criterion_main!(benches);
