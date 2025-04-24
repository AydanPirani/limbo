use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, OwnedValue, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

fn rusqlite_open(path: &str) -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open(path).unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn setup_test_data(limbo_conn: Rc<limbo_core::Connection>, io: &Arc<PlatformIO>) {
    // Create test table
    let queries = [
        "DROP TABLE IF EXISTS mvcc_bench_data",
        "CREATE TABLE mvcc_bench_data (id INTEGER PRIMARY KEY, value INTEGER, updated_at TEXT)"
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

    // Insert 1000 rows
    let mut stmt = limbo_conn.prepare("INSERT INTO mvcc_bench_data (id, value, updated_at) VALUES (?, ?, datetime('now'))").unwrap();
    for i in 1..=1000 {
        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(2.try_into().unwrap(), OwnedValue::Integer(i * 10));
        
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

fn bench_read_during_write_transaction(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Read During Write Transaction");

    // Benchmark reading from a table while a write transaction is active
    group.bench_function("Limbo - Read During Write", |b| {
        b.iter(|| {
            // Start a write transaction in the main connection that updates some rows
            let mut begin_stmt = limbo_conn.prepare("BEGIN TRANSACTION").unwrap();
            loop {
                match begin_stmt.step().unwrap() {
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

            // Update some rows
            let mut update_stmt = limbo_conn.prepare("UPDATE mvcc_bench_data SET value = value + 1, updated_at = datetime('now') WHERE id <= 500").unwrap();
            loop {
                match update_stmt.step().unwrap() {
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

            // Now open a second connection and read the data
            // In SQLite this would block, but in MVCC it should read the old version
            let reader_db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
            let reader_conn = reader_db.connect().unwrap();
            
            let mut read_stmt = reader_conn.prepare("SELECT * FROM mvcc_bench_data LIMIT 1000").unwrap();
            loop {
                match read_stmt.step().unwrap() {
                    limbo_core::StepResult::Row => {
                        black_box(read_stmt.row());
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

            // Commit the transaction
            let mut commit_stmt = limbo_conn.prepare("COMMIT").unwrap();
            loop {
                match commit_stmt.step().unwrap() {
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
        // For comparison, SQLite will block reads during a write transaction,
        // so we need to use a slightly different approach for fairness
        
        group.bench_function("Sqlite3 - Read After Write", |b| {
            b.iter(|| {
                // Setup fresh SQLite connection
                let sqlite_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                
                // Create and populate the test table
                sqlite_conn.execute("DROP TABLE IF EXISTS mvcc_bench_data", []).unwrap();
                sqlite_conn.execute("CREATE TABLE mvcc_bench_data (id INTEGER PRIMARY KEY, value INTEGER, updated_at TEXT)", []).unwrap();
                
                let mut stmt = sqlite_conn.prepare("INSERT INTO mvcc_bench_data (id, value, updated_at) VALUES (?, ?, datetime('now'))").unwrap();
                for i in 1..=1000 {
                    stmt.execute((i, i * 10)).unwrap();
                }
                
                // Start a write transaction that updates some rows
                sqlite_conn.execute("BEGIN TRANSACTION", []).unwrap();
                sqlite_conn.execute("UPDATE mvcc_bench_data SET value = value + 1, updated_at = datetime('now') WHERE id <= 500", []).unwrap();
                
                // Commit the transaction
                sqlite_conn.execute("COMMIT", []).unwrap();
                
                // Now read the data (after the write transaction is complete)
                let mut read_stmt = sqlite_conn.prepare("SELECT * FROM mvcc_bench_data LIMIT 1000").unwrap();
                let mut rows = read_stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

fn bench_concurrent_readers(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Concurrent Readers");

    // Benchmark multiple concurrent reader connections
    let reader_counts = [2, 4, 8];
    
    for &num_readers in reader_counts.iter() {
        group.bench_with_input(
            BenchmarkId::new("Limbo - Concurrent Readers", num_readers),
            &num_readers,
            |b, &num_readers| {
                b.iter(|| {
                    // Create thread handles
                    let mut handles = Vec::with_capacity(num_readers);
                    
                    // Spawn reader threads
                    for _ in 0..num_readers {
                        let io_clone = io.clone();
                        
                        let handle = thread::spawn(move || {
                            // Open database connection in each thread
                            let thread_db = Database::open_file(io_clone.clone(), "../testing/mvcc_testing.db", false).unwrap();
                            let thread_conn = thread_db.connect().unwrap();
                            
                            // Execute a read query
                            let mut stmt = thread_conn.prepare("SELECT * FROM mvcc_bench_data ORDER BY id LIMIT 1000").unwrap();
                            
                            // Process the results
                            loop {
                                match stmt.step().unwrap() {
                                    limbo_core::StepResult::Row => {
                                        black_box(stmt.row());
                                    }
                                    limbo_core::StepResult::IO => {
                                        let _ = io_clone.run_once();
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
                        
                        handles.push(handle);
                    }
                    
                    // Wait for all threads to complete
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
        
        if enable_rusqlite {
            group.bench_with_input(
                BenchmarkId::new("Sqlite3 - Concurrent Readers", num_readers),
                &num_readers,
                |b, &num_readers| {
                    b.iter(|| {
                        // Create thread handles
                        let mut handles = Vec::with_capacity(num_readers);
                        
                        // Setup fresh SQLite database
                        let setup_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                        setup_conn.execute("DROP TABLE IF EXISTS mvcc_bench_data", []).unwrap();
                        setup_conn.execute("CREATE TABLE mvcc_bench_data (id INTEGER PRIMARY KEY, value INTEGER, updated_at TEXT)", []).unwrap();
                        
                        let mut stmt = setup_conn.prepare("INSERT INTO mvcc_bench_data (id, value, updated_at) VALUES (?, ?, datetime('now'))").unwrap();
                        for i in 1..=1000 {
                            stmt.execute((i, i * 10)).unwrap();
                        }
                        drop(stmt);
                        drop(setup_conn);
                        
                        // Spawn reader threads
                        for _ in 0..num_readers {
                            let handle = thread::spawn(move || {
                                let thread_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                                let mut stmt = thread_conn.prepare("SELECT * FROM mvcc_bench_data ORDER BY id LIMIT 1000").unwrap();
                                let mut rows = stmt.raw_query();
                                while let Some(row) = rows.next().unwrap() {
                                    black_box(row);
                                }
                            });
                            
                            handles.push(handle);
                        }
                        
                        // Wait for all threads to complete
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_transaction_isolation(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    let mut group = criterion.benchmark_group("Transaction Isolation");

    // Benchmark transaction isolation behavior
    group.bench_function("Limbo - Read After Transaction Start", |b| {
        b.iter(|| {
            // Open two connections
            let writer_db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
            let writer_conn = writer_db.connect().unwrap();
            
            let reader_db = Database::open_file(io.clone(), "../testing/mvcc_testing.db", false).unwrap();
            let reader_conn = reader_db.connect().unwrap();
            
            // Begin a transaction on the reader connection
            let mut reader_begin_stmt = reader_conn.prepare("BEGIN TRANSACTION").unwrap();
            loop {
                match reader_begin_stmt.step().unwrap() {
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
            
            // Execute a write on the writer connection
            let mut writer_begin_stmt = writer_conn.prepare("BEGIN TRANSACTION").unwrap();
            loop {
                match writer_begin_stmt.step().unwrap() {
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
            
            let mut writer_update_stmt = writer_conn.prepare("UPDATE mvcc_bench_data SET value = value * 2 WHERE id <= 500").unwrap();
            loop {
                match writer_update_stmt.step().unwrap() {
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
            
            let mut writer_commit_stmt = writer_conn.prepare("COMMIT").unwrap();
            loop {
                match writer_commit_stmt.step().unwrap() {
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
            
            // Now read from the reader connection - should see pre-update values due to isolation
            let mut reader_select_stmt = reader_conn.prepare("SELECT * FROM mvcc_bench_data WHERE id <= 500").unwrap();
            loop {
                match reader_select_stmt.step().unwrap() {
                    limbo_core::StepResult::Row => {
                        black_box(reader_select_stmt.row());
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
            
            // Commit the reader transaction
            let mut reader_commit_stmt = reader_conn.prepare("COMMIT").unwrap();
            loop {
                match reader_commit_stmt.step().unwrap() {
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
        group.bench_function("Sqlite3 - Read After Transaction Start", |b| {
            b.iter(|| {
                // Setup fresh SQLite database
                let setup_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                setup_conn.execute("DROP TABLE IF EXISTS mvcc_bench_data", []).unwrap();
                setup_conn.execute("CREATE TABLE mvcc_bench_data (id INTEGER PRIMARY KEY, value INTEGER, updated_at TEXT)", []).unwrap();
                
                let mut stmt = setup_conn.prepare("INSERT INTO mvcc_bench_data (id, value, updated_at) VALUES (?, ?, datetime('now'))").unwrap();
                for i in 1..=1000 {
                    stmt.execute((i, i * 10)).unwrap();
                }
                
                drop(stmt);
                drop(setup_conn);
                
                // Open two connections
                let writer_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                let reader_conn = rusqlite_open("../testing/sqlite_mvcc_testing.db");
                
                // Begin a transaction on the reader connection
                reader_conn.execute("BEGIN TRANSACTION", []).unwrap();
                
                // Execute a write on the writer connection
                writer_conn.execute("BEGIN TRANSACTION", []).unwrap();
                writer_conn.execute("UPDATE mvcc_bench_data SET value = value * 2 WHERE id <= 500", []).unwrap();
                writer_conn.execute("COMMIT", []).unwrap();
                
                // Now read from the reader connection - should see pre-update values due to isolation
                let mut read_stmt = reader_conn.prepare("SELECT * FROM mvcc_bench_data WHERE id <= 500").unwrap();
                let mut rows = read_stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
                
                // Commit the reader transaction
                reader_conn.execute("COMMIT", []).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_read_during_write_transaction, bench_concurrent_readers, bench_transaction_isolation
}
criterion_main!(benches);