use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO};

use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;
use limbo_core::IO;

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/database.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_join_query(criterion: &mut Criterion) {
    // Skip rusqlite if disabled via env var
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/database.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("join_query");

    // The join query
    let query = "SELECT o.id, u.first_name, o.product_id FROM users u JOIN orders_1000 o ON u.id = o.user_id";

    // Benchmark Limbo execution
    group.bench_with_input(
        BenchmarkId::new("limbo_hashjoin_orders_1000_execute", query),
        &query,
        |b, query| {
            let io = io.clone();
            b.iter(|| {
                let mut stmt = limbo_conn.prepare_hardcoded(query).unwrap();
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
                // stmt.reset();
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("limbo_orders_1000_execute", query),
        &query,
        |b, query| {
            let io = io.clone();
            b.iter(|| {
                let mut stmt = limbo_conn.prepare(query).unwrap();
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
                // stmt.reset();
            });
        },
    );

    let query2 = "SELECT o.id, u.first_name, o.product_id FROM users u JOIN orders_1000000 o ON u.id = o.user_id";

    group.bench_with_input(
        BenchmarkId::new("limbo_hashjoin_orders_1000000_execute", query2),
        &query2,
        |b, query2| {
            let io = io.clone();
            b.iter(|| {
                let mut stmt = limbo_conn.prepare_hardcoded(query2).unwrap();
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
                // stmt.reset();
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("limbo_orders_1000000_execute", query2),
        &query2,
        |b, query2| {
            let io = io.clone();
            b.iter(|| {
                let mut stmt = limbo_conn.prepare(query2).unwrap();
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
                // stmt.reset();
            });
        },
    );

    // if enable_rusqlite {
    //     let sqlite_conn = rusqlite_open();

    //     group.bench_with_input(
    //         BenchmarkId::new("sqlite_orders_1000_execute", query),
    //         &query,
    //         |b, query| {
    //             b.iter(|| {
    //                 let mut stmt = sqlite_conn.prepare(query).unwrap();
    //                 let mut rows = stmt.query([]).unwrap();
    //                 while let Some(row) = rows.next().unwrap() {
    //                     black_box(row);
    //                 }
    //             });
    //         },
    //     );
    // }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_join_query
}
criterion_main!(benches);
