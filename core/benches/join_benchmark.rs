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

    // The different order user_sizes we want to test
    let user_sizes = [
        1000,    // orders_1000
        10000,   // orders_10000
        100000,  // orders_100000
    ];
    let order_sizes = [
        1000,
        10000,
        100000,
    ];

    for &num_users in &user_sizes {
        for &num_orders in &order_sizes {
            let query = format!(
                "SELECT o.id, u.first_name, o.product_id FROM users_{0} u JOIN orders_{0}_{1} o ON u.id = o.user_id",
                num_users,
                num_orders
            );
    
            group.bench_with_input(
                BenchmarkId::new(format!("limbo_hashjoin_users{}_orders{}_execute", num_users, num_orders), &query),
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
                                limbo_core::StepResult::Done => break,
                                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                                    unreachable!();
                                }
                            }
                        }
                    });
                },
            );
    
            group.bench_with_input(
                BenchmarkId::new(format!("limbo_users{}_orders{}_execute", num_users, num_orders), &query),
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
                                limbo_core::StepResult::Done => break,
                                limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                                    unreachable!();
                                }
                            }
                        }
                    });
                },
            );
    
            if enable_rusqlite {
                let sqlite_conn = rusqlite_open();
    
                group.bench_with_input(
                    BenchmarkId::new(format!("sqlite_users{}_orders{}_execute", num_users, num_orders), &query),
                    &query,
                    |b, query| {
                        b.iter(|| {
                            let mut stmt = sqlite_conn.prepare(query).unwrap();
                            let mut rows = stmt.query([]).unwrap();
                            while let Some(row) = rows.next().unwrap() {
                                black_box(row);
                            }
                        });
                    },
                );
            }
        }

    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_join_query
}
criterion_main!(benches);
