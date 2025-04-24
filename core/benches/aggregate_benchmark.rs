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
    // Create sales table
    let queries = [
        "DROP TABLE IF EXISTS agg_bench_sales",
        "CREATE TABLE agg_bench_sales (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            category_id INTEGER,
            date TEXT,
            quantity INTEGER,
            unit_price REAL,
            customer_id INTEGER
        )",
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

    // Insert 10,000 sales records with varied data
    let mut stmt = limbo_conn.prepare(
        "INSERT INTO agg_bench_sales (id, product_id, category_id, date, quantity, unit_price, customer_id) 
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=10000 {
        // Generate some varied data for testing aggregates
        let product_id = i % 100 + 1;
        let category_id = product_id % 10 + 1;
        let year = 2020 + (i % 4);
        let month = (i % 12) + 1;
        let day = (i % 28) + 1;
        let date = format!("{}-{:02}-{:02}", year, month, day);
        let quantity = (i % 10) + 1;
        let unit_price = 10.0 + ((i % 50) as f64);
        let customer_id = (i % 500) + 1;

        stmt.bind_at(1.try_into().unwrap(), OwnedValue::Integer(i));
        stmt.bind_at(2.try_into().unwrap(), OwnedValue::Integer(product_id));
        stmt.bind_at(3.try_into().unwrap(), OwnedValue::Integer(category_id));
        stmt.bind_at(4.try_into().unwrap(), OwnedValue::Text(date.into()));
        stmt.bind_at(5.try_into().unwrap(), OwnedValue::Integer(quantity));
        stmt.bind_at(6.try_into().unwrap(), OwnedValue::Float(unit_price));
        stmt.bind_at(7.try_into().unwrap(), OwnedValue::Integer(customer_id));

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

    // Create index to help with aggregates
    let mut stmt = limbo_conn
        .prepare("CREATE INDEX idx_sales_category ON agg_bench_sales(category_id)")
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

    let mut stmt = limbo_conn
        .prepare("CREATE INDEX idx_sales_date ON agg_bench_sales(date)")
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

fn bench_simple_aggregates(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    // Define the aggregate queries to benchmark
    let aggregate_queries = [
        (
            "COUNT",
            "SELECT COUNT(*) FROM agg_bench_sales"
        ),
        (
            "SUM",
            "SELECT SUM(quantity) FROM agg_bench_sales"
        ),
        (
            "AVG",
            "SELECT AVG(unit_price) FROM agg_bench_sales"
        ),
        (
            "MIN/MAX",
            "SELECT MIN(unit_price), MAX(unit_price) FROM agg_bench_sales"
        ),
        (
            "Multiple Aggregates",
            "SELECT COUNT(*), SUM(quantity), AVG(unit_price), MIN(unit_price), MAX(unit_price) FROM agg_bench_sales"
        ),
    ];

    for (name, query) in aggregate_queries.iter() {
        let mut group = criterion.benchmark_group(format!("Simple Aggregate - {}", name));

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

            // Setup test data for SQLite
            sqlite_conn
                .execute("DROP TABLE IF EXISTS agg_bench_sales", [])
                .unwrap();
            sqlite_conn
                .execute(
                    "CREATE TABLE agg_bench_sales (
                    id INTEGER PRIMARY KEY,
                    product_id INTEGER,
                    category_id INTEGER,
                    date TEXT,
                    quantity INTEGER,
                    unit_price REAL,
                    customer_id INTEGER
                )",
                    [],
                )
                .unwrap();

            let mut stmt = sqlite_conn.prepare(
                "INSERT INTO agg_bench_sales (id, product_id, category_id, date, quantity, unit_price, customer_id) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            ).unwrap();

            for i in 1..=10000 {
                let product_id = i % 100 + 1;
                let category_id = product_id % 10 + 1;
                let year = 2020 + (i % 4);
                let month = (i % 12) + 1;
                let day = (i % 28) + 1;
                let date = format!("{}-{:02}-{:02}", year, month, day);
                let quantity = (i % 10) + 1;
                let unit_price = 10.0 + ((i % 50) as f64);
                let customer_id = (i % 500) + 1;

                stmt.execute((
                    &i,
                    &product_id,
                    &category_id,
                    &date,
                    &quantity,
                    &unit_price,
                    &customer_id,
                ))
                .unwrap();
            }

            sqlite_conn
                .execute(
                    "CREATE INDEX idx_sales_category ON agg_bench_sales(category_id)",
                    [],
                )
                .unwrap();
            sqlite_conn
                .execute("CREATE INDEX idx_sales_date ON agg_bench_sales(date)", [])
                .unwrap();

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

fn bench_group_by_aggregates(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define the GROUP BY queries to benchmark
    let group_by_queries = [
        (
            "Simple GROUP BY",
            "SELECT category_id, COUNT(*) 
             FROM agg_bench_sales 
             GROUP BY category_id",
        ),
        (
            "GROUP BY with multiple aggregates",
            "SELECT category_id, COUNT(*), SUM(quantity), AVG(unit_price) 
             FROM agg_bench_sales 
             GROUP BY category_id",
        ),
        (
            "GROUP BY with HAVING",
            "SELECT category_id, COUNT(*) as count 
             FROM agg_bench_sales 
             GROUP BY category_id 
             HAVING count > 800",
        ),
        (
            "GROUP BY with ORDER BY",
            "SELECT category_id, COUNT(*) as count 
             FROM agg_bench_sales 
             GROUP BY category_id 
             ORDER BY count DESC",
        ),
        (
            "GROUP BY date extract",
            "SELECT substr(date, 1, 4) as year, COUNT(*) 
             FROM agg_bench_sales 
             GROUP BY year",
        ),
    ];

    for (name, query) in group_by_queries.iter() {
        let mut group = criterion.benchmark_group(format!("GROUP BY - {}", name));

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

fn bench_complex_queries(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define complex queries that combine multiple features
    let complex_queries = [
        (
            "Filtered Aggregate with Subquery",
            "SELECT category_id, AVG(unit_price) as avg_price 
             FROM agg_bench_sales 
             WHERE product_id IN (
                 SELECT product_id 
                 FROM agg_bench_sales 
                 GROUP BY product_id 
                 HAVING SUM(quantity) > 100
             ) 
             GROUP BY category_id 
             ORDER BY avg_price DESC",
        ),
        (
            "Date Range with Window Function Simulation",
            "SELECT date, 
                   SUM(quantity * unit_price) as daily_revenue,
                   (SELECT SUM(quantity * unit_price) 
                    FROM agg_bench_sales s2 
                    WHERE s2.date <= s1.date AND 
                          s2.date >= date(s1.date, '-7 days')) as rolling_week_revenue
             FROM agg_bench_sales s1
             WHERE date BETWEEN '2023-01-01' AND '2023-12-31'
             GROUP BY date
             ORDER BY date",
        ),
        (
            "Customer Segmentation",
            "SELECT 
                 CASE 
                     WHEN total_spent >= 5000 THEN 'High Value'
                     WHEN total_spent >= 1000 THEN 'Medium Value'
                     ELSE 'Low Value'
                 END as customer_segment,
                 COUNT(*) as customer_count,
                 AVG(total_spent) as avg_spent
             FROM (
                 SELECT customer_id, SUM(quantity * unit_price) as total_spent
                 FROM agg_bench_sales
                 GROUP BY customer_id
             )
             GROUP BY customer_segment
             ORDER BY avg_spent DESC",
        ),
        (
            "Yearly Category Performance",
            "SELECT
                 substr(date, 1, 4) as year,
                 category_id,
                 COUNT(*) as num_sales,
                 SUM(quantity) as total_quantity,
                 SUM(quantity * unit_price) as total_revenue,
                 AVG(unit_price) as avg_unit_price
             FROM agg_bench_sales
             GROUP BY year, category_id
             ORDER BY year, total_revenue DESC",
        ),
    ];

    for (name, query) in complex_queries.iter() {
        let mut group = criterion.benchmark_group(format!("Complex Query - {}", name));

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

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_simple_aggregates, bench_group_by_aggregates, bench_complex_queries
}
criterion_main!(benches);
