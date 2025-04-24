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
    // Create tables
    let queries = [
        "DROP TABLE IF EXISTS text_bench_articles",
        "CREATE TABLE text_bench_articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            content TEXT,
            tags TEXT,
            author TEXT,
            created_at TEXT
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

    // Insert some test articles with varying content
    let mut stmt = limbo_conn
        .prepare(
            "INSERT INTO text_bench_articles (id, title, content, tags, author, created_at) 
         VALUES (?, ?, ?, ?, ?, ?)",
        )
        .unwrap();

    // Generate some article data
    let titles = [
        "Introduction to Rust Programming",
        "Advanced SQL Techniques",
        "The Future of Database Technology",
        "Building High-Performance Applications",
        "Understanding Concurrency Models",
        "Modern Web Development",
        "Machine Learning Basics",
        "Data Analysis with SQL",
        "Functional Programming Patterns",
        "Optimizing Database Performance",
        "Cloud Computing Architecture",
        "Microservices and Distributed Systems",
        "Security Best Practices",
        "Mobile Development Frameworks",
        "Open Source Software Development",
        "Test-Driven Development",
        "User Experience Design",
        "Algorithmic Thinking",
        "Network Programming",
        "Real-time Data Processing",
    ];

    let authors = [
        "John Smith",
        "Emma Johnson",
        "Michael Davis",
        "Sarah Wilson",
        "David Thompson",
    ];

    let tag_sets = [
        "rust,programming,systems",
        "sql,database,techniques",
        "database,future,technology",
        "performance,optimization,applications",
        "concurrency,programming,threads",
        "web,javascript,development",
        "machine-learning,ai,data-science",
        "sql,data,analysis",
        "functional,programming,patterns",
        "database,performance,optimization",
        "cloud,architecture,infrastructure",
        "microservices,distributed,systems",
        "security,best-practices,privacy",
        "mobile,ios,android",
        "open-source,collaboration,github",
        "tdd,testing,quality",
        "ux,design,user-interface",
        "algorithms,data-structures,problem-solving",
        "networking,protocols,communication",
        "real-time,streaming,processing",
    ];

    let lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. ";

    // Generate 1000 articles with varied content
    for i in 1..=1000 {
        let title_index = i % titles.len();
        let author_index = i % authors.len();
        let tag_index = i % tag_sets.len();

        // Generate a longer content string by repeating the lorem ipsum text
        let repetitions = (i % 10) + 1;
        let content = lorem_ipsum.repeat(repetitions)
            + &format!(
                " Article number {}. Keywords: rust database sql performance concurrency.",
                i
            );

        let year = 2020 + (i % 4);
        let month = (i % 12) + 1;
        let day = (i % 28) + 1;
        let created_at = format!("{}-{:02}-{:02}", year, month, day);

        stmt.bind_at(
            1.try_into().unwrap(),
            OwnedValue::Integer(i.try_into().unwrap()),
        );
        stmt.bind_at(
            2.try_into().unwrap(),
            OwnedValue::Text(format!("{} {}", titles[title_index], i).into()),
        );
        stmt.bind_at(3.try_into().unwrap(), OwnedValue::Text(content.into()));
        stmt.bind_at(
            4.try_into().unwrap(),
            OwnedValue::Text(tag_sets[tag_index].to_string().into()),
        );
        stmt.bind_at(
            5.try_into().unwrap(),
            OwnedValue::Text(authors[author_index].to_string().into()),
        );
        stmt.bind_at(6.try_into().unwrap(), OwnedValue::Text(created_at.into()));

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

    // Create index to help with text searches
    let mut stmt = limbo_conn
        .prepare("CREATE INDEX idx_articles_title ON text_bench_articles(title)")
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
        .prepare("CREATE INDEX idx_articles_author ON text_bench_articles(author)")
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

fn bench_like_pattern_matching(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Setup test data
    setup_test_data(limbo_conn.clone(), &io);

    // Define the LIKE pattern queries to benchmark
    let pattern_queries = [
        (
            "Simple Prefix Pattern",
            "SELECT * FROM text_bench_articles WHERE title LIKE 'Introduction%'"
        ),
        (
            "Middle Wildcard Pattern",
            "SELECT * FROM text_bench_articles WHERE title LIKE '%SQL%'"
        ),
        (
            "Suffix Pattern",
            "SELECT * FROM text_bench_articles WHERE title LIKE '%Applications'"
        ),
        (
            "Complex Pattern",
            "SELECT * FROM text_bench_articles WHERE title LIKE '%Data%' AND content LIKE '%performance%'"
        ),
        (
            "Single Character Wildcard",
            "SELECT * FROM text_bench_articles WHERE author LIKE 'John S_ith'"
        ),
    ];

    for (name, query) in pattern_queries.iter() {
        let mut group = criterion.benchmark_group(format!("LIKE Pattern - {}", name));

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
                .execute("DROP TABLE IF EXISTS text_bench_articles", [])
                .unwrap();
            sqlite_conn
                .execute(
                    "CREATE TABLE text_bench_articles (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    content TEXT,
                    tags TEXT,
                    author TEXT,
                    created_at TEXT
                )",
                    [],
                )
                .unwrap();

            let mut stmt = sqlite_conn.prepare(
                "INSERT INTO text_bench_articles (id, title, content, tags, author, created_at) 
                 VALUES (?, ?, ?, ?, ?, ?)"
            ).unwrap();

            // Same data generation as before
            let titles = [
                "Introduction to Rust Programming",
                "Advanced SQL Techniques",
                "The Future of Database Technology",
                "Building High-Performance Applications",
                "Understanding Concurrency Models",
                "Modern Web Development",
                "Machine Learning Basics",
                "Data Analysis with SQL",
                "Functional Programming Patterns",
                "Optimizing Database Performance",
                "Cloud Computing Architecture",
                "Microservices and Distributed Systems",
                "Security Best Practices",
                "Mobile Development Frameworks",
                "Open Source Software Development",
                "Test-Driven Development",
                "User Experience Design",
                "Algorithmic Thinking",
                "Network Programming",
                "Real-time Data Processing",
            ];

            let authors = [
                "John Smith",
                "Emma Johnson",
                "Michael Davis",
                "Sarah Wilson",
                "David Thompson",
            ];

            let tag_sets = [
                "rust,programming,systems",
                "sql,database,techniques",
                "database,future,technology",
                "performance,optimization,applications",
                "concurrency,programming,threads",
                "web,javascript,development",
                "machine-learning,ai,data-science",
                "sql,data,analysis",
                "functional,programming,patterns",
                "database,performance,optimization",
                "cloud,architecture,infrastructure",
                "microservices,distributed,systems",
                "security,best-practices,privacy",
                "mobile,ios,android",
                "open-source,collaboration,github",
                "tdd,testing,quality",
                "ux,design,user-interface",
                "algorithms,data-structures,problem-solving",
                "networking,protocols,communication",
                "real-time,streaming,processing",
            ];

            let lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. ";

            for i in 1..=1000 {
                let title_index = i % titles.len();
                let author_index = i % authors.len();
                let tag_index = i % tag_sets.len();

                // Generate a longer content string by repeating the lorem ipsum text
                let repetitions = (i % 10) + 1;
                let content = lorem_ipsum.repeat(repetitions)
                    + &format!(
                        " Article number {}. Keywords: rust database sql performance concurrency.",
                        i
                    );

                let year = 2020 + (i % 4);
                let month = (i % 12) + 1;
                let day = (i % 28) + 1;
                let created_at = format!("{}-{:02}-{:02}", year, month, day);

                stmt.execute((
                    &i,
                    format!("{} {}", titles[title_index], i),
                    &content,
                    &tag_sets[tag_index],
                    &authors[author_index],
                    &created_at,
                ))
                .unwrap();
            }

            sqlite_conn
                .execute(
                    "CREATE INDEX idx_articles_title ON text_bench_articles(title)",
                    [],
                )
                .unwrap();
            sqlite_conn
                .execute(
                    "CREATE INDEX idx_articles_author ON text_bench_articles(author)",
                    [],
                )
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

fn bench_text_functions(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define queries that use various text functions
    let text_function_queries = [
        (
            "UPPER/LOWER",
            "SELECT id, UPPER(title), LOWER(author) FROM text_bench_articles LIMIT 100"
        ),
        (
            "LENGTH",
            "SELECT id, title, LENGTH(content) FROM text_bench_articles ORDER BY LENGTH(content) DESC LIMIT 100"
        ),
        (
            "SUBSTR",
            "SELECT id, SUBSTR(title, 1, 10), SUBSTR(content, 1, 50) FROM text_bench_articles LIMIT 100"
        ),
        (
            "REPLACE",
            "SELECT id, REPLACE(title, 'SQL', 'Structured Query Language') FROM text_bench_articles WHERE title LIKE '%SQL%'"
        ),
        (
            "TRIM",
            "SELECT id, TRIM(title), LTRIM(author), RTRIM(tags) FROM text_bench_articles LIMIT 100"
        ),
        (
            "Combined Text Functions",
            "SELECT id, UPPER(SUBSTR(title, 1, 10)), LENGTH(LOWER(content)) FROM text_bench_articles LIMIT 100"
        ),
    ];

    for (name, query) in text_function_queries.iter() {
        let mut group = criterion.benchmark_group(format!("Text Functions - {}", name));

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

fn bench_glob_pattern_matching(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define the GLOB pattern queries to benchmark
    let glob_queries = [
        (
            "Simple GLOB Pattern",
            "SELECT * FROM text_bench_articles WHERE title GLOB 'Introduction*'"
        ),
        (
            "Character Class GLOB",
            "SELECT * FROM text_bench_articles WHERE title GLOB '*[0-9]*'"
        ),
        (
            "Complex GLOB Pattern",
            "SELECT * FROM text_bench_articles WHERE tags GLOB '*,programming,*' OR tags GLOB 'programming,*'"
        ),
    ];

    for (name, query) in glob_queries.iter() {
        let mut group = criterion.benchmark_group(format!("GLOB Pattern - {}", name));

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

fn bench_advanced_text_operations(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    // Define more complex queries combining multiple text operations
    let complex_queries = [
        (
            "Tag Extraction",
            "SELECT id, title, 
                SUBSTR(tags, 1, INSTR(tags, ',')-1) as first_tag,
                CASE 
                    WHEN INSTR(SUBSTR(tags, INSTR(tags, ',')+1), ',') > 0 
                    THEN SUBSTR(SUBSTR(tags, INSTR(tags, ',')+1), 1, INSTR(SUBSTR(tags, INSTR(tags, ',')+1), ',')-1)
                    ELSE SUBSTR(tags, INSTR(tags, ',')+1)
                END as second_tag
             FROM text_bench_articles
             LIMIT 100"
        ),
        (
            "Text Analysis",
            "SELECT
                SUBSTR(title, 1, INSTR(title, ' ')-1) as first_word,
                LENGTH(content) as content_length,
                (LENGTH(content) - LENGTH(REPLACE(content, ' ', ''))) as word_count_estimate
             FROM text_bench_articles
             ORDER BY content_length DESC
             LIMIT 100"
        ),
        (
            "Combined Pattern Matching",
            "SELECT * FROM text_bench_articles
             WHERE 
                title LIKE '%Database%' AND
                tags LIKE '%sql%' AND
                author GLOB '[A-M]*'
             LIMIT 100"
        ),
        (
            "Text Transformation",
            "SELECT
                id,
                UPPER(SUBSTR(title, 1, 1)) || LOWER(SUBSTR(title, 2)) as title_proper_case,
                REPLACE(content, 'Lorem ipsum', 'LOREM IPSUM') as modified_content
             FROM text_bench_articles
             WHERE LENGTH(content) > 1000
             LIMIT 50"
        ),
    ];

    for (name, query) in complex_queries.iter() {
        let mut group = criterion.benchmark_group(format!("Advanced Text - {}", name));

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
    targets = bench_like_pattern_matching, bench_text_functions, bench_glob_pattern_matching, bench_advanced_text_operations
}
criterion_main!(benches);
