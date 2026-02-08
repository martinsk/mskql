#include <benchmark/benchmark.h>

extern "C" {
#include "database.h"
#include "query.h"
#include "parser.h"
}

#include <cstring>

// Benchmark: Database initialization
static void BM_DatabaseInit(benchmark::State& state) {
    for (auto _ : state) {
        struct database db;
        db_init(&db, "test_db");
        benchmark::DoNotOptimize(&db);
        db_free(&db);
    }
}
BENCHMARK(BM_DatabaseInit);

// Benchmark: Create table
static void BM_CreateTable(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        struct database db;
        db_init(&db, "test_db");
        state.ResumeTiming();
        
        const char* sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        struct rows result;
        db_exec_sql(&db, sql, &result);
        
        benchmark::DoNotOptimize(&db);
        benchmark::ClobberMemory();
        
        state.PauseTiming();
        db_free(&db);
        state.ResumeTiming();
    }
}
BENCHMARK(BM_CreateTable);

// Benchmark: Insert single row
static void BM_InsertSingleRow(benchmark::State& state) {
    struct database db;
    db_init(&db, "test_db");
    
    // Setup: create table
    const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
    struct rows create_result;
    db_exec_sql(&db, create_sql, &create_result);
    
    for (auto _ : state) {
        const char* insert_sql = "INSERT INTO users VALUES (1, 'Alice', 30)";
        struct rows result;
        db_exec_sql(&db, insert_sql, &result);
        benchmark::DoNotOptimize(&result);
        benchmark::ClobberMemory();
    }
    
    db_free(&db);
}
BENCHMARK(BM_InsertSingleRow);

// Benchmark: Insert multiple rows
static void BM_InsertMultipleRows(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        struct database db;
        db_init(&db, "test_db");
        
        const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        struct rows create_result;
        db_exec_sql(&db, create_sql, &create_result);
        state.ResumeTiming();
        
        for (int i = 0; i < state.range(0); ++i) {
            char insert_sql[256];
            snprintf(insert_sql, sizeof(insert_sql), 
                    "INSERT INTO users VALUES (%d, 'User%d', %d)", 
                    i, i, 20 + (i % 50));
            struct rows result;
            db_exec_sql(&db, insert_sql, &result);
            benchmark::DoNotOptimize(&result);
        }
        
        benchmark::ClobberMemory();
        
        state.PauseTiming();
        db_free(&db);
        state.ResumeTiming();
    }
}
BENCHMARK(BM_InsertMultipleRows)->Range(10, 1000);

// Benchmark: Select all rows
static void BM_SelectAll(benchmark::State& state) {
    // Setup database with data
    struct database db;
    db_init(&db, "test_db");
    
    const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
    struct rows create_result;
    db_exec_sql(&db, create_sql, &create_result);
    
    // Insert test data
    for (int i = 0; i < state.range(0); ++i) {
        char insert_sql[256];
        snprintf(insert_sql, sizeof(insert_sql), 
                "INSERT INTO users VALUES (%d, 'User%d', %d)", 
                i, i, 20 + (i % 50));
        struct rows result;
        db_exec_sql(&db, insert_sql, &result);
    }
    
    for (auto _ : state) {
        const char* select_sql = "SELECT * FROM users";
        struct rows result;
        db_exec_sql(&db, select_sql, &result);
        benchmark::DoNotOptimize(&result);
        benchmark::ClobberMemory();
    }
    
    db_free(&db);
}
BENCHMARK(BM_SelectAll)->Range(10, 1000);

// Benchmark: Select with WHERE clause
static void BM_SelectWithWhere(benchmark::State& state) {
    // Setup database with data
    struct database db;
    db_init(&db, "test_db");
    
    const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
    struct rows create_result;
    db_exec_sql(&db, create_sql, &create_result);
    
    // Insert test data
    for (int i = 0; i < 1000; ++i) {
        char insert_sql[256];
        snprintf(insert_sql, sizeof(insert_sql), 
                "INSERT INTO users VALUES (%d, 'User%d', %d)", 
                i, i, 20 + (i % 50));
        struct rows result;
        db_exec_sql(&db, insert_sql, &result);
    }
    
    for (auto _ : state) {
        const char* select_sql = "SELECT * FROM users WHERE age > 30";
        struct rows result;
        db_exec_sql(&db, select_sql, &result);
        benchmark::DoNotOptimize(&result);
        benchmark::ClobberMemory();
    }
    
    db_free(&db);
}
BENCHMARK(BM_SelectWithWhere);

// Benchmark: Update rows
static void BM_UpdateRows(benchmark::State& state) {
    // Setup database with data
    struct database db;
    db_init(&db, "test_db");
    
    const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
    struct rows create_result;
    db_exec_sql(&db, create_sql, &create_result);
    
    // Insert test data
    for (int i = 0; i < 100; ++i) {
        char insert_sql[256];
        snprintf(insert_sql, sizeof(insert_sql), 
                "INSERT INTO users VALUES (%d, 'User%d', %d)", 
                i, i, 20 + (i % 50));
        struct rows result;
        db_exec_sql(&db, insert_sql, &result);
    }
    
    for (auto _ : state) {
        const char* update_sql = "UPDATE users SET age = 35 WHERE age > 30";
        struct rows result;
        db_exec_sql(&db, update_sql, &result);
        benchmark::DoNotOptimize(&result);
        benchmark::ClobberMemory();
    }
    
    db_free(&db);
}
BENCHMARK(BM_UpdateRows);

// Benchmark: Delete rows
static void BM_DeleteRows(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        struct database db;
        db_init(&db, "test_db");
        
        const char* create_sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        struct rows create_result;
        db_exec_sql(&db, create_sql, &create_result);
        
        // Insert test data
        for (int i = 0; i < 100; ++i) {
            char insert_sql[256];
            snprintf(insert_sql, sizeof(insert_sql), 
                    "INSERT INTO users VALUES (%d, 'User%d', %d)", 
                    i, i, 20 + (i % 50));
            struct rows result;
            db_exec_sql(&db, insert_sql, &result);
        }
        state.ResumeTiming();
        
        const char* delete_sql = "DELETE FROM users WHERE age > 40";
        struct rows result;
        db_exec_sql(&db, delete_sql, &result);
        benchmark::DoNotOptimize(&result);
        benchmark::ClobberMemory();
        
        state.PauseTiming();
        db_free(&db);
        state.ResumeTiming();
    }
}
BENCHMARK(BM_DeleteRows);

// Benchmark: SQL parsing
static void BM_ParseSimpleSelect(benchmark::State& state) {
    for (auto _ : state) {
        const char* sql = "SELECT id, name FROM users WHERE age > 25";
        struct query q;
        query_parse(sql, &q);
        benchmark::DoNotOptimize(&q);
        benchmark::ClobberMemory();
        query_free(&q);
    }
}
BENCHMARK(BM_ParseSimpleSelect);

// Benchmark: Complex query with JOIN
static void BM_ParseComplexQuery(benchmark::State& state) {
    for (auto _ : state) {
        const char* sql = "SELECT u.id, u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 25 ORDER BY o.amount DESC LIMIT 10";
        struct query q;
        query_parse(sql, &q);
        benchmark::DoNotOptimize(&q);
        benchmark::ClobberMemory();
        query_free(&q);
    }
}
BENCHMARK(BM_ParseComplexQuery);
