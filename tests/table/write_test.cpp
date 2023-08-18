#include <gtest/gtest.h>
#include <thread>
#include "db/db_options.h"
#include "test_util.h"
#include "table_operator.h"
#include "common/logger.h"


namespace LindormContest {

TEST(WriteTest, MemTaleCompress) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    auto wr = test.GenerateWriteRequest(0, 1000, 12);
    ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

    sleep(5);
    auto qr = test.GenerateLatestQueryRequest(0, 100);
    std::vector<Row> results;
    ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
    test.CheckQuery(results, 0, 100);
    options->bg_task_->WaitForEmptyQueue();
}

TEST(WriteTest, L0Compress) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    auto wr = test.GenerateWriteRequest(0, 2000, 12);

    ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

    auto qr = test.GenerateLatestQueryRequest(0, 100);
    std::vector<Row> results;
    ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
    test.CheckQuery(results, 0, 100);

    options->bg_task_->WaitForEmptyQueue();
}

TEST(WriteTest, NotWrittenInOrder) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    for(int i = 0; i < 5; i++) {
        auto wr1 = test.GenerateWriteRequest(70, 140, i + 10);
        ASSERT_EQ(table->Upsert(wr1), 0) << "Upsert failed";

        auto wr2 = test.GenerateWriteRequest(130, 190, i + 3);
        ASSERT_EQ(table->Upsert(wr2), 0) << "Upsert failed";

        auto wr3 = test.GenerateWriteRequest(200, 190, i + 7);
        ASSERT_EQ(table->Upsert(wr3), 0) << "Upsert failed";

        auto wr4 = test.GenerateWriteRequest(290, 190, i);
        ASSERT_EQ(table->Upsert(wr4), 0) << "Upsert failed";
    }

    for(int i = 0; i < 10; i++) {
        auto qr = test.GenerateLatestQueryRequest(100, 300);
        std::vector<Row> results;
        ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
        test.CheckQuery(results, 100, 300);
        sleep(1);
    }

    options->bg_task_->WaitForEmptyQueue();

    for(int i = 0; i < 10; i++) {
        auto qr = test.GenerateLatestQueryRequest(100, 300);
        std::vector<Row> results;
        ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
        test.CheckQuery(results, 100, 300);
    }
}

TEST(WriteTest, BasicsRangeQuery) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    for(int i = 0; i <= 100; i++) {
        auto wr = test.GenerateWriteRequest(0, 1, i);
        ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";
    }

    for(int i = 0; i < 100; i++) {
        auto qr = test.GenerateTimeRangeQueryRequest(0, 0, 100);
        std::vector<Row> results;
        ASSERT_EQ(table->ExecuteTimeRangeQuery(qr, results), 0) << "ExecuteLatestQuery failed";
        test.CheckRangeQuery(results, 0, 0, 100);
    }

    options->bg_task_->WaitForEmptyQueue();
}

TEST(WriteTest, RangeQuery) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    for(int i = 0; i <= 100; i++) {
        auto wr = test.GenerateWriteRequest(0, 10, i);
        ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";
    }

    options->bg_task_->WaitForEmptyQueue();

    auto qr = test.GenerateTimeRangeQueryRequest(0, 0, 100);
    std::vector<Row> results;
    ASSERT_EQ(table->ExecuteTimeRangeQuery(qr, results), 0) << "ExecuteLatestQuery failed";
    test.CheckRangeQuery(results, 0, 0, 100);

    options->bg_task_->WaitForEmptyQueue();
}

TEST(WriteTest, RangeQuery2) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);

    auto options = NewDBOptions();

    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    for(int i = 0; i <= 100; i++) {
        auto start = rand() % 1000;
        auto wr = test.GenerateWriteRequest(0, 10, start);
        ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";
    }

    options->bg_task_->WaitForEmptyQueue();

    auto qr = test.GenerateTimeRangeQueryRequest(0, 200, 400);
    std::vector<Row> results;
    ASSERT_EQ(table->ExecuteTimeRangeQuery(qr, results), 0) << "ExecuteLatestQuery failed";
    test.CheckRangeQuery(results, qr, false);

    options->bg_task_->WaitForEmptyQueue();
}


}  // namespace LindormContest