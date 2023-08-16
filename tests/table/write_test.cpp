#include <gtest/gtest.h>
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



}  // namespace LindormContest