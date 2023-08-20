#include <gtest/gtest.h>
#include <thread>
#include "db/db_options.h"
#include "test_util.h"
#include "table_operator.h"


namespace LindormContest {

    TEST(MultiColTest, BasicAssertions) {
        auto options = NewDBOptions();

        {
            TestTableOperator test("test", TestSchemaType::Complex);
            auto table = test.GenerateTable(options);

            for(int i = 1; i < 100; i++) {
                auto wr = test.GenerateWriteRequest(i, 20, i);
                ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

                auto qr = test.GenerateLatestQueryRequest(i, 100);
                std::vector<Row> results;
                ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, true);
            }
        }

        {
            TestTableOperator test("test", TestSchemaType::Long);
            auto table = test.GenerateTable(options);

            for(int i = 1; i < 100; i++) {
                auto wr = test.GenerateWriteRequest(i, 20, i + 10);
                ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

                auto qr = test.GenerateLatestQueryRequest(i, 100);
                std::vector<Row> results;
                ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, true);
            }
        }
    }

    TEST(MultiColTest, SSTable) {
        auto options = NewDBOptions();

        {
            TestTableOperator test("test", TestSchemaType::Complex);
            auto table = test.GenerateTable(options);

            for(int i = 1; i < 100; i++) {
                auto wr = test.GenerateWriteRequest(i, 20, i);
                ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

                options->bg_task_->WaitForEmptyQueue();

                auto qr = test.GenerateLatestQueryRequest(i, 100);
                std::vector<Row> results;
                ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, true);
            }
        }

        {
            TestTableOperator test("test", TestSchemaType::Long);
            auto table = test.GenerateTable(options);

            for(int i = 1; i < 100; i++) {
                auto wr = test.GenerateWriteRequest(i, 20, i + 10);
                ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

                options->bg_task_->WaitForEmptyQueue();

                auto qr = test.GenerateLatestQueryRequest(i, 100);
                std::vector<Row> results;
                ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, true);
            }
        }

    }

}  // namespace LindormContest