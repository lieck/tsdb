#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"
#include "test_util.h"
#include "table_operator.h"

namespace LindormContest {

    TEST(HardTest, HardWrite) {
        TSDBEngine *engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);
        engine->createTable("test", GenerateSchema(TestSchemaType::Basic));

        TestTableOperator test("test", TestSchemaType::Basic);

        for(int i = 0; i < 10000; i++) {
            auto wr = test.GenerateWriteRequest(0, 1000, i);
            wr.tableName = "test";
            ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
        }

        engine->shutdown();
    }

    TEST(HardTest, HardTest) {
        TSDBEngine *engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);
        engine->createTable("test", GenerateSchema(TestSchemaType::Basic));

        TestTableOperator test("test", TestSchemaType::Basic);

        for(int i = 0; i < 1000; i++) {
            int64_t key = rand() % 1000;
            int64_t timestamp = rand() % 1000;
            int64_t limit = rand() % 100 + 100;

            if((i % 3) == 0) {
                auto wr = test.GenerateWriteRequest(key, limit, timestamp);
                wr.tableName = "test";
                ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
            } else if(i & 1) {
                auto qr = test.GenerateLatestQueryRequest(key, limit);
                qr.tableName = "test";
                std::vector<Row> results;
                ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, false);
            } else {
                auto qr = test.GenerateTimeRangeQueryRequest(key, timestamp, timestamp + 300);
                qr.tableName = "test";
                std::vector<Row> results;
                ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteTimeRangeQuery failed";
                test.CheckRangeQuery(results, qr, false);
            }
        }

        engine->shutdown();
    }

    TEST(HardTest, HardTest2) {
        TSDBEngine *engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);
        engine->createTable("test", GenerateSchema(TestSchemaType::Basic));

        TestTableOperator test("test", TestSchemaType::Basic);

        for(int i = 0; i < 10000; i++) {
            int64_t key = rand() % 1000;
            int64_t timestamp = rand() % 1000;
            int64_t limit = rand() % 100 + 100;

            if((i % 10) == 0) {
                auto qr = test.GenerateLatestQueryRequest(key, limit);
                qr.tableName = "test";
                std::vector<Row> results;
                ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                test.CheckLastQuery(results, qr, false);
            } else {
                auto wr = test.GenerateWriteRequest(key, limit, timestamp);
                wr.tableName = "test";
                ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
            }
        }

        engine->shutdown();
    }
} // namespace LindormContest