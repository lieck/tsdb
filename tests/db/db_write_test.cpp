#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"
#include "test_util.h"
#include "table_operator.h"

namespace LindormContest {


    TEST(DBWriterTest, BasicAssertions) {

        TSDBEngine *engine = new TSDBEngineImpl("./");
        ASSERT_EQ(engine->connect(), 0);

        TestTableOperator test("test", TestSchemaType::Basic);

        auto schema = GenerateSchema(TestSchemaType::Basic);
        ASSERT_EQ(engine->createTable("test", schema), 0);

        auto wr = test.GenerateWriteRequest(0, 1000, 12);
        wr.tableName = "test";
        ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";

        auto qr = test.GenerateLatestQueryRequest(0, 1000);
        qr.tableName = "test";
        std::vector<Row> results;
        ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
        test.CheckLastQuery(results, qr, false);

        engine->shutdown();
    }

    TEST(DBWriterTest, HardWrite) {
        TSDBEngine *engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);
        engine->createTable("test", GenerateSchema(TestSchemaType::Basic));

        TestTableOperator test("test", TestSchemaType::Basic);

        for(int i = 0; i < 5000; i++) {
            auto wr = test.GenerateWriteRequest(0, 1000, i);
            wr.tableName = "test";
            ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
        }

        engine->shutdown();
    }

    TEST(DBWriterTest, HardTest) {
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

} // namespace LindormContest