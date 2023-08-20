#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"
#include "test_util.h"
#include "table_operator.h"

namespace LindormContest {


    TEST(DBWriterTest, BasicAssertions) {

        auto engine = CreateTestTSDBEngine();
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
        delete engine;
    }

    TEST(DBWriterTest, ShutdownTest) {
        TestTableOperator test("test", TestSchemaType::Complex);
        TSDBEngine *engine = CreateTestTSDBEngine();

        for(int c = 0; c < 2; c++) {
            if(engine == nullptr) {
                engine = new TSDBEngineImpl("./db");
            }

            ASSERT_EQ(engine->connect(), 0);
            engine->createTable("test", GenerateSchema(TestSchemaType::Complex));

            for(int i = 1; i <= 100; i++) {
                int start_key = rand() % 100;

                auto wr = test.GenerateWriteRequest(start_key, 100, i);
                wr.tableName = "test";
                ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";

                if(i % 10 == 0) {
                    auto qr = test.GenerateLatestQueryRequest(start_key, 100);
                    qr.tableName = "test";
                    std::vector<Row> results;
                    ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                    test.CheckLastQuery(results, qr, true);
                } else if(i % 10 == 1) {
                    auto qr = test.GenerateTimeRangeQueryRequest(start_key, 0, 200);
                    qr.tableName = "test";
                    std::vector<Row> results;
                    ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteRangeQuery failed";
                    test.CheckRangeQuery(results, qr, true);
                }
            }

            ASSERT_EQ(engine->shutdown(), 0);
            delete engine;
            engine = nullptr;
        }

    }

} // namespace LindormContest