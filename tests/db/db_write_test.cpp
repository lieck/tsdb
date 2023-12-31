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

    TEST(DBWriterTest, WriteTest) {

        auto engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);

        TestTableOperator test("test", TestSchemaType::Basic);
        auto schema = GenerateSchema(TestSchemaType::Basic);
        ASSERT_EQ(engine->createTable("test", schema), 0);

        for(int i = 0; i < 10000; i++) {
            int key = rand() % 10000;
            int time = rand() % 10000;
            auto wr = test.GenerateWriteRequest(key, 10, time);
            wr.tableName = "test";
            ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
        }

        for(int i = 0; i < 100000; i++) {
            int key = rand() % 10000;
            auto qr = test.GenerateLatestQueryRequest(key, 10);
            qr.tableName = "test";
            std::vector<Row> results;
            ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckLastQuery(results, qr, false);
        }

        for(int i = 0; i < 100000; i++) {
            int key = rand() % 10000;
            int time = rand() % 10000;
            auto qr = test.GenerateTimeRangeQueryRequest(key, time, time + 1000);
            qr.tableName = "test";
            std::vector<Row> results;
            ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckRangeQuery(results, qr, false);
        }

        engine->shutdown();
        delete engine;
    }

    TEST(DBWriterTest, DBWriterTest_ShutdownTest_Test) {

        auto engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);

        TestTableOperator test("test", TestSchemaType::Basic);
        auto schema = GenerateSchema(TestSchemaType::Basic);
        ASSERT_EQ(engine->createTable("test", schema), 0);

        for(int i = 0; i < 10000; i++) {
            int key = rand() % 10000;
            int time = rand() % 10000;
            auto wr = test.GenerateWriteRequest(key, 10, time);
            wr.tableName = "test";
            ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
        }

        engine->shutdown();
        delete engine;
        engine = new TSDBEngineImpl("./db");
        engine->connect();

        for(int i = 0; i < 100000; i++) {
            int key = rand() % 10000;
            auto qr = test.GenerateLatestQueryRequest(key, 10);
            qr.tableName = "test";
            std::vector<Row> results;
            ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckLastQuery(results, qr, false);
        }

        for(int i = 0; i < 100000; i++) {
            int key = rand() % 10000;
            int time = rand() % 10000;
            auto qr = test.GenerateTimeRangeQueryRequest(key, time, time + 1000);
            qr.tableName = "test";
            std::vector<Row> results;
            ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckRangeQuery(results, qr, false);
        }

        engine->shutdown();
        delete engine;
    }

} // namespace LindormContest