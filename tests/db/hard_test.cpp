#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"
#include "test_util.h"
#include "table_operator.h"

namespace LindormContest {

    TEST(HardTest, RangeQuery) {

        TestTableOperator test("test", TestSchemaType::Long);

        TSDBEngine *engine = CreateTestTSDBEngine();

        {
            ASSERT_EQ(engine->connect(), 0);
            engine->createTable("test", GenerateSchema(TestSchemaType::Long));

            for(int i = 0; i < 3000000; i++) {
                int key = rand() % 10000;
                int timestamp = 1000000 + rand() % 10000;
                auto wr = test.GenerateWriteRequest(key, 1, timestamp);
                wr.tableName = "test";
                ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
            }

            engine->shutdown();
            delete engine;
        }

        {
            engine = new TSDBEngineImpl("./db");
            ASSERT_EQ(engine->connect(), 0);

            for(int i = 0; i < 1000; i++) {
                if(i % 2 == 0) {
                    int key = rand() % 10000;
                    int timestamp = 1000000 + rand() % 10000;
                    auto qr = test.GenerateTimeRangeQueryRequest(key, timestamp, timestamp + 3000);
                    qr.tableName = "test";
                    std::vector<Row> results;
                    ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteTimeRangeQuery failed";
                    test.CheckRangeQuery(results, qr, true);
                } else {
                    int start_key = rand() % 10000;
                    int limit = rand() % 1000;
                    auto qr = test.GenerateLatestQueryRequest(start_key, limit);
                    qr.tableName = "test";
                    std::vector<Row> results;
                    ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery";
                    test.CheckLastQuery(results, qr, true);
                }
            }

            engine->shutdown();
            delete engine;
        }

    }

    TEST(HardTest, MutildRangeQuery) {

        TestTableOperator test("test", TestSchemaType::Long);

        TSDBEngine *engine = CreateTestTSDBEngine();

        {
            ASSERT_EQ(engine->connect(), 0);
            engine->createTable("test", GenerateSchema(TestSchemaType::Long));

            for(int i = 0; i < 3000000; i++) {
                int key = rand() % 100000;
                int timestamp = 1000000 + rand() % 10000;
                auto wr = test.GenerateWriteRequest(key, 1, timestamp);
                wr.tableName = "test";
                ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
            }

            engine->shutdown();
            delete engine;
        }

        {
            engine = new TSDBEngineImpl("./db");
            ASSERT_EQ(engine->connect(), 0);

            std::vector<std::thread> threads;
            for(int tc = 0; tc < 20; tc++) {
                threads.emplace_back([&](){
                    for(int i = 0; i < 500; i++) {
                        if(i % 2 == 0) {
                            int key = rand() % 100000;
                            int timestamp = 1000000 + rand() % 10000;
                            auto qr = test.GenerateTimeRangeQueryRequest(key, timestamp, timestamp + (rand() % 3000));
                            qr.tableName = "test";
                            std::vector<Row> results;
                            ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteTimeRangeQuery failed";
                            test.CheckRangeQuery(results, qr, true);
                        } else {
                            int limit = rand() % 1000;
                            auto qr = test.GenerateLatestQueryRequest(0, 0);
                            qr.tableName = "test";
                            for(int k = 0; k < limit; k++) {
                                int key = rand() % 100000;
                                qr.vins.push_back(GenerateVin(key));
                            }

                            std::vector<Row> results;
                            ASSERT_EQ(engine->executeLatestQuery(qr, results), 0) << "ExecuteLatestQuery";
                            test.CheckLastQuery(results, qr, true);
                        }
                    }
                });
            }

            for(auto &t : threads) {
                t.join();
            }

            engine->shutdown();
            delete engine;
        }

    }



} // namespace LindormContest