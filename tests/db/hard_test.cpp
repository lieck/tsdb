#include <gtest/gtest.h>

#include "TSDBEngineImpl.h"
#include "test_util.h"
#include "table_operator.h"

namespace LindormContest {

    TEST(HardTest, RangeQuery) {
        TSDBEngine *engine = CreateTestTSDBEngine();
        ASSERT_EQ(engine->connect(), 0);
        engine->createTable("test", GenerateSchema(TestSchemaType::Complex));

        TestTableOperator test("test", TestSchemaType::Complex);
        for(int i = 0; i < 3000000; i++) {
            int key = rand() % 10000;
            int timestamp = 1000000 + rand() % 10000;
            auto wr = test.GenerateWriteRequest(key, 1, timestamp);
            wr.tableName = "test";
            ASSERT_EQ(engine->upsert(wr), 0) << "Upsert failed";
        }

        for(int i = 0; i < 1000; i++) {
            int key = rand() % 10000;
            int timestamp = 1000000 + rand() % 10000;
            auto qr = test.GenerateTimeRangeQueryRequest(key, timestamp, timestamp + 3000);
            qr.tableName = "test";
            std::vector<Row> results;
            ASSERT_EQ(engine->executeTimeRangeQuery(qr, results), 0) << "ExecuteTimeRangeQuery failed";
            test.CheckRangeQuery(results, qr, true);
        }

        engine->shutdown();
        delete engine;

    }



} // namespace LindormContest