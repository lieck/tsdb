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


}