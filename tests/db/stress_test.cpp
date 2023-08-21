

#include <gtest/gtest.h>
#include "db_test.h"

namespace LindormContest {

    TEST(StressTest, WriteTest) {
        TestSchema test_schema(TestSchemaType::Basic);
        DBTest db_test(CheckDBType::Memory);
        db_test.Init(TestSchemaType::Basic);

        for(int j = 0; j < 100; j++) {
            auto wr = test_schema.GenerateUpsetRequest();
            db_test.Upsert(wr);
        }

        db_test.Restart();

        for(int j = 0; j < 100; j++) {
            auto wr = test_schema.GenerateLatestQueryRequest(100);
            db_test.ExecuteLatestQuery(wr);
        }

        for(int j = 0; j < 100; j++) {
            auto wr = test_schema.GenerateRangeQueryRequest();
            db_test.ExecuteTimeRangeQuery(wr);
        }

        db_test.EndTest();
    }


} // namespace LindormContest