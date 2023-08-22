

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

    TEST(StressTest, ConcurrentWriteTest) {
        TestSchema test_schema(TestSchemaType::Long);
        DBTest db_test(CheckDBType::Memory);
        db_test.Init(TestSchemaType::Long);

        for(int j = 0; j < 6000; j++) {
            auto wr = test_schema.GenerateUpsetRequest();
            db_test.Upsert(wr);
        }

        db_test.Restart();

        {
            std::vector<std::thread> threads;
            for(int i = 0; i < 100; i++) {
                threads.push_back(std::thread([&](){
                    for(int j = 0; j < 60; j++) {
                        auto wr = test_schema.GenerateRangeQueryRequest();
                        db_test.ExecuteTimeRangeQuery(wr);
                    }
                }));
            }

            for(auto &t : threads) {
                t.join();
            }
        }

        db_test.EndTest();
    }


} // namespace LindormContest