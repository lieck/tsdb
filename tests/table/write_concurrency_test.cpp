#include <gtest/gtest.h>
#include <thread>
#include "db/db_options.h"
#include "test_util.h"
#include "table_operator.h"
#include "common/logger.h"



namespace LindormContest {

void RunTest(int write_thread_count, int read_thread_count, int write_count, bool range_query) {
    LOG_INFO("memtable write buffer size = %d", K_MEM_TABLE_SIZE_THRESHOLD);
    auto options = NewDBOptions();
    TestTableOperator test("test", TestSchemaType::Basic);
    auto table = test.GenerateTable(options);

    std::vector<std::thread> write_threads;
    for(int i = 0; i < write_thread_count; i++) {
        write_threads.emplace_back([&]() {
            for(int i = 0; i < write_count; i++) {
                int64_t start = rand() % 1000;
                int64_t timestamp = rand() % 1000;
                auto wr = test.GenerateWriteRequest(start, 20, timestamp);
                ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";
            }
        });
    }

    for(auto& t : write_threads) {
        t.join();
    }

    std::vector<std::thread> read_threads;
    for(int i = 0; i < read_thread_count; i++) {
        read_threads.emplace_back([&]() {
            for(int i = 0; i < write_count; i++) {
                int64_t start = rand() % 1000;
//                if(range_query && (i % 3) == 0) {
                    int64_t time_range = rand() % 1000;
                    auto qr = test.GenerateTimeRangeQueryRequest(start, time_range, time_range + 100);
                    std::vector<Row> results;
                    ASSERT_EQ(table->ExecuteTimeRangeQuery(qr, results), 0) << "ExecuteRangeQuery failed";
                    test.CheckRangeQuery(results, qr, false);
//                } else {
//                    auto qr = test.GenerateLatestQueryRequest(start, 100);
//                    std::vector<Row> results;
//                    ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
//                    test.CheckLastQuery(results, qr, false);
//                }
            }
        });
    }

    for(auto& t : read_threads) {
        t.join();
    }

    options->bg_task_->WaitForEmptyQueue();
}


TEST(WriteConcurrencTest, SingleWrite) {
    RunTest(1, 1, 50, false);
}

TEST(WriteConcurrencTest, TwoWrite) {
    RunTest(2, 1, 50, false);
}

TEST(WriteConcurrencTest, MultipleWrite) {
    RunTest(4, 1, 50, false);
}

TEST(WriteConcurrencTest, Normal) {
    RunTest(2, 2, 50, true);
}

TEST(WriteConcurrencTest, NormalPlus) {
    RunTest(4, 4, 50, true);
}

TEST(WriteConcurrencTest, NormalPlusAdd) {
    RunTest(6, 6, 50, true);
}

} // namespace LindormContest