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

        {
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

            table->Shutdown();
            options->bg_task_->WaitForEmptyQueue();

            std::ofstream manifest_file("manifest");
            ASSERT_EQ(manifest_file.is_open(), true) << "manifest file not found";
            table->WriteMetaData(manifest_file);
            manifest_file.flush();
            manifest_file.close();
        }

        {
            std::ifstream manifest_file("manifest");
            ASSERT_EQ(manifest_file.is_open(), true) << "manifest file not found";

            auto table = test.GenerateTable(options);
            table->ReadMetaData(manifest_file);

            std::vector<std::thread> read_threads;
            for(int i = 0; i < read_thread_count; i++) {
                read_threads.emplace_back([&]() {
                    for(int i = 0; i < write_count; i++) {
                        int64_t start = rand() % 1000;
                        if(range_query && (i % 3) == 0) {
                            int64_t time_range = rand() % 1000;
                            auto qr = test.GenerateTimeRangeQueryRequest(start, time_range, time_range + 100);
                            std::vector<Row> results;
                            ASSERT_EQ(table->ExecuteTimeRangeQuery(qr, results), 0) << "ExecuteRangeQuery failed";
                            test.CheckRangeQuery(results, qr, false);
                        } else {
                            auto qr = test.GenerateLatestQueryRequest(start, 100);
                            std::vector<Row> results;
                            ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
                            test.CheckLastQuery(results, qr, false);
                        }
                    }
                });
            }

            for(auto& t : read_threads) {
                t.join();
            }

            options->bg_task_->WaitForEmptyQueue();
        }
    }



    TEST(ManifestTest, BasicAssertions) {
        auto options = NewDBOptions();

        TestTableOperator test("test", TestSchemaType::Basic);

        {
            auto table = test.GenerateTable(options);

            auto wr = test.GenerateWriteRequest(0, 1000, 12);
            ASSERT_EQ(table->Upsert(wr), 0) << "Upsert failed";

            auto qr = test.GenerateLatestQueryRequest(0, 1000);
            std::vector<Row> results;
            ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckLastQuery(results, qr, false);

            table->Shutdown();
            options->bg_task_->WaitForEmptyQueue();

            std::ofstream manifest_file("manifest");
            ASSERT_EQ(manifest_file.is_open(), true) << "manifest file not found";
            table->WriteMetaData(manifest_file);
            manifest_file.flush();
            manifest_file.close();

            delete table;
        }

        {
            std::ifstream manifest_file("manifest");
            ASSERT_EQ(manifest_file.is_open(), true) << "manifest file not found";

            auto table = test.GenerateTable(options);
            table->ReadMetaData(manifest_file);

            auto qr = test.GenerateLatestQueryRequest(0, 1000);
            std::vector<Row> results;
            ASSERT_EQ(table->ExecuteLatestQuery(qr, results), 0) << "ExecuteLatestQuery failed";
            test.CheckLastQuery(results, qr, false);
        }
    }

    TEST(ManifestTest, Normal) {
        RunTest(2, 4, 50, true);
    }

    TEST(ManifestTest, MultiThread) {
        RunTest(4, 4, 100, true);
    }

} // namespace LindormContest