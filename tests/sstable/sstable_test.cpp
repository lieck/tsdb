#include <gtest/gtest.h>
#include <fstream>
#include <thread>
#include "disk/disk_manager.h"
#include "sstable/sstable_builder.h"
#include "test_util.h"

namespace LindormContest {





TEST(SSTableTest, WriteSSTable) {
    int32_t test_file_number = 13;

    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));

    SStableBuilder builder(test_file_number);

    std::map<InternalKey, Row> data;
    for(int i = 0; i < 100; i++) {
        auto key = GenerateKey(i);
        auto row = GenerateRow(i);
        data[key] = row;
    }

    for(auto &item : data) {
        auto value = CodingUtil::EncodeRow(item.second);
        builder.Add(item.first, value);
    }

    auto sstable = builder.Builder();
    auto iter = sstable->NewIterator();
    for(auto &item : data) {
        ASSERT_EQ(iter->Valid(), true);
        ASSERT_EQ(iter->GetKey(), item.first);
        ASSERT_EQ(iter->GetValue(), CodingUtil::EncodeRow(item.second));
        iter->Next();
    }
    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));
}

TEST(SSTableTest, HardWriteSSTable) {
    int32_t test_file_number = 13;

    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));

    SStableBuilder builder(test_file_number);

    std::map<InternalKey, Row> data;
    for(int i = 0; i < 3000; i++) {
        auto key = GenerateKey(i);
        auto row = GenerateRow(i);
        data[key] = row;
    }

    for(auto &item : data) {
        auto value = CodingUtil::EncodeRow(item.second);
        builder.Add(item.first, value);
    }

    auto sstable = builder.Builder();
    for(int i = 1; i <= 4000; i++) {
        auto iter = sstable->NewIterator();
        int start = rand() % 3000;
        iter->Seek(GenerateKey(start));

        auto map_iter = data.lower_bound(GenerateKey(start));
        while(map_iter != data.end()) {
            ASSERT_EQ(iter->Valid(), true);
            ASSERT_EQ(iter->GetKey(), map_iter->first) << "  iter->GetKey = " << iter->GetKey().vin_.vin
                << ", map_iter->first = " << map_iter->first.vin_.vin;
            ASSERT_EQ(iter->GetValue(), CodingUtil::EncodeRow(map_iter->second));

            map_iter++;
            iter->Next();
            break;
        }
        // ASSERT_EQ(iter->Valid(), false);
    }
    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));
}

    TEST(SSTableTest, Concurrency) {
        int32_t test_file_number = 13;

        DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));

        Cache<Block> cache(1 << 20);
        SStableBuilder builder(test_file_number, &cache);

        std::map<InternalKey, Row> data;
        for(int i = 0; i < 10000; i++) {
            auto key = GenerateKey(i);
            auto row = GenerateRow(i);
            data[key] = row;
        }

        for(auto &item : data) {
            auto value = CodingUtil::EncodeRow(item.second);
            builder.Add(item.first, value);
        }

        auto sstable = builder.Builder();

        std::vector<std::thread> threads;
        for(int i = 0; i < 10; i++) {
            threads.emplace_back([&](){
                for(int i = 1; i <= 30000; i++) {
                    auto iter = sstable->NewIterator();
                    int start = rand() % 10000;
                    iter->Seek(GenerateKey(start));

                    auto map_iter = data.lower_bound(GenerateKey(start));
                    if(map_iter != data.end()) {
                        ASSERT_EQ(iter->Valid(), true);
                        ASSERT_EQ(iter->GetKey(), map_iter->first) << "  iter->GetKey = " << iter->GetKey().vin_.vin
                            << ", map_iter->first = " << map_iter->first.vin_.vin;
                        ASSERT_EQ(iter->GetValue(), CodingUtil::EncodeRow(map_iter->second));

                        map_iter++;
                        iter->Next();
                    }
                }
            });
        }

        for(auto &thread : threads) {
            thread.join();
        }

        DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));
    }






}  // namespace LindormContest