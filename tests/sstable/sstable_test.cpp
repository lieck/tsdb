#include <gtest/gtest.h>
#include <fstream>
#include "disk/disk_manager.h"
#include "sstable/sstable_builder.h"
#include "test_util.h"

namespace LindormContest {



TEST(SSTableTest, BasicAssertions) {
    int32_t test_file_number = 13;

    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));

    SStableBuilder builder(test_file_number);
    for(int i = 0; i < 100; i++) {
        auto key = GenerateKey(i);
        std::string value = "value" + std::to_string(i);
        builder.Add(key, value);
    }

    auto sstable = builder.Builder();
    auto iter = sstable->NewIterator();
    for(int i = 0; i < 100; i++) {
        ASSERT_EQ(iter->Valid(), true);
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        ASSERT_EQ(iter->GetKey(), GenerateKey(i));
        ASSERT_EQ(iter->GetValue(), value);
        iter->Next();
    }
    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));
}


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
    for(int i = 0; i < 300; i++) {
        auto key = GenerateKey(i);
        auto row = GenerateRow(i);
        data[key] = row;
    }

    for(auto &item : data) {
        auto value = CodingUtil::EncodeRow(item.second);
        builder.Add(item.first, value);
    }

    auto sstable = builder.Builder();
    for(int i = 18; i <= 300; i++) {
        auto iter = sstable->NewIterator();
        iter->Seek(GenerateKey(i));

        auto map_iter = data.lower_bound(GenerateKey(i));
        while(map_iter != data.end()) {
            ASSERT_EQ(iter->Valid(), true);
            ASSERT_EQ(iter->GetKey(), map_iter->first) << "  iter->GetKey = " << iter->GetKey().vin_.vin
                << ", map_iter->first = " << map_iter->first.vin_.vin;
            ASSERT_EQ(iter->GetValue(), CodingUtil::EncodeRow(map_iter->second));

            map_iter++;
            iter->Next();
        }
        ASSERT_EQ(iter->Valid(), false);
    }
    DiskManager::RemoveFile(GET_SSTABLE_NAME(test_file_number));
}


}  // namespace LindormContest