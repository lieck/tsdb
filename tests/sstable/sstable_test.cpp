#include <gtest/gtest.h>
#include <fstream>
#include "disk/disk_manager.h"
#include "sstable/sstable_builder.h"
#include "test_util.h"

namespace ljdb {



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


} // namespace ljdb