#include <gtest/gtest.h>
#include "mem_table/mem_table.h"
#include "test_util.h"

namespace LindormContest {

TEST(MemTableTest, BasicAssertions) {
    auto mem = new MemTable();
    for (int i = 0; i < 1000; i++) {
        int key = rand() % 5000;
        int timestamp = rand() % 10000;

        Row row;
        row.vin = GenerateVin(key);
        row.timestamp = timestamp;
        mem->Insert(row);
    }

    auto mem_iter = mem->NewIterator();
    InternalKey key;
    while (mem_iter->Valid()) {
        auto k = mem_iter->GetKey();
        ASSERT_TRUE(key < k);
        mem_iter->Next();
    }

    delete mem;
}


} // namespace LindormContest
