#include <gtest/gtest.h>
#include "sstable/block_builder.h"
#include "db/format.h"
#include "test_util.h"


namespace ljdb {


TEST(BlockTest, BasicAssertions) {
    BlockBuilder builder(4096);
    builder.Init();
    ASSERT_TRUE(builder.Add(generate_key(1), "value1"));
    ASSERT_TRUE(builder.Add(generate_key(2), "value2"));
    ASSERT_TRUE(builder.Add(generate_key(3), "value3"));

    auto block = builder.Builder();
    auto iter = block->NewIterator();
    ASSERT_EQ(iter->Valid(), true);
    ASSERT_EQ(iter->GetKey(), generate_key(1));
    ASSERT_EQ(iter->GetValue(), "value1");
    iter->Next();

    ASSERT_EQ(iter->Valid(), true);
    ASSERT_EQ(iter->GetKey(), generate_key(2));
    ASSERT_EQ(iter->GetValue(), "value2");
    iter->Next();

    ASSERT_EQ(iter->Valid(), true);
    ASSERT_EQ(iter->GetKey(), generate_key(3));
    ASSERT_EQ(iter->GetValue(), "value3");
    iter->Next();

    ASSERT_EQ(iter->Valid(), false);
}


}  // namespace ljdb