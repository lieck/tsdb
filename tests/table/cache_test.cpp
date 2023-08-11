#include <gtest/gtest.h>
#include "cache/cache.h"


namespace ljdb {


void InsertCache(Cache& cache, size_t key) {
    auto value = new size_t(key);
    auto handle = cache.Insert(key, value, 1, nullptr);
    cache.Release(handle);
}

//void QueryCache(Cache& cache, size_t key, bool is) {
//    auto handle = cache.Lookup(key);
//    if(is) {
//        ASSERT_TRUE(handle != nullptr);
//        ASSERT_EQ(*reinterpret_cast<size_t*>(handle->value_), key);
//        cache.Release(handle);
//    } else {
//        ASSERT_TRUE(handle == nullptr);
//    }
//}


TEST(CacheTest, BasicAssertions) {
//    const size_t cache_capacity = 10;
//    Cache cache(cache_capacity);
//
//
//    for(size_t i = 0; i < cache_capacity; ++i) {
//        InsertCache(cache, i);
//    }
//
//    for(size_t i = 0; i < cache_capacity; ++i) {
//        QueryCache(cache, i, true);
//    }
//
//    InsertCache(cache, 11);
//    QueryCache(cache, 0, false);
//    for(size_t i = 0; i < cache_capacity; ++i) {
//        QueryCache(cache, i + 1, true);
//    }
//
//    for(size_t i = 0; i < 3; i++) {
//        InsertCache(cache, i + 12);
//    }
//    QueryCache(cache, 1, false);
//    QueryCache(cache, 2, false);
//    QueryCache(cache, 3, false);
//    QueryCache(cache, 12, true);
//    QueryCache(cache, 13, true);
//    QueryCache(cache, 14, true);
}


} // namespace ljdb