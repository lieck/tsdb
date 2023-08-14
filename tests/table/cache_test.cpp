#include <gtest/gtest.h>

#include <memory>
#include "cache/cache.h"


namespace ljdb {


void InsertCache(LRUCache<size_t>& cache, size_t key) {
    auto value = new size_t(key);

    auto handle = cache.Insert(key, std::unique_ptr<size_t>(value), 1);
    cache.Release(handle);

    ASSERT(*handle->value_ == key, "value is not equal to key");
}

void QueryCache(LRUCache<size_t>& cache, size_t key, bool is) {
    auto handle = cache.Lookup(key);
    if(is) {
        ASSERT_TRUE(handle != nullptr);
        ASSERT_EQ(*handle->value_, key);
        cache.Release(handle);
    } else {
        ASSERT_TRUE(handle == nullptr);
    }
}


TEST(CacheTest, BasicAssertions) {
    const uint64_t cache_capacity = 10;
    LRUCache<size_t> cache;
    cache.SetCapacity(cache_capacity);

    for(size_t i = 0; i < cache_capacity; ++i) {
        InsertCache(cache, i);
    }

    for(size_t i = 0; i < cache_capacity; ++i) {
        QueryCache(cache, i, true);
    }

    InsertCache(cache, 10);
    QueryCache(cache, 0, false);
    for(size_t i = 0; i < cache_capacity; ++i) {
        QueryCache(cache, i + 1, true);
    }

    for(size_t i = 0; i < 3; i++) {
        InsertCache(cache, i + 11);
    }
    QueryCache(cache, 1, false);
    QueryCache(cache, 2, false);
    QueryCache(cache, 3, false);
    QueryCache(cache, 11, true);
    QueryCache(cache, 12, true);
    QueryCache(cache, 13, true);
}


} // namespace ljdb