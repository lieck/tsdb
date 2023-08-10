#pragma once

#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace ljdb {

static constexpr int CACHE_LINE_SIZE = 64;


class Cache {
private:
    struct LRUHandle {
        void *value_;
        void (*deleter_)(void* value);
        uint32_t charge_;

        uint32_t refs_;

        LRUHandle *next_;
        LRUHandle *prev_;

        explicit LRUHandle(void *value, uint32_t charge, void (*deleter)(void* value) = nullptr);
    };

public:
    explicit Cache();
    ~Cache();

    DISALLOW_COPY_AND_MOVE(Cache);

    void SetCapacity(uint64_t capacity) { capacity_ = capacity; }

    auto Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void* value)) -> void;

    auto Lookup(uint64_t key) -> void*;

    auto Release(uint64_t key) -> void;

    auto TotalCharge() -> uint64_t { return usage_; }

private:
    // 将 e 从 lru 中移除
    auto LruRemove(LRUHandle *e) -> void;

    // 将 e 插入到 list 的头节点之前
    auto LruAppend(LRUHandle *list, LRUHandle *e) -> void;

    std::mutex mutex_;

    uint64_t capacity_;
    uint64_t usage_{0};

    LRUHandle lru_;
    std::unordered_map<uint64_t, LRUHandle*> table_{};
};

class ShardedCache {
public:
    explicit ShardedCache(uint64_t capacity) {
        for (auto & i : cache_) {
            i.SetCapacity(capacity / 16);
        }
    }

    auto Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void* value) = nullptr) -> void {
        cache_[Hash(key)].Insert(key, value, charge, deleter);
    }

    auto Lookup(uint64_t key) -> void* {
        return cache_[Hash(key)].Lookup(key);
    }

    auto Release(uint64_t key) -> void {
        cache_[Hash(key)].Release(key);
    }

private:
    auto Hash(uint64_t key) -> size_t { return hasher_(key); }

    Cache cache_[16];
    std::hash<uint64_t> hasher_;
};





}; // namespace ljdb