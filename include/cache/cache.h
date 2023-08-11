#pragma once

#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace ljdb {

struct CacheHandle {
    cache_id_t cache_id_;

    void *value_;
    void (*deleter_)(void* value);
    uint32_t charge_;

    uint32_t refs_{1};
    bool in_hash_table_{false};

    CacheHandle *next_{nullptr};
    CacheHandle *prev_{nullptr};

    explicit CacheHandle(cache_id_t cache_id, void *value, uint32_t charge, void (*deleter)(void* value) = nullptr);
};

class LRUCache {
public:
    explicit LRUCache();
    ~LRUCache();

    DISALLOW_COPY_AND_MOVE(LRUCache);

    // 设置 lru 的容量
    void SetCapacity(uint64_t capacity) { capacity_ = capacity; }

    // 插入一个新的节点到 lru 中, 返回该节点的指针
    // 如果 key 已经存在, 不会真正写入到 lru
    auto Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void* value)) -> CacheHandle*;

    // 查找 key 对应的节点, 如果不存在返回 nullptr
    // 返回的 CacheHandle 的所有权由 LRU 管理, 需要调用 Release 释放
    auto Lookup(uint64_t key) -> CacheHandle*;

    // 释放一个节点
    auto Release(CacheHandle *handle) -> void;

private:
    // 将 e 从 lru 中移除
    auto LruRemove(CacheHandle *e) -> void;

    // 将 e 插入到 list 的头节点之前
    auto LruAppend(CacheHandle *list, CacheHandle *e) -> void;

    std::mutex mutex_;

    uint64_t capacity_;
    uint64_t usage_{0};

    CacheHandle lru_;
    std::unordered_map<uint64_t, CacheHandle*> table_{};
};

class Cache {
public:
    Cache() = default;
    ~Cache() = default;

    DISALLOW_COPY_AND_MOVE(Cache);

    explicit Cache(uint64_t capacity) {
        for (auto & i : cache_) {
            i.SetCapacity(capacity / 16);
        }
    }

    auto Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void* value) = nullptr) -> CacheHandle* {
//        return cache_[Hash(key)].Insert(key, value, charge, deleter);
    }

    auto Lookup(uint64_t key) -> CacheHandle* {
//        return cache_[Hash(key)].Lookup(key);
    }

    auto Release(CacheHandle *handle) -> void {
        cache_[Hash(handle->cache_id_)].Release(handle);
    }

private:
    auto Hash(uint64_t key) -> size_t { return hasher_(key); }

    LRUCache cache_[16];
    std::hash<uint64_t> hasher_;
};





} // namespace ljdb