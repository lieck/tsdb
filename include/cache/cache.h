#pragma once

#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace LindormContest {

    template<typename T>
    struct CacheHandle {
        cache_id_t cache_id_;

        std::unique_ptr<T> value_;
        uint32_t charge_;

        uint32_t refs_{1};
        bool in_hash_table_{false};

        CacheHandle *next_{nullptr};
        CacheHandle *prev_{nullptr};

        explicit CacheHandle(cache_id_t cache_id, std::unique_ptr<T> value, uint32_t charge) :
                cache_id_(cache_id), value_(std::move(value)), charge_(charge) {}
    };

    template<typename T>
    class LRUCache {
    public:
        explicit LRUCache() : lru_(0, nullptr, 0) {
            lru_.next_ = &lru_;
            lru_.prev_ = &lru_;
        }

        ~LRUCache() {
            for (auto &it: table_) {
                delete it.second;
            }
        }

        DISALLOW_COPY_AND_MOVE(LRUCache);

        // 设置 lru 的容量
        void SetCapacity(uint64_t capacity) { capacity_ = capacity; }

        // 插入一个新的节点到 lru 中, 返回该节点的指针
        // 如果 key 已经存在, 不会真正写入到 lru
        // deleter 用于释放 value 的内存, 如果为 nullptr, 则使用 free
        auto Insert(uint64_t key, std::unique_ptr<T> value, uint32_t charge) -> CacheHandle<T> * {
            auto cache_handle = new CacheHandle(key, std::move(value), charge);

            std::scoped_lock<std::mutex> lock(mutex_);
            if (table_.find(key) != table_.end()) {
                return cache_handle;
            }

            usage_ += charge;
            while (usage_ > capacity_ && lru_.next_ != &lru_) {
                auto old = lru_.next_;
                LruRemove(old);
                table_.erase(old->cache_id_);
                usage_ -= old->charge_;
                delete old;
            }

            cache_handle->in_hash_table_ = true;
            table_[key] = cache_handle;

            return cache_handle;
        }

        // 查找 key 对应的节点, 如果不存在返回 nullptr
        // 返回的 CacheHandle 的所有权由 LRU 管理, 需要调用 Release 释放
        auto Lookup(uint64_t key) -> CacheHandle<T> * {
            std::scoped_lock<std::mutex> lock(mutex_);
            auto it = table_.find(key);
            if (it == table_.end()) {
                return nullptr;
            }
            if (it->second->refs_ == 0) {
                LruRemove(it->second);
            }

            it->second->refs_++;
            return it->second;
        }

        // 释放一个节点
        auto Release(CacheHandle<T> *handle) -> void {
            std::scoped_lock<std::mutex> lock(mutex_);
            assert(handle->refs_ > 0);
            handle->refs_--;
            if (handle->refs_ == 0) {
                LruAppend(&lru_, handle);
            }

            while (usage_ > capacity_ && lru_.next_ != &lru_) {
                auto old = lru_.next_;
                LruRemove(old);
                table_.erase(old->cache_id_);
                usage_ -= old->charge_;
                delete old;
            }
        }

    private:
        // 将 e 从 lru 中移除
        auto LruRemove(CacheHandle<T> *e) -> void {
            ASSERT(e->next_ != nullptr && e->prev_ != nullptr, "e->next_ != nullptr && e->prev_ != nullptr");
            e->prev_->next_ = e->next_;
            e->next_->prev_ = e->prev_;
        }

        // 将 e 插入到 list 的头节点之前
        auto LruAppend(CacheHandle<T> *list, CacheHandle<T> *e) -> void {
            e->prev_ = list->prev_;
            e->next_ = list;

            list->prev_->next_ = e;
            list->prev_ = e;
        }

        std::mutex mutex_;

        uint64_t capacity_{0};
        uint64_t usage_{0};

        CacheHandle<T> lru_;
        std::unordered_map<uint64_t, CacheHandle<T> *> table_{};
    };

    template<typename T>
    class Cache {
    public:
        explicit Cache(uint64_t capacity) {
            for (auto &i: cache_) {
                i.SetCapacity(capacity / 16);
            }
        }

        ~Cache() = default;

        DISALLOW_COPY_AND_MOVE(Cache);

        auto Insert(uint64_t key, std::unique_ptr<T> value, uint32_t charge) -> CacheHandle<T> * {
            return cache_[Hash(key)].Insert(key, std::move(value), charge);
        }

        auto Lookup(uint64_t key) -> CacheHandle<T> * {
            return cache_[Hash(key)].Lookup(key);
        }

        auto Release(CacheHandle<T> *handle) -> void {
            cache_[Hash(handle->cache_id_)].Release(handle);
        }

    private:
        auto Hash(uint64_t key) -> size_t { return hasher_(key) % 16; }

        LRUCache<T> cache_[16];
        std::hash<uint64_t> hasher_{};
    };


} // namespace ljdb
