#include "cache/cache.h"


namespace ljdb {

LRUCache::LRUCache() : lru_(0, nullptr, 0, nullptr), capacity_(0) {
    lru_.next_ = &lru_;
    lru_.prev_ = &lru_;
}

LRUCache::~LRUCache() {
    for (auto & it : table_) {
        auto e = it.second;
        e->deleter_(e->value_);
        delete e;
    }
}

auto LRUCache::Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void *)) -> CacheHandle* {
    auto cache_handle = new CacheHandle(key, value, charge, deleter);

    std::scoped_lock<std::mutex> lock(mutex_);
    if(table_.find(key) != table_.end()) {
        return cache_handle;
    }

    LruAppend(&lru_, cache_handle);
    cache_handle->in_hash_table_ = true;
    usage_ += charge;
    table_[key] = cache_handle;

    while(usage_ > capacity_ && lru_.next_ != &lru_) {
        auto old = lru_.next_;
        LruRemove(old);
        table_.erase(reinterpret_cast<uint64_t>(old));
        usage_ -= old->charge_;
        old->deleter_(old->value_);
        delete old;
    }
    return cache_handle;
}

auto LRUCache::Lookup(uint64_t key) -> CacheHandle * {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    if(it->second->refs_ == 0) {
        LruRemove(it->second);
    }

    it->second->refs_++;
    return it->second;
}

auto LRUCache::Release(CacheHandle *handle) -> void {
    std::scoped_lock<std::mutex> lock(mutex_);
    assert(handle->refs_ > 0);
    handle->refs_--;
    if (handle->refs_ == 0) {
        LruRemove(handle);
    }
}

auto LRUCache::LruRemove(CacheHandle *e) -> void {
    e->prev_->next_ = e->next_;
    e->next_->prev_ = e->prev_;
}

auto LRUCache::LruAppend(CacheHandle *list, CacheHandle *e) -> void {
    e->next_ = list;
    e->prev_ = list->prev_;
    e->prev_->next_ = e;
    e->next_->prev_ = e;
}

CacheHandle::CacheHandle(cache_id_t cache_id, void *value, uint32_t charge, void (*deleter)(void *))
    : cache_id_(cache_id), value_(value), deleter_(deleter), charge_(charge) {}

} // namespace ljdb