#include "cache/cache.h"


namespace ljdb {

Cache::Cache() : lru_(nullptr, nullptr, 0), capacity_(0) {
    lru_.next_ = &lru_;
    lru_.prev_ = &lru_;
}

Cache::~Cache() {
    for (auto & it : table_) {
        auto e = it.second;
        e->deleter_(e->value_);
        delete e;
    }
}

auto Cache::Insert(uint64_t key, void *value, uint32_t charge, void (*deleter)(void *)) -> void {
    std::scoped_lock<std::mutex> lock(mutex_);
    ASSERT(table_.find(key) == table_.end(), "key already exists");

    auto e = new LRUHandle(value, deleter, charge);
    LruAppend(&lru_, e);
    usage_ += charge;
    table_[key] = e;

    while(usage_ > capacity_ && lru_.next_ != &lru_) {
        auto old = lru_.next_;
        LruRemove(old);
        table_.erase(reinterpret_cast<uint64_t>(old));
        usage_ -= old->charge_;
        old->deleter_(old->value_);
        delete old;
    }
}

auto Cache::Lookup(uint64_t key) -> void * {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto it = table_.find(key);
    if (it == table_.end()) {
        return nullptr;
    }
    if(it->second->refs_ == 0) {
        LruRemove(it->second);
    }

    it->second->refs_++;
    return it->second->value_;
}

auto Cache::Release(uint64_t key) -> void {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto it = table_.find(key);
    if (it == table_.end()) {
        return;
    }

    assert(it->second->refs_ > 0);
    it->second->refs_--;
    if (it->second->refs_ == 0) {
        LruAppend(&lru_, it->second);
    }
}

auto Cache::LruRemove(Cache::LRUHandle *e) -> void {
    e->prev_->next_ = e->next_;
    e->next_->prev_ = e->prev_;
}

auto Cache::LruAppend(Cache::LRUHandle *list, Cache::LRUHandle *e) -> void {
    e->next_ = list;
    e->prev_ = list->prev_;
    e->prev_->next_ = e;
    e->next_->prev_ = e;
}

Cache::LRUHandle::LRUHandle(void *value, void (*deleter)(void *), uint32_t charge)
    : value_(value), deleter_(deleter), charge_(charge), refs_(0), next_{nullptr}, prev_{nullptr} {}

} // namespace ljdb