#include "cache/table_cache.h"

#include <utility>
#include "cache/cache.h"

namespace LindormContest {


// iterator 中支持 cache 的删除操作
static void IteratorCleanupTableCache(void *arg, void* value) {
    auto table_cache = reinterpret_cast<Cache<SSTable>*>(arg);
    auto handle = reinterpret_cast<CacheHandle<SSTable>*>(value);
    table_cache->Release(handle);
}

auto TableCache::NewTableIterator(const FileMetaDataPtr& file_meta_data) -> std::unique_ptr<Iterator> {
    auto handle = FindTable(file_meta_data);
    auto iter = handle->value_->NewIterator();
    iter->RegisterCleanup(IteratorCleanupTableCache, &this->cache_, handle);
    return iter;
}

void TableCache::AddSSTable(std::unique_ptr<SSTable> sstable) {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto file_number = sstable->GetFileNumber();
    cache_.Insert(file_number, std::move(sstable), 1);
}

auto TableCache::FindTable(const FileMetaDataPtr& file_meta_data) -> CacheHandle<SSTable> * {
    // 加锁保护同时调用 FindTable 查询同一个不在缓存的 SSTable
    std::scoped_lock<std::mutex> lock(mutex_);

    auto handle = cache_.Lookup(file_meta_data->file_number_);
    if(handle != nullptr) {
        return handle;
    }

    auto sstable = new SSTable(file_meta_data->file_number_, file_meta_data->file_size_, block_cache_);
    handle = cache_.Insert(file_meta_data->file_number_, std::unique_ptr<SSTable>(sstable), 1);
    return handle;
}


}  // namespace LindormContest