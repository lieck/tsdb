#include "cache/table_cache.h"

#include <utility>
#include "cache/cache.h"

namespace ljdb {

auto TableCache::NewTableIterator(const FileMetaDataPtr& file_meta_data) -> std::unique_ptr<Iterator> {
    auto sstable = FindTable(file_meta_data);
    return sstable->NewIterator();
}

void TableCache::AddSSTable(std::unique_ptr<SSTable> sstable) {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto file_number = sstable->GetFileNumber();
    cache_.Insert(file_number, std::move(sstable), 1);
}

auto TableCache::FindTable(const FileMetaDataPtr& file_meta_data) -> SSTable * {
    // 加锁保护同时调用 FindTable 查询同一个不在缓存的 SSTable
    std::scoped_lock<std::mutex> lock(mutex_);

    auto handle = cache_.Lookup(file_meta_data->file_number_);
    if(handle != nullptr) {
        return handle->value_.get();
    }

    auto sstable = new SSTable(file_meta_data->file_number_, file_meta_data->file_size_);
    cache_.Insert(file_meta_data->file_number_, std::unique_ptr<SSTable>(sstable), 1);
    return sstable;
}


} // namespace ljdb