#include "cache/table_cache.h"
#include "cache/cache.h"
#include "db/file_meta_data.h"

namespace ljdb {


auto TableCache::NewTableIterator(FileMetaData *file_meta_data) -> std::unique_ptr<Iterator> {
    auto file_handle = FindTable(file_meta_data);
    return file_handle->sstable_->NewIterator();
}

void TableCache::AddSSTable(std::unique_ptr<SSTable> sstable) {
    std::scoped_lock<std::mutex> lock(mutex_);
    auto file_number = sstable->GetFileNumber();
    auto file_handle = new FileHandle{sstable.release()};
    cache_.Insert(file_number, file_handle, 1, FileHandle::Deleter);
}

auto TableCache::FindTable(FileMetaData *file_meta_data) -> TableCache::FileHandle * {
    // 加锁保护同时调用 FindTable 查询同一个不在缓存的 SSTable
    std::scoped_lock<std::mutex> lock(mutex_);

    auto handle = cache_.Lookup(file_meta_data->file_number_);
    if(handle != nullptr) {
        return reinterpret_cast<FileHandle*>(handle);
    }

    auto sstable = new SSTable(file_meta_data->file_number_, file_meta_data->file_size_);
    auto file_handle = new FileHandle{sstable};
    cache_.Insert(file_meta_data->file_number_, file_handle, 1, FileHandle::Deleter);
    return file_handle;

}


} // namespace ljdb