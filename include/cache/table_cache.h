#pragma once


#include "sstable/sstable.h"
#include "cache.h"
#include "db/file_meta_data.h"

namespace ljdb {


// 维护 SSTable 资源的 Cache
class TableCache {
private:
    // FileHandle 可能会被多个线程使用
    struct FileHandle {
        SSTable* sstable_{};

        static void Deleter(void* value) {
            auto file_handle = reinterpret_cast<FileHandle*>(value);
            delete file_handle->sstable_;
            delete file_handle;
        }
    };

public:
    explicit TableCache(size_t max_file_number) : cache_(max_file_number) {}

    void AddSSTable(std::unique_ptr<SSTable> sstable);

    // 为 SSTable 创建一个迭代器
    auto NewTableIterator(FileMetaData *file_meta_data) -> std::unique_ptr<Iterator>;

private:
    auto FindTable(FileMetaData *file_meta_data) -> SSTable*;

    std::mutex mutex_;
    Cache<SSTable> cache_;


};



} // namespace ljdb