#pragma once


#include "sstable/sstable.h"
#include "cache.h"
#include "db/file_meta_data.h"

namespace LindormContest {


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
    explicit TableCache(size_t max_file_number, Cache<Block> *block_cache = nullptr) : cache_(max_file_number), block_cache_(block_cache) {}

    void AddSSTable(std::unique_ptr<SSTable> sstable);

    // 为 SSTable 创建一个迭代器
    auto NewTableIterator(const FileMetaDataPtr& file_meta_data) -> std::unique_ptr<Iterator>;

private:
    auto FindTable(const FileMetaDataPtr& file_meta_data) -> CacheHandle<SSTable>*;

    Cache<Block> *block_cache_;

    std::mutex mutex_;
    Cache<SSTable> cache_;
};



}  // namespace LindormContest