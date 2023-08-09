#pragma once


#include "sstable/sstable.h"
#include "cache.h"

namespace ljdb {


// 维护 SSTable 资源的 Cache
class TableCache {
private:
    struct FileHandle {
        std::shared_ptr<SSTable> sstable_;
        int refs_;
    };

public:
    explicit TableCache(size_t max_file_number) : cache_(max_file_number) {}

    auto OpenSSTable(file_number_t file_number) -> SSTable*;

    auto CloseSSTable(file_number_t file_number) -> void;

    void AddSSTable(std::unique_ptr<SSTable> sstable);

    // 为 SSTable 创建一个迭代器
    auto NewTableIterator(file_number_t file_number) -> std::unique_ptr<Iterator>;

private:
    ShardedCache cache_;

};



} // namespace ljdb