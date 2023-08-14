#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <fstream>

#include "sstable/block.h"
#include "disk/disk_manager.h"
#include "common/macros.h"
#include "cache/cache.h"

namespace ljdb {

const constexpr size_t SSTABLE_FOOTER_LENGTH = CodingUtil::LENGTH_SIZE;


// iterator 中支持 cache 的删除操作
static void IteratorCleanupBlockCache(void *arg, void* value) {
    auto block_cache = reinterpret_cast<Cache<Block>*>(arg);
    auto handle = reinterpret_cast<CacheHandle<Block>*>(value);
    block_cache->Release(handle);
}

// iterator 中不使用 cache 的删除操作
static void IteratorCleanupBlock(void *arg, void* value) {
    auto block = reinterpret_cast<Block*>(value);
    delete block;
}


class SSTable {
public:
    SSTable(file_number_t file_number, uint64_t file_size, Cache<Block> *cache = nullptr);

    SSTable(file_number_t file_number, uint64_t file_size, std::unique_ptr<Block> index_block, Cache<Block> *cache = nullptr)
        : file_number_(file_number), file_size_(file_size), index_block_(std::move(index_block)), cache_(cache) {}

    DISALLOW_COPY_AND_MOVE(SSTable);

    auto NewIterator() -> std::unique_ptr<Iterator>;

    auto GetFileNumber() const -> file_number_t { return file_number_; }

    auto GetFileSize() const -> uint64_t { return file_size_; }

    static auto ReadBlock(void* arg, const std::string &key) -> std::unique_ptr<Iterator>;

    auto GetBlockCacheID(block_id_t block_id) -> cache_id_t;

private:
    file_number_t file_number_; // sstable 编号
    uint64_t file_size_; // sstable 大小

    Cache<Block>* cache_;

    // index block
    std::unique_ptr<Block> index_block_{nullptr};
};

}  // namespace ljdb

