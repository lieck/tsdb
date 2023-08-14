
#include <algorithm>

#include "sstable/sstable.h"
#include "util/coding.h"
#include "common/macros.h"
#include "disk/disk_manager.h"
#include "common/two_level_iterator.h"
#include "sstable/sstable_builder.h"

namespace ljdb {


SSTable::SSTable(file_number_t file_number, uint64_t file_size, Cache<Block> *cache) : file_number_(file_number), file_size_(file_size), cache_(cache) {
    // read sstable footer
    char footer_buffer[SSTABLE_FOOTER_LENGTH];
    auto footer_offset = file_size_ - SSTABLE_FOOTER_LENGTH;
    DiskManager::ReadBlock(file_number_, footer_buffer, SSTABLE_FOOTER_LENGTH, footer_offset);

    // read sstable index block
    auto index_offset = CodingUtil::DecodeUint32(footer_buffer);
    auto index_block_size = footer_offset - index_offset;

    char *index_buffer = new char[index_block_size];
    DiskManager::ReadBlock(file_number_, index_buffer, index_block_size, index_offset);

    index_block_ = std::make_unique<Block>(index_buffer, index_block_size);
}

auto SSTable::NewIterator() -> std::unique_ptr<Iterator> {
    auto index_iterator = index_block_->NewIterator();
    return NewTwoLevelIterator(std::move(index_iterator), ReadBlock, this);
}

auto SSTable::ReadBlock(void *arg, const std::string &key) -> std::unique_ptr<Iterator> {
    auto *sstable = reinterpret_cast<SSTable *>(arg);
    BlockHeader block_header(key);

    // 读取 cache
    if(sstable->cache_ != nullptr) {
        auto cache_id = sstable->GetBlockCacheID(block_header.offset_);
        auto handle = sstable->cache_->Lookup(cache_id);
        if(handle != nullptr) {
            auto iter = handle->value_->NewIterator();
            iter->RegisterCleanup(IteratorCleanupBlockCache, sstable->cache_, handle);
            return iter;
        }
    }

    // cache 不存在, 从磁盘中读取
    char *block_buffer = new char[block_header.size_];
    DiskManager::ReadBlock(sstable->file_number_, block_buffer, block_header.size_, block_header.offset_);
    auto block = new Block(block_buffer, block_header.size_);

    auto iter = block->NewIterator();

    if(sstable->cache_ != nullptr) {
        // 插入到 cache 中, 当前引用计数为1
        auto cache_id = sstable->GetBlockCacheID(block_header.offset_);
        auto handle = sstable->cache_->Insert(cache_id, std::unique_ptr<Block>(block), 1);
        iter->RegisterCleanup(IteratorCleanupBlockCache, sstable->cache_, handle);
    } else {
        iter->RegisterCleanup(IteratorCleanupBlock, nullptr, block);
    }

    return iter;
}

auto SSTable::GetBlockCacheID(block_id_t block_id) -> cache_id_t {
    return static_cast<cache_id_t>(file_number_) << 32 | block_id;
}


}  // namespace ljdb