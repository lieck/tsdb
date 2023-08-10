
#include <algorithm>

#include "sstable/sstable.h"
#include "util/coding.h"
#include "common/macros.h"
#include "disk/disk_manager.h"
#include "common/two_level_iterator.h"
#include "sstable/sstable_builder.h"

namespace ljdb {

static void DeleterBlock(void* value) {
    delete reinterpret_cast<Block*>(value);
}


SSTable::SSTable(file_number_t file_number, uint64_t file_size, ShardedCache *cache = nullptr) : file_number_(file_number), file_size_(file_size), cache_(cache) {
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

    // TODO 使用 Cache 读取
    if(sstable->cache_ != nullptr) {
        auto cache_id = sstable->GetBlockCacheID(block_header.offset_);
        auto cache_block = sstable->cache_->Lookup(cache_id);
        if(cache_block != nullptr) {
            auto block = reinterpret_cast<Block *>(cache_block);
            return block->NewIterator();
        }
    }

    char *block_buffer = new char[block_header.size_];
    DiskManager::ReadBlock(sstable->file_number_, block_buffer, block_header.size_, block_header.offset_);
    auto block = new Block(block_buffer, block_header.size_);

    if(sstable->cache_ != nullptr) {
        // TODO cache 冲突为解决
        auto cache_id = sstable->GetBlockCacheID(block_header.offset_);
        sstable->cache_->Insert(cache_id, block, 1, DeleterBlock);
    }

    return block->NewIterator();
}

auto SSTable::GetBlockCacheID(block_id_t block_id) -> cache_id_t {
    return static_cast<cache_id_t>(file_number_) << 32 | block_id;
}


}  // namespace ljdb