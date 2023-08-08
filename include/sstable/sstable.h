#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <fstream>

#include "sstable/block.h"
#include "buffer/buffer_pool_manager.h"
#include "disk/disk_manager.h"
#include "common/macros.h"

namespace ljdb {

class SSTable {
private:
    class SSTableIterator;

public:
    explicit SSTable(file_number_t file_number) : file_number_(file_number) {
        file_ = DiskManager::OpenSSTableFile(file_number);
        ReadMetaBlock();
    }

    explicit SSTable(file_number_t file_number, std::vector<BlockMeta> &&block_metas, uint64_t meta_block_offset)
        : block_metas_(std::move(block_metas)), meta_block_offset_(meta_block_offset), file_number_(file_number) {
        file_ = DiskManager::OpenSSTableFile(file_number);
    }

    DISALLOW_COPY_AND_MOVE(SSTable);

    auto NewIterator() -> std::unique_ptr<Iterator>;

    auto GetFileNumber() const -> file_number_t { return file_number_; }

    // get the number of blocks
    auto GetBlockNum() const -> uint32_t { return block_metas_.size(); }

private:
    // read the meta block from the file
    auto ReadMetaBlock() -> void;

    // read the block from the file
    auto ReadBlock(uint32_t block_idx) -> std::unique_ptr<Block>;

    // find the block index of the key
    auto FindBlockIdx(const InternalKey &key) -> uint32_t;

    // the file of the sstable
    std::ifstream file_;

    file_number_t file_number_;

    std::vector<BlockMeta> block_metas_;

    uint64_t meta_block_offset_{0};
};


// SSTableIterator 的生命周期依赖于 SSTable
// 不应该在 SSTable 释放后保留 SSTableIterator
class SSTable::SSTableIterator : public Iterator {
public:
    explicit SSTableIterator(SSTable *sstable);

    explicit SSTableIterator(SSTable *sstable, const InternalKey &key);

    ~SSTableIterator() override = default;

    DISALLOW_COPY_AND_MOVE(SSTableIterator);

    auto GetKey() -> InternalKey override;

    auto GetValue() -> std::string override;

    auto Valid() -> bool override { return sstable_ != nullptr && block_idx_ < sstable_->GetBlockNum(); }

    void Next() override;

    void SeekToFirst() override;

    void Seek(const InternalKey &key) override;

private:
    SSTable *sstable_{nullptr};
    std::unique_ptr<Block> block_{nullptr};
    uint32_t block_idx_{0};
    std::unique_ptr<Iterator> block_iter_{};
};



}  // namespace ljdb

