
#include <algorithm>
#include <cstring>

#include "sstable/sstable.h"
#include "util/coding.h"
#include "common/macros.h"
#include "disk/disk_manager.h"

namespace ljdb {

auto SSTable::ReadBlock(uint32_t block_idx) -> std::unique_ptr<Block> {
    ASSERT(block_idx < block_metas_.size(), "block index out of range");

    auto begin_offset = block_metas_[block_idx].offset_;
    auto end_offset = block_idx + 1 != block_metas_.size() ? block_metas_[block_idx + 1].offset_ : meta_block_offset_;
    auto size = end_offset - begin_offset;

    char *data = new char[size];
    DiskManager::ReadBlock(file_, data, size, begin_offset);

    return std::make_unique<Block>(data, size);
}

auto SSTable::FindBlockIdx(const InternalKey &key) -> uint32_t {
    auto cmp = [](BlockMeta &meta, const InternalKey &b) -> bool {
        return InternalKey(meta.first_key_) < b;
    };

    uint32_t idx = std::lower_bound(block_metas_.begin(), block_metas_.end(), key, cmp) - block_metas_.begin();
    return idx;
}

auto SSTable::ReadMetaBlock() -> void {
    // read meta block offset
    char buf[CodingUtil::LENGTH_SIZE];

    auto offset = DiskManager::GetFileSize(file_) - CodingUtil::LENGTH_SIZE;
    DiskManager::ReadBlock(file_, buf, CodingUtil::LENGTH_SIZE, offset);
    auto curr_offset = CodingUtil::DecodeUint32(buf);
    meta_block_offset_ = curr_offset;

    auto meta_block_size = offset - curr_offset;
    char *meta_block = new char[meta_block_size];
    DiskManager::ReadBlock(file_, meta_block, meta_block_size, curr_offset);

    Block block(meta_block, meta_block_size);
    block_metas_.reserve(block.GetEntrySize());

    for(auto iter = block.NewIterator(); iter->Valid(); iter->Next()) {
        InternalKey key = iter->GetKey();
        auto value = iter->GetValue();
        auto block_offset = CodingUtil::DecodeUint32(value.data());

        block_metas_.emplace_back(block_offset, key.Encode());
    }
}

auto SSTable::NewIterator() -> std::unique_ptr<Iterator> {
    return std::make_unique<SSTableIterator>(this);
}


SSTable::SSTableIterator::SSTableIterator(SSTable *sstable) : sstable_(sstable) {
    if(sstable_->GetBlockNum() == 0) {
        return;
    }
    block_idx_ = 0;
    block_ = sstable->ReadBlock(block_idx_);
    block_iter_ = block_->NewIterator();
}

SSTable::SSTableIterator::SSTableIterator(SSTable *sstable, const InternalKey &key) : sstable_(sstable) {
    block_idx_ = sstable->FindBlockIdx(key);
    block_ = sstable->ReadBlock(block_idx_);
    block_iter_ = block_->NewIterator();
    block_iter_->Seek(key);
    if(!block_iter_->Valid()) {
        block_idx_++;
    }
}

auto SSTable::SSTableIterator::GetKey() -> InternalKey {
    return block_iter_->GetKey();
}

auto SSTable::SSTableIterator::GetValue() -> std::string {
    return block_iter_->GetValue();
}

void SSTable::SSTableIterator::Next()  {
    block_iter_->Next();
    if(!block_iter_->Valid()) {
        ++block_idx_;

        if(Valid()) {
            block_ = sstable_->ReadBlock(block_idx_);
            block_iter_ = block_->NewIterator();
        }
    }
}

void SSTable::SSTableIterator::SeekToFirst() {
    block_idx_ = 0;
    block_ = sstable_->ReadBlock(block_idx_);
    block_iter_ = block_->NewIterator();
}

void SSTable::SSTableIterator::Seek(const InternalKey &key) {
    block_idx_ = sstable_->FindBlockIdx(key);
    block_ = sstable_->ReadBlock(block_idx_);
    block_iter_ = block_->NewIterator();
    block_iter_->Seek(key);
    if(!block_iter_->Valid()) {
        block_idx_++;
    }
}

}  // namespace ljdb