#include "snappy/snappy.h"
#include "sstable/sstable_builder.h"
#include "common/macros.h"
#include "common/exception.h"
#include "util/coding.h"

namespace LindormContest {

auto SStableBuilder::Add(const InternalKey &key, std::string &value) -> void {
    ASSERT(end_key_ < key || end_key_ == key, "SStableBuilder::Add key must be increasing");
    if(!block_builder_.Add(key, value)) {
        FlushBlock();

        // create a new block
        block_builder_.Init();
        if(!block_builder_.Add(key, value)) {
            throw Exception("SStableBuilder failed to add");
        }

        estimated_size_ += 8 + INTERNAL_KEY_SIZE;
    }

    end_key_ = key;
    estimated_size_ += INTERNAL_KEY_SIZE + value.size() + 8;
}

auto SStableBuilder::FlushBlock() -> void {
    if(!block_builder_.Empty()) {
        auto block = block_builder_.Builder();

        // TODO(block compress)

        // TODO(check sum)

        // 压缩并写入
        std::string compressed;
        snappy::Compress(block->GetData(), block->GetDataSize(), &compressed);
        DiskManager::WriteBlock(file_, compressed.data(), compressed.size());

        auto block_id = offset_;

        // update block meta
        block_meta_.emplace_back(offset_, compressed.size(), end_key_);
        offset_ += compressed.size();

        // block cache
        if(block_cache_ != nullptr) {
            auto block_size = block->GetDataSize();
            auto bh = block_cache_->Insert(
                    GetBlockCacheID(block_id), std::move(block), block_size);
            block_cache_->Release(bh);
        }

    }
}

auto SStableBuilder::Builder() -> std::unique_ptr<SSTable> {
    FlushBlock();

    // calculate index block size
    uint32_t size = block_meta_.size() * (BlockBuilder::ENTRY_LENGTH_SIZE + BLOCK_HEADER_SIZE  +
            INTERNAL_KEY_SIZE) + 400;

    // write index block
    BlockBuilder builder(size);
    builder.Init();

    char buffer[BLOCK_HEADER_SIZE];
    for(auto &meta : block_meta_) {
        BlockHeader value(meta.offset_, meta.size_);
        value.EncodeTo(buffer);

        if(!builder.Add(meta.end_key_, std::string_view(buffer, BLOCK_HEADER_SIZE))) {
            throw Exception("SStableBuilder failed to add");
        }
    }

    auto index_block = builder.Builder();
    DiskManager::WriteBlock(file_, index_block->GetData(), index_block->GetDataSize());
    ASSERT(index_block->GetDataSize() > 0, "index block size must be greater than 0");

    // write meta block offset
    CodingUtil::PutUint32(buffer, offset_);
    offset_ += index_block->GetDataSize();

    DiskManager::WriteBlock(file_, buffer, CodingUtil::LENGTH_SIZE);
    offset_ += CodingUtil::LENGTH_SIZE;

    file_.flush();
    file_.close();

    estimated_size_ = 0;
    return std::make_unique<SSTable>(file_number_, offset_, std::move(index_block), block_cache_);
}

auto SStableBuilder::GetBlockCacheID(block_id_t block_id) -> cache_id_t {
    return static_cast<cache_id_t>(file_number_) << 32 | block_id;
}

}  // namespace LindormContest