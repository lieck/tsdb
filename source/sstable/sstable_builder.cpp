
#include "sstable/sstable_builder.h"
#include "common/macros.h"
#include "common/exception.h"
#include "util/coding.h"

namespace ljdb {

auto SStableBuilder::Add(const InternalKey &key, std::string &value) -> void {
    if(!block_builder_.Add(key, value)) {
        FlushBlock();

        // create a new block
        block_builder_.Init();
        block_meta_.emplace_back(offset_, key.Encode());

        if(!block_builder_.Add(key, value)) {
            throw Exception("SStableBuilder failed to add");
        }

        estimated_size_ += 8 + INTERNAL_KEY_LENGTH;
    }

    estimated_size_ += INTERNAL_KEY_LENGTH + value.size() + 8;
}

auto SStableBuilder::FlushBlock() -> void {
    if(!block_builder_.Empty()) {
        auto block = block_builder_.Builder();

        // TODO(block compress)

        // TODO(check sum)

        // TODO(write file) 写入压缩后的数据, 校验和
        DiskManager::WriteBlock(file_, block->GetData(), block->GetDataSize());

        offset_ += block->GetDataSize();

//        if(cache_ != nullptr) {
//            // TODO(cache) add to cache
//        }
    }
}

auto SStableBuilder::Builder() -> std::unique_ptr<SSTable> {
    FlushBlock();

    auto meta_block_offset = offset_;

    char coding_offset[4];

    // calculate meta block size
    uint32_t size = block_meta_.size() * BlockBuilder::ENTRY_LENGTH_SIZE + 100;
    for(auto &meta : block_meta_) {
        size += CodingUtil::LENGTH_SIZE;
        size += meta.first_key_.size();
    }

    // write meta block
    BlockBuilder builder(size);
    builder.Init();
    for(auto &meta : block_meta_) {
        CodingUtil::PutUint32(coding_offset, meta.offset_);
        if(!builder.Add(InternalKey(meta.first_key_), coding_offset)) {
            throw Exception("SStableBuilder failed to add");
        }
    }
    auto meta_block = builder.Builder();
    DiskManager::WriteBlock(file_, meta_block->GetData(), meta_block->GetDataSize());

    // write meta block offset
    CodingUtil::PutUint32(coding_offset, offset_);
    DiskManager::WriteBlock(file_, coding_offset, CodingUtil::LENGTH_SIZE);

    file_.flush();
    file_.close();

    return std::make_unique<SSTable>(file_number_, std::move(block_meta_), meta_block_offset);
}

}  // namespace ljdb