#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <fstream>

#include "common/config.h"
#include "sstable/block.h"
#include "sstable/block_builder.h"
#include "sstable/sstable.h"
#include "disk/disk_manager.h"

namespace LindormContest {

static constexpr uint32_t BLOCK_HEADER_SIZE = 16;

struct BlockHeader {
    uint64_t offset_;
    uint64_t size_;

    BlockHeader(uint64_t offset, uint64_t size) : offset_(offset), size_(size) {}

    explicit BlockHeader(const std::string_view &data) {
        offset_ = CodingUtil::DecodeFixed64(data.data());
        size_ = CodingUtil::DecodeFixed64(data.data() + CodingUtil::FIXED_64_SIZE);
    }

    auto EncodeTo(char *dst) const -> void {
        CodingUtil::EncodeValue(dst, offset_);
        CodingUtil::EncodeValue(dst + CodingUtil::FIXED_64_SIZE, size_);
    }
};

struct BlockMeta {
    uint32_t offset_;
    uint64_t size_;
    InternalKey first_key_;

    BlockMeta(uint32_t offset_, uint64_t size, InternalKey first_key)
            : offset_(offset_), size_(size), first_key_(std::move(first_key)) {}
};

class SStableBuilder {
public:
    explicit SStableBuilder(file_number_t file_number, Cache<Block> *block_cache = nullptr, uint32_t block_size = SSTABLE_BLOCK_CAPACITY)
        : file_number_(file_number), block_cache_(block_cache), file_(DiskManager::CreateSSTableFile(file_number)), block_builder_(block_size) {}

    // 要求：之前没有调用过 Builder
    // TODO 将行式存储改为列式存储
    auto Add(const InternalKey &key, std::string &value) -> void;

    auto Builder() -> std::unique_ptr<SSTable>;

    auto EstimatedSize() const { return estimated_size_; }

private:
    auto FlushBlock() -> void;

    // 与 SSTable 的 GetBlockCacheID 相同
    auto GetBlockCacheID(block_id_t block_id) -> cache_id_t;

    file_number_t file_number_;

    std::ofstream file_;
    Cache<Block> *block_cache_;

    BlockBuilder block_builder_;    // 用于构建 block
    InternalKey end_key_;         // 当前构建 block 的最后一个 key

    std::vector<BlockMeta> block_meta_{};

    uint32_t estimated_size_{0};
    uint64_t offset_{0};
};

}  // namespace ljdb


