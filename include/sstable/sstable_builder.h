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

namespace ljdb {

class SStableBuilder {
public:
    explicit SStableBuilder(file_number_t file_number, uint32_t block_size = SSTABLE_BLOCK_CAPACITY)
        : file_number_(file_number), file_(DiskManager::CreateSSTableFile(file_number)), block_builder_(block_size) {}

    // 要求：之前没有调用过 Builder
    // TODO 将行式存储改为列式存储
    auto Add(const InternalKey &key, std::string &value) -> void;

    auto Builder() -> std::unique_ptr<SSTable>;

    auto EstimatedSize() const { return estimated_size_; }

private:
    auto FlushBlock() -> void;

    file_number_t file_number_;

    std::ofstream file_;
    BlockBuilder block_builder_;

    std::vector<BlockMeta> block_meta_{};

    uint32_t estimated_size_{0};
    uint64_t offset_{0};
};

}  // namespace ljdb


