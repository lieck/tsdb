#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <fstream>

#include "sstable/block.h"
#include "disk/disk_manager.h"
#include "common/macros.h"

namespace ljdb {

const constexpr size_t SSTABLE_FOOTER_LENGTH = CodingUtil::LENGTH_SIZE;

class SSTable {
public:
    SSTable(file_number_t file_number, uint64_t file_size);

    SSTable(file_number_t file_number, uint64_t file_size, Block* index_block)
        : file_number_(file_number), file_size_(file_size), index_block_(index_block) {}

    DISALLOW_COPY_AND_MOVE(SSTable);

    auto NewIterator() -> std::unique_ptr<Iterator>;

    auto GetFileNumber() const -> file_number_t { return file_number_; }

    auto GetFileSize() const -> uint64_t { return file_size_; }

    static auto ReadBlock(void* arg, const std::string &key) -> std::unique_ptr<Iterator>;

    auto GetBlockCacheID(block_id_t block_id) -> cache_id_t;

private:
    file_number_t file_number_; // sstable 编号
    uint64_t file_size_; // sstable 大小

    // index block
    Block* index_block_{nullptr};
};

}  // namespace ljdb

