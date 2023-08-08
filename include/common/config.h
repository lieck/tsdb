#pragma once

#include <cstdint>

namespace ljdb {


static constexpr uint32_t SSTABLE_BLOCK_CAPACITY = 4096;

static constexpr int64_t MAX_TIMESTAMP = INT64_MAX;


// sstable
static constexpr int K_NUM_LEVELS = 7;
static constexpr int K_MEM_TABLE_SIZE_THRESHOLD = 20 * 1024 * 1024;

// sstable file 的最大大小
static constexpr uint64_t MAX_FILE_SIZE = 4 * 1024 * 1024;

// l0 的压缩触发阈值
static constexpr int K_L0_COMPACTION_TRIGGER = 4;



using block_id_t = int64_t;
using file_number_t = int32_t;


}; // namespace ljdb

