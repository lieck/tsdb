#pragma once

#include <cstdint>

namespace LindormContest {

//#define DEBUG_MODE
//#define DEBUG_MODE_CLOSE_COMPRESSION


static constexpr uint32_t SSTABLE_BLOCK_CAPACITY = 4096;

static constexpr int64_t MAX_TIMESTAMP = INT64_MAX;


// sstable
static constexpr int K_NUM_LEVELS = 7;

// memtable 的写入缓存区
#ifdef DEBUG_MODE
static constexpr int K_MEM_TABLE_SIZE_THRESHOLD = 20 * 1024;
#else
static constexpr int K_MEM_TABLE_SIZE_THRESHOLD = 4 * 1024 * 1024;
#endif

// sstable file 的最大大小
#ifdef DEBUG_MODE
static constexpr uint64_t MAX_FILE_SIZE = 40 * 1024;
#else
static constexpr uint64_t MAX_FILE_SIZE = 6 * 1024 * 1024;
#endif

// l0 的压缩触发阈值
static constexpr int K_L0_COMPACTION_TRIGGER = 4;



using block_id_t = uint32_t;
using file_number_t = uint32_t;
using cache_id_t = uint64_t;


} // namespace LindormContest

