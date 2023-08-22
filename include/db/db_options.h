#pragma once

#include <atomic>
#include "background.h"
#include "cache/table_cache.h"

namespace LindormContest {

struct DBOptions {

    auto NextFileNumber() -> file_number_t {
        return next_file_number_.fetch_add(1);
    }

    TableCache *table_cache_;
    Cache<Block> *block_cache_;

    BackgroundTask *bg_task_;

    std::atomic<int32_t> next_file_number_{0};
};

auto NewDBOptions() -> DBOptions *;

}  // namespace LindormContest