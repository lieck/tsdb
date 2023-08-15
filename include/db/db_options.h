#pragma once

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

    std::atomic_int32_t next_file_number_;
};

auto NewDBOptions() -> DBOptions * {
    auto db_options = new DBOptions();
    db_options->table_cache_ = new TableCache(1024);
    db_options->block_cache_ = new Cache<Block>(1 << 30);
    db_options->bg_task_ = new BackgroundTask();
    db_options->next_file_number_ = 0;
    return db_options;
}

} // namespace ljdb