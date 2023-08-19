#include "db/db_options.h"

namespace LindormContest {

    auto NewDBOptions() -> DBOptions * {
        auto db_options = new DBOptions();
        db_options->block_cache_ = new Cache<Block>(1 << 30);
        db_options->table_cache_ = new TableCache(1024, nullptr);
        db_options->bg_task_ = new BackgroundTask();
        return db_options;
    }



} // namespace LindormContest