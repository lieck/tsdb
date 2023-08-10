

#include "background.h"
#include "cache/table_cache.h"

namespace ljdb {

struct DBOptions {
    std::atomic_int32_t next_file_number_;

    BackgroundTask *bg_task_;
    TableCache *table_cache_;
};


struct DBMetaData {
public:

    auto NextFileNumber() -> file_number_t {
        return next_file_number_.fetch_add(1);
    }

    auto GetBackgroundTask() -> BackgroundTask* {
        return bg_task_;
    }

    auto GetTableCache() -> TableCache* {
        return table_cache_;
    }


private:
    std::atomic_int32_t next_file_number_;

    BackgroundTask *bg_task_;
    TableCache *table_cache_;

};


} // namespace ljdb