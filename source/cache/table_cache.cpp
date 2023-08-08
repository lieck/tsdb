#include "cache/table_cache.h"

namespace ljdb {


auto TableCache::OpenSSTable(file_number_t file_number) -> SSTable * {
    for(auto table : tables_) {
        if (table->GetFileNumber() == file_number) {
            return table;
        }
    }

    auto sstable = new SSTable(file_number);
    tables_.push_back(sstable);
    return sstable;
}

auto TableCache::CloseSSTable(file_number_t file_number) -> void {
}


} // namespace ljdb