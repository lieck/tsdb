#include "cache/table_cache.h"
#include "cache/cache.h"

namespace ljdb {


auto TableCache::OpenSSTable(file_number_t file_number) -> SSTable * {

}

auto TableCache::CloseSSTable(file_number_t file_number) -> void {
}

auto TableCache::NewTableIterator(file_number_t file_number) -> std::unique_ptr<Iterator> {
    return {};
}


} // namespace ljdb