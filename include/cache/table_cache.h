#pragma once


#include "sstable/sstable.h"

namespace ljdb {

// TODO 暂未实现 LRU
class TableCache {
public:
    auto OpenSSTable(file_number_t file_number) -> SSTable*;

    auto CloseSSTable(file_number_t file_number) -> void;

private:
    std::vector<SSTable*> tables_;

};



} // namespace ljdb