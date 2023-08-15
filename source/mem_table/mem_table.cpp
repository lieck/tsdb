#include <memory>
#include "mem_table/mem_table.h"

namespace LindormContest {


auto MemTable::Insert(Row row) -> bool {
    std::string buffer;
    for(auto &col : row.columns) {
        if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
            buffer.resize(8);
            memcpy(buffer.data(), &col.second.columnData, 8);
            approximate_size_ += 8;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_INTEGER) {
            buffer.resize(4);
            memcpy(buffer.data(), &col.second.columnData, 4);
            approximate_size_ += 4;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_STRING) {
            std::pair<int32_t, const char *> value;
            if(col.second.getStringValue(value) != 0) {
                return false;
            }
            buffer.resize(4 + value.first);
            memcpy(buffer.data(), &value.first, 4);
            memcpy(buffer.data() + 4, value.second, value.first);
            approximate_size_ += 4 + value.first;
        } else {
            return false;
        }
    }
    data_.emplace(InternalKey(row.vin, row.timestamp), std::move(buffer));
    return true;
}

auto MemTable::Clear() -> void {
    data_.clear();
    approximate_size_ = 0;
}

auto MemTable::NewIterator() -> std::unique_ptr<Iterator> {
    return std::make_unique<MemTableIterator>(this);
}

void MemTable::MemTableIterator::SeekToFirst() {
    iter_ = mem_table_->data_.begin();
}

void MemTable::MemTableIterator::Seek(const InternalKey &key) {
    iter_ = mem_table_->data_.lower_bound(key);
}

auto MemTable::MemTableIterator::GetKey() -> InternalKey {
    return iter_->first;
}

auto MemTable::MemTableIterator::GetValue() -> std::string {
    return iter_->second;
}

auto MemTable::MemTableIterator::Valid() -> bool {
    return iter_ != mem_table_->data_.end();
}

void MemTable::MemTableIterator::Next() {
    ++iter_;
}




} // namespace ljdb