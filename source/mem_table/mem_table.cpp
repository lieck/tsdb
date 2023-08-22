#include <memory>
#include "mem_table/mem_table.h"

namespace LindormContest {


auto MemTable::Insert(const Row& row) -> bool {
    approximate_size_ += INTERNAL_KEY_SIZE;

    // 计算 value length
    uint32_t value_len = 0;
    for(auto &col : row.columns) {
        if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
            value_len += 8;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_INTEGER) {
            value_len += 4;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_STRING) {
            std::pair<int32_t, const char *> value;
            if(col.second.getStringValue(value) != 0) {
                return false;
            }
            value_len += 4 + value.first;
        }
    }

    ASSERT(value_len < UINT32_MAX, "value_len < UINT32_MAX");

    std::string str_value;
    str_value.resize(value_len);
    char *buffer = str_value.data();
    int32_t buffer_offset = 0;

    for(auto &col : row.columns) {
        if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
            memcpy(buffer + buffer_offset, col.second.columnData, 8);
            buffer_offset += 8;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_INTEGER) {
            memcpy(buffer + buffer_offset, col.second.columnData, 4);
            buffer_offset += 4;
        } else if(col.second.getColumnType() == LindormContest::COLUMN_TYPE_STRING) {
            std::pair<int32_t, const char *> value;
            if(col.second.getStringValue(value) != 0) {
                return false;
            }

            memcpy(buffer + buffer_offset, &value.first, 4);
            buffer_offset += 4;

            memcpy(buffer + buffer_offset, value.second, value.first);
            buffer_offset += value.first;
        } else {
            return false;
        }
    }

    data_.emplace(InternalKey(row.vin, row.timestamp), str_value);
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




}  // namespace LindormContest