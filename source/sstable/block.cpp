
#include <cstring>
#include <algorithm>

#include "util/coding.h"
#include "sstable/block.h"

namespace ljdb {

auto Block::NewIterator() -> std::unique_ptr<Iterator> {
    return std::make_unique<BlockIterator>(data_, size_, num_entries_);
}

Block::Block(const char *data, uint32_t size) : data_(data), size_(size) {
    auto offset = size_ - CodingUtil::LENGTH_SIZE;
    num_entries_ = CodingUtil::DecodeUint32(data_ + offset);
}

Block::BlockIterator::BlockIterator(const char *data, uint32_t size, uint32_t num_entries)
    : data_(data), num_entries_(num_entries) {
    entry_array_ = reinterpret_cast<const uint32_t*>(data + size - CodingUtil::LENGTH_SIZE * (num_entries_ + 1));
    curr_idx_ = 0;
}

void Block::BlockIterator::SeekToFirst() {
    curr_idx_ = 0;
}

void Block::BlockIterator::Seek(const InternalKey &key) {
    auto cmp = [this](uint32_t offset, const InternalKey &key) -> bool {
        size_t key_size = CodingUtil::DecodeUint32(data_ + offset);
        offset += CodingUtil::LENGTH_SIZE;

        InternalKey a(data_ + offset);

        return a < key;
    };

    uint32_t l = 0;
    uint32_t r = num_entries_ - 1;
    while(l <= r) {
        uint32_t mid = (l + r) >> 1;
        if(cmp(entry_array_[mid * CodingUtil::LENGTH_SIZE], key)) {
            l = mid + 1;
        } else {
            r = mid - 1;
        }
    }

    curr_idx_ = l;
}

auto Block::BlockIterator::GetKey() -> InternalKey {
    auto offset = entry_array_[curr_idx_];
    return InternalKey(data_ + offset);
}

auto Block::BlockIterator::GetValue() -> std::string {
    auto offset = entry_array_[curr_idx_] + INTERNAL_KEY_SIZE;

    auto value_size = CodingUtil::DecodeUint32(data_ + offset);
    offset += CodingUtil::LENGTH_SIZE;
    return {data_ + offset, value_size};
}

auto Block::BlockIterator::Valid() -> bool {
    return curr_idx_ < num_entries_;
}

auto Block::BlockIterator::Next() -> void {
    curr_idx_++;
}


}  // namespace ljdb