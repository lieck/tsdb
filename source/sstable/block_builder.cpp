#pragma once

#include <iostream>
#include <cstring>
#include <memory>
#include "sstable/block_builder.h"
#include "common/macros.h"


namespace ljdb {

BlockBuilder::~BlockBuilder() {
    delete data_;
}

auto BlockBuilder::Add(const InternalKey &key, std::string_view value) -> bool {
    if(data_ == nullptr) {
        return false;
    }

    uint32_t size = INTERNAL_KEY_LENGTH + value.size() + ENTRY_LENGTH_SIZE;

    if(curr_size_ + size > capacity_) {
        return false;
    }
    entry_offsets_.push_back(offset_);

    // 将 key 写入 data_ 中
    std::memcpy(data_ + offset_, key.Encode().data(), INTERNAL_KEY_LENGTH);
    offset_ += INTERNAL_KEY_LENGTH;

    // 将 value_size 和 value 写入 data_ 中
    CodingUtil::PutUint32(data_ + offset_, value.size());
    offset_ += CodingUtil::LENGTH_SIZE;
    std::memcpy(data_ + offset_, value.data(), value.size());
    offset_ += value.size();

    curr_size_ += size;
    return true;
}

auto BlockBuilder::Builder() -> std::unique_ptr<Block> {
    for(auto offset : entry_offsets_) {
        CodingUtil::PutUint32(data_ + offset_, offset);
        offset_ += CodingUtil::LENGTH_SIZE;
    }

    CodingUtil::PutUint32(data_ + offset_, entry_offsets_.size());
    offset_ += CodingUtil::LENGTH_SIZE;

    auto block = std::make_unique<Block>(data_, offset_);
    data_ = nullptr;

    return block;
}

auto BlockBuilder::Init() -> void {
    data_ = new char[capacity_];
    offset_ = 0;
    curr_size_ = INITIAL_SIZE;
    entry_offsets_ = {};
}

}  // namespace ljdb
