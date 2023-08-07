#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "db/iterator.h"
#include "db/format.h"


namespace ljdb {

struct BlockMeta {
    uint32_t offset_;
    std::string first_key_;

    BlockMeta(uint32_t offset_, std::string first_key) : offset_(offset_), first_key_(std::move(first_key)) {}
};

class Block {
private:
    class BlockIterator;

public:
    explicit Block(const char* data, uint32_t size);

    ~Block() { delete data_; }

    auto NewIterator() -> std::unique_ptr<Iterator>;

    auto GetData() const -> const char* { return data_; }

    auto GetDataSize() const -> uint32_t { return size_; }

    auto GetEntrySize() const -> uint32_t { return num_entries_; }

private:
    const char *data_;
    const uint32_t size_;
    
    uint32_t num_entries_;
};


class Block::BlockIterator : public Iterator {
public:
    explicit BlockIterator(const char *data, uint32_t size, uint32_t num_entries);

    ~BlockIterator() override = default;

    auto SeekToFirst() -> void override;

    void Seek(const InternalKey &key) override;

    auto GetKey() -> InternalKey override;

    auto GetValue() -> std::string override;

    auto Valid() -> bool override;

    auto Next() -> void override;

private:
    const char *data_;

    // Offset of entry array
    const uint32_t *entry_array_;

    uint32_t num_entries_;

    uint32_t curr_idx_{0};
};


}  // namespace ljdb

