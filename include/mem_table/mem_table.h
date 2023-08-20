#pragma once

#include <mutex>
#include "common/macros.h"
#include "sklip_list.h"
#include "common/iterator.h"

namespace LindormContest {

class MemTable {
private:
    class MemTableIterator;

public:
    explicit MemTable() = default;

    DISALLOW_COPY_AND_MOVE(MemTable);

    auto Insert(const Row& row) -> bool;

    auto ApproximateSize() const -> size_t { return approximate_size_; }

    auto Clear() -> void;

    auto Empty() const -> bool { return approximate_size_ == 0; }

    auto NewIterator() -> std::unique_ptr<Iterator>;

    auto Lock() -> void { mutex_.lock(); }

    auto Unlock() -> void { mutex_.unlock(); }

private:
    std::mutex mutex_;

    std::map<InternalKey, std::string> data_;
    size_t approximate_size_ = 0;
};

class MemTable::MemTableIterator : public Iterator {
public:
    explicit MemTableIterator(MemTable *mem_table) : mem_table_(mem_table), iter_(mem_table->data_.begin()) {};

    ~MemTableIterator() override = default;

    auto SeekToFirst() -> void override;

    auto Seek(const InternalKey &key) -> void override;

    auto GetKey() -> InternalKey override;

    auto GetValue() -> std::string override;

    auto Valid() -> bool override;

    auto Next() -> void override;

private:
    MemTable *mem_table_;
    std::map<InternalKey, std::string>::iterator iter_;
};

}  // namespace LindormContest