#pragma once


#include <cstdint>
#include <string_view>
#include <vector>
#include <memory>

#include "sstable/block.h"
#include "common/config.h"
#include "common/macros.h"

namespace LindormContest {

// block format:
// entry : | key | value length | value data|
//
// Block : | entry  | entry    | ... | entry |      | entry offset array | num entries|
class BlockBuilder {
public:
    static constexpr uint32_t INITIAL_SIZE = 4;
    static constexpr uint32_t ENTRY_LENGTH_SIZE = 4 + 4 + 4;

    explicit BlockBuilder(uint32_t capacity) : capacity_(capacity) {}
    DISALLOW_COPY_AND_MOVE(BlockBuilder);

    ~BlockBuilder();

    auto Init() -> void;

    auto Add(const InternalKey &key, std::string_view value) -> bool;

    auto EstimatedSize() const -> uint32_t {return curr_size_;}

    auto Builder() -> std::unique_ptr<Block>;

    auto Empty() -> bool {return entry_offsets_.empty(); }

private:
    char* data_{nullptr};
    uint32_t capacity_;
    uint32_t offset_{};
    uint32_t curr_size_{INITIAL_SIZE};
    std::vector<uint32_t> entry_offsets_{};
};

}  // namespace ljdb


