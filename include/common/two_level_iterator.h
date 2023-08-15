#pragma once

#include <memory>
#include "common/iterator.h"

namespace LindormContest {

using BlockFunction = std::unique_ptr<Iterator> (*)(void *, const std::string &);

auto NewTwoLevelIterator(std::unique_ptr<Iterator> index_iter, BlockFunction block_function, void *arg) -> std::unique_ptr<Iterator>;

}; // namespace ljdb