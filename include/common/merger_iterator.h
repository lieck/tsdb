#pragma once

#include <memory>
#include "common/iterator.h"

namespace LindormContest {

auto NewMergingIterator(const std::vector<std::unique_ptr<Iterator>> &children) -> std::unique_ptr<Iterator>;

}; // namespace ljdb
