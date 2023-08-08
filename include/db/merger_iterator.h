#pragma once

#include <memory>
#include "common/iterator.h"

namespace ljdb {

auto NewMergingIterator(const std::vector<std::unique_ptr<Iterator>> &children) -> std::unique_ptr<Iterator>;

}; // namespace ljdb
