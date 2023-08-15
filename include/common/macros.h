#pragma once

#include <cassert>
#include <stdexcept>

namespace LindormContest {

#define ASSERT(expr, message) assert((expr) && (message))

#define ENSURE(expr, message) \
  if (!(expr)) {                     \
    throw std::logic_error(message); \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);


// SSTable name
static std::string SSTABLE_NAME = "ljdb_sstable";
#define GET_SSTABLE_NAME(file_number) (SSTABLE_NAME + "_" + std::to_string(file_number))

}; // end namespace ljdb