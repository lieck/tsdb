#pragma once

#include <string>
#include "common/macros.h"
#include "db/format.h"

namespace ljdb {

class Iterator {
public:
    Iterator() = default;

    DISALLOW_COPY_AND_MOVE(Iterator);

    virtual ~Iterator() = default;

    virtual auto SeekToFirst() -> void = 0;

    virtual void Seek(const InternalKey &key) = 0;

    virtual auto GetKey() -> InternalKey = 0;

    virtual auto GetValue() -> std::string = 0;

    virtual auto Valid() -> bool = 0;

    virtual auto Next() -> void = 0;
};

} // namespace ljdb