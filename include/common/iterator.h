#pragma once

#include <string>
#include "common/macros.h"
#include "db/format.h"

namespace ljdb {

class Iterator {
public:
    Iterator() = default;

    DISALLOW_COPY_AND_MOVE(Iterator);

    virtual ~Iterator() {
        if (deleter_ != nullptr) {
            (*deleter_)(deleter_arg_, deleter_value_);
        }
    }

    virtual auto SeekToFirst() -> void = 0;

    virtual void Seek(const InternalKey &key) = 0;

    virtual auto GetKey() -> InternalKey = 0;

    virtual auto GetValue() -> std::string = 0;

    virtual auto Valid() -> bool = 0;

    virtual auto Next() -> void = 0;

    void RegisterCleanup(void (*deleter)(void*, void*), void *arg, void *value) {
        deleter_ = deleter;
        deleter_arg_ = arg;
        deleter_value_ = value;
    }

private:

    // iterator 生命周期结束时，调用 deleter_ 释放资源
    // iterator 不释放 deleter_value_ 的内存资源
    void (*deleter_)(void*, void*){nullptr};
    void *deleter_arg_{nullptr};
    void *deleter_value_{nullptr};
};

} // namespace ljdb