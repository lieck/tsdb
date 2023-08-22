#pragma once

#include "util/coding.h"


namespace LindormContest {

static const int32_t INTERNAL_KEY_SIZE = LindormContest::VIN_LENGTH + 8;

struct InternalKey {
    explicit InternalKey() = default;

    explicit InternalKey(const Vin &vin, int64_t timestamp) : vin_(vin), timestamp_(timestamp) {}

    explicit InternalKey(const char *data)  {
        memcpy(vin_.vin, data, LindormContest::VIN_LENGTH);
        timestamp_ = CodingUtil::DecodeInt64(data + LindormContest::VIN_LENGTH);
    }

    explicit InternalKey(const std::string &s) {
        s.substr(0, LindormContest::VIN_LENGTH).copy(vin_.vin, LindormContest::VIN_LENGTH);
        timestamp_ = CodingUtil::DecodeInt64(s.data() + LindormContest::VIN_LENGTH);
    }

    auto operator==(const InternalKey &rhs) const -> bool {
        return vin_ == rhs.vin_ && timestamp_ == rhs.timestamp_;
    }

    auto operator!=(const InternalKey &rhs) const -> bool {
        return rhs.vin_ != vin_ || rhs.timestamp_ != timestamp_;
    }

    auto operator<(const InternalKey &rhs) const -> bool {
        if (vin_ == rhs.vin_) {
            return timestamp_ > rhs.timestamp_;
        }
        return vin_ < rhs.vin_;
    }

    auto operator>(const InternalKey &rhs) const -> bool {
        if (vin_ == rhs.vin_) {
            return timestamp_ < rhs.timestamp_;
        }
        return vin_ > rhs.vin_;
    }

    auto Encode() const -> std::string {
        std::string s;
        s.append(vin_.vin);
        s.resize(LindormContest::VIN_LENGTH + 8);
        CodingUtil::PutInt64(const_cast<char *>(s.data() + LindormContest::VIN_LENGTH), timestamp_);
        return s;
    }

    Vin vin_;
    int64_t timestamp_{};
};




}; // namespace ljdb
