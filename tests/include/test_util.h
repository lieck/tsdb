#pragma once

namespace ljdb {


auto generate_key(int32_t key) -> InternalKey {
    InternalKey ret;
    ret.timestamp_ = 0;
    for(auto i = 0; i < LindormContest::VIN_LENGTH; ++i) {
        ret.vin_.vin[i] = 'x';
    }

    auto k = "key-" + std::to_string(key);
    k.copy(ret.vin_.vin, k.size());
    return ret;
}

}; // namespace ljdb