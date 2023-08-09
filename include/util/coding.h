
#pragma once

#include <cstdint>
#include <string>
#include <cmath>
#include "struct/Schema.h"
#include "struct/Row.h"

namespace ljdb {



class CodingUtil {
public:
    static constexpr uint32_t LENGTH_SIZE = 4;


    static auto PutUint32(char* data, uint32_t value) -> void {
        auto *p = reinterpret_cast<uint32_t*>(data);
        *p = value;
//
//        data[0] = static_cast<char>((value >> 24) & 0xff);
//        data[1] = static_cast<char>((value >> 16) & 0xff);
//        data[2] = static_cast<char>((value >> 8) & 0xff);
//        data[3] = static_cast<char>(value & 0xff);
    }

    static auto DecodeUint32(const char* data) -> uint32_t {
        return *reinterpret_cast<const int32_t*>(data);

//        uint32_t value = 0;
//        for(size_t i = 0; i < 4; i++) {
//            value <<= 8;
//            value += static_cast<uint8_t>(data[i]);
//        }
//        return value;
    }

    static auto PutInt64(char* data, int64_t value) -> void {
        auto *p = reinterpret_cast<int64_t *>(data);
        *p = value;
    }

    static auto DecodeInt64(const char* data) -> int64_t {
        return *reinterpret_cast<const int64_t*>(data);
    }

    static auto DecodeInteger(const char* data) -> int32_t {
        return *reinterpret_cast<const int32_t*>(data);
    }

    static auto DecodeDoubleFloat(const char* data) -> double_t {
        return *reinterpret_cast<const double_t*>(data);
    }

    static auto DecodeRow(const char *data, LindormContest::Schema &schema, const std::set<std::string> *columns) -> LindormContest::Row {
        LindormContest::Row row;
        auto col_iter = columns->begin();

        for(auto &col : schema.columnTypeMap) {
            bool put = col.first == *col_iter;
            if(put) {
                col_iter++;
            }

            if(col.second == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                auto value = CodingUtil::DecodeDoubleFloat(data);
                if(put) {
                    row.columns.insert(std::make_pair(*col_iter, value));
                }
                data += 8;
            } else if(col.second == LindormContest::COLUMN_TYPE_INTEGER) {
                auto value = CodingUtil::DecodeInteger(data);
                if(put) {
                    row.columns.insert(std::make_pair(*col_iter, value));
                }
                data += 4;
            } else if(col.second == LindormContest::COLUMN_TYPE_STRING) {
                auto len = CodingUtil::DecodeInteger(data);
                data += 4;
                if(put) {
                    row.columns.insert(std::make_pair(*col_iter, std::string(data, len)));
                }
                data += len;
            }
        }

        return std::move(row);
    }


    static auto DecodeFixed64(const char* data) -> uint64_t {
        uint64_t value = 0;
        for(size_t i = 0; i < 8; i++) {
            value <<= 8;
            value += static_cast<uint8_t>(data[i]);
        }
        return value;
    }

    static auto EncodeFixed64(std::string* dst, uint64_t value) -> void {
        for(size_t i = 0; i < 8; i++) {
            dst->push_back(static_cast<char>(value & 0xff));
            value >>= 8;
        }
    }

};

} // namespace ljdb
