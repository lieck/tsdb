
#pragma once

#include <cstdint>
#include <string>
#include <cmath>
#include "struct/Schema.h"
#include "struct/Row.h"

namespace LindormContest {



class CodingUtil {
public:
    static constexpr uint32_t LENGTH_SIZE = 4;

    static constexpr uint32_t FIXED_32_SIZE = 4;
    static constexpr uint32_t FIXED_64_SIZE = 8;


    template<class T>
    static auto EncodeValue(char *data, T value) -> void {
        auto *p = reinterpret_cast<T *>(data);
        *p = value;
    }

    static auto DecodeFixed64(const char *data) -> uint64_t {
        return *reinterpret_cast<const uint64_t *>(data);
    }


    static auto PutUint32(char *data, uint32_t value) -> void {
        auto *p = reinterpret_cast<uint32_t *>(data);
        *p = value;
//
//        data[0] = static_cast<char>((value >> 24) & 0xff);
//        data[1] = static_cast<char>((value >> 16) & 0xff);
//        data[2] = static_cast<char>((value >> 8) & 0xff);
//        data[3] = static_cast<char>(value & 0xff);
    }

    static auto DecodeUint32(const char *data) -> uint32_t {
        return *reinterpret_cast<const int32_t *>(data);

//        uint32_t value = 0;
//        for(size_t i = 0; i < 4; i++) {
//            value <<= 8;
//            value += static_cast<uint8_t>(data[i]);
//        }
//        return value;
    }

    static auto PutInt64(char *data, int64_t value) -> void {
        auto *p = reinterpret_cast<int64_t *>(data);
        *p = value;
    }

    static auto DecodeInt64(const char *data) -> int64_t {
        return *reinterpret_cast<const int64_t *>(data);
    }

    static auto DecodeInteger(const char *data) -> int32_t {
        return *reinterpret_cast<const int32_t *>(data);
    }

    static auto DecodeDoubleFloat(const char *data) -> double_t {
        return *reinterpret_cast<const double_t *>(data);
    }

    static auto DecodeRow(const char *data, LindormContest::Schema &schema,
                          const std::set<std::string> *columns) -> LindormContest::Row {
        LindormContest::Row row;
        auto col_iter = columns->begin();

        for (auto &col: schema.columnTypeMap) {
            bool put = col.first == *col_iter;
            if (put) {
                col_iter++;
            }

            if (col.second == LindormContest::COLUMN_TYPE_DOUBLE_FLOAT) {
                auto value = CodingUtil::DecodeDoubleFloat(data);
                if (put) {
                    row.columns.insert(std::make_pair(col.first, value));
                }
                data += 8;
            } else if (col.second == LindormContest::COLUMN_TYPE_INTEGER) {
                auto value = CodingUtil::DecodeInteger(data);
                if (put) {
                    row.columns.insert(std::make_pair(col.first, value));
                }
                data += 4;
            } else if (col.second == LindormContest::COLUMN_TYPE_STRING) {
                auto len = CodingUtil::DecodeInteger(data);
                data += 4;
                if (put) {
                    row.columns.insert(std::make_pair(col.first, std::string(data, len)));
                }
                data += len;
            } else {
                throw std::runtime_error("unknown column type");
            }
        }

        return std::move(row);
    }

    static auto SchemaItemToBytes(const std::string &key, LindormContest::ColumnType value) -> std::string {
        std::string bytes;
        bytes.append(key);
        bytes += '\0';
        bytes.append(1, value);
        return std::move(bytes);
    }

    static auto SchemaToBytes(LindormContest::Schema schema) -> std::string {
        std::string bytes;
        for (auto &item: schema.columnTypeMap) {
            bytes.append(SchemaItemToBytes(item.first, item.second));
        }
        return std::move(bytes);
    }

    static auto BytesToSchema(const std::string &bytes) -> LindormContest::Schema {
        LindormContest::Schema schema;
        for(size_t idx = 0; idx < bytes.size(); ) {
            size_t pos = idx + 1;
            while(pos < bytes.size() && bytes[pos] != '\0') {
                pos++;
            }

            std::string key(bytes.substr(idx, pos - idx));
            idx = pos + 1;
            schema.columnTypeMap.insert(std::make_pair(key, static_cast<LindormContest::ColumnType>(bytes[idx])));
            idx++;
        }
        return std::move(schema);
    }
};

} // namespace ljdb