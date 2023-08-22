#pragma once

#include <memory>
#include "common/config.h"
#include "format.h"
#include "common/iterator.h"

namespace LindormContest {

const constexpr uint32_t FILE_META_DATA_SIZE = INTERNAL_KEY_SIZE * 2 + sizeof(uint64_t) + sizeof(uint32_t);

class FileMetaData {
public:
    explicit FileMetaData() = default;

    explicit FileMetaData(file_number_t file_number, InternalKey smallest, InternalKey largest, uint64_t file_size);

    explicit FileMetaData(const std::string &src);

    void EncodeTo(std::string* dst) const;

    auto GetFileSize() const -> uint64_t { return file_size_; }

    auto GetFileNumber() const -> file_number_t { return file_number_; }

    auto GetSmallest() const -> const InternalKey& { return smallest_; }

    auto GetLargest() const -> const InternalKey& { return largest_; }

    auto operator<(const FileMetaData& rhs) const -> bool {
        return smallest_ < rhs.smallest_;
    }

    file_number_t file_number_{};

    uint64_t file_size_{};

    InternalKey smallest_;
    InternalKey largest_;

    uint32_t allowed_seeks_{};
};

using FileMetaDataPtr = std::shared_ptr<FileMetaData>;



auto NewFileMetaDataIterator(const std::vector<FileMetaDataPtr>& files) -> std::unique_ptr<Iterator>;

auto GetFileIterator(void *arg, const std::string &file_value) -> std::unique_ptr<Iterator>;

}  // namespace LindormContest