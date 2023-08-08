#pragma once

#include <atomic>
#include <utility>
#include <algorithm>

#include "common/config.h"
#include "format.h"
#include "common/iterator.h"


namespace ljdb {

class FileMetaData {
public:
    explicit FileMetaData() {}

    explicit FileMetaData(file_number_t file_number, InternalKey smallest, InternalKey largest)
        : file_number_(file_number), smallest_(std::move(smallest)), largest_(std::move(largest)) {}

    explicit FileMetaData(const std::string &src);

    void DecodeFrom(const std::string& src);

    void EncodeTo(std::string* dst) const;

    auto GetFileSize() const -> uint64_t { return file_size_; }

    auto GetFileNumber() const -> file_number_t { return file_number_; }

    auto GetSmallest() const -> const InternalKey& { return smallest_; }

    auto GetLargest() const -> const InternalKey& { return largest_; }

    auto operator<(const FileMetaData& rhs) const -> bool {
        return smallest_ < rhs.smallest_;
    }

    file_number_t file_number_;

    uint64_t file_size_;

    InternalKey smallest_;
    InternalKey largest_;

    uint32_t allowed_seeks_;
};

struct TableMetaData {
public:
    auto GetFileMetaData(int32_t level) const -> const std::vector<FileMetaData*>& {
        return files_[level];
    }

    auto NextFileNumber() -> file_number_t {
        return next_file_number_.fetch_add(1);
    }

    void AddFileMetaData(int32_t level, const std::vector<FileMetaData*> &file) {
        files_[level].insert(files_[level].end(), file.begin(), file.end());
        std::sort(files_[level].begin(), files_[level].end());
    }

    // 返回 level 层与 [begin, end] 存在重叠的文件
    void GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs);

    // 返回 level 层的文件总大小
    auto TotalFileSize(int level) -> uint64_t;

private:

    int32_t table_number_;
    std::atomic_int32_t next_file_number_;

    std::vector<FileMetaData*> files_[K_NUM_LEVELS];
};


auto NewFileMetaDataIterator(const std::vector<FileMetaData *> &files) -> std::unique_ptr<Iterator>;

}; // namespace ljdb