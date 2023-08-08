

#include <memory>
#include "common/config.h"
#include "format.h"
#include "common/iterator.h"
#include "cache/table_cache.h"

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


static auto GetFileIterator(void *arg, const std::string &file_value) -> std::unique_ptr<Iterator> {
    auto table_cache = reinterpret_cast<TableCache*>(arg);
    FileMetaData file_meta_data(file_value);
    auto sstable = table_cache->OpenSSTable(file_meta_data.GetFileNumber());
    return sstable->NewIterator();
}

auto NewFileMetaDataIterator(const std::vector<FileMetaData *> &files) -> std::unique_ptr<Iterator>;


} // namespace ljdb