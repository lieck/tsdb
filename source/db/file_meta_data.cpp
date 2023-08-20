

#include <utility>
#include <algorithm>
#include "cache/table_cache.h"
#include "db/file_meta_data.h"

namespace LindormContest {

FileMetaData::FileMetaData(file_number_t file_number, InternalKey smallest, InternalKey largest,
                           uint64_t file_size): file_number_(file_number), smallest_(std::move(smallest)),
                           largest_(std::move(largest)), file_size_(file_size) {
    // 简单的复制 leveldb
    allowed_seeks_ = static_cast<uint32_t>(file_size_ / 16384U);
    if(allowed_seeks_ < 100) {
        allowed_seeks_ = 100;
    }
}

FileMetaData::FileMetaData(const std::string &src) {
    file_number_ = CodingUtil::DecodeFixed64(src.data());
    file_size_ = CodingUtil::DecodeFixed64(src.data() + 4);
    smallest_ = InternalKey(src.substr(12, INTERNAL_KEY_SIZE));
    largest_ = InternalKey(src.substr(12 + INTERNAL_KEY_SIZE, INTERNAL_KEY_SIZE));
    allowed_seeks_ = static_cast<uint32_t>(file_size_ / 16384U);
}

void FileMetaData::EncodeTo(std::string *dst) const {
    char buffer[INTERNAL_KEY_SIZE];
    CodingUtil::EncodeValue(buffer, file_number_);
    dst->append(buffer, sizeof(file_number_));

    CodingUtil::EncodeValue(buffer, file_size_);
    dst->append(buffer, sizeof(file_size_));

    dst->append(smallest_.Encode());
    dst->append(largest_.Encode());
}

class FileMetaDataIterator : public Iterator {
public:
    explicit FileMetaDataIterator(std::vector<std::shared_ptr<FileMetaData>> files) : files_(std::move(files)) {
        std::sort(files_.begin(), files_.end(), [](const auto &a, const auto &b) {
            return a->GetSmallest() < b->GetSmallest();
        });
    }

    auto SeekToFirst() -> void override {
        index_ = 0;
    }

    void Seek(const InternalKey &key) override {
        index_ = 0;
        while(index_ < files_.size() && files_[index_]->GetLargest() < key) {
            index_++;
        }
    }

    auto GetKey() -> InternalKey override {
        return InternalKey();
    }

    auto GetValue() -> std::string override {
        ASSERT(index_ < files_.size(), "index_ is out of range");
        std::string ret;
        files_[index_]->EncodeTo(&ret);
        return ret;
    }

    auto Valid() -> bool override {
        return index_ < files_.size();
    }

    auto Next() -> void override {
        index_++;
    }

private:
    std::vector<FileMetaDataPtr> files_;
    size_t index_{0};
};

auto NewFileMetaDataIterator(const std::vector<FileMetaDataPtr>& files) -> std::unique_ptr<Iterator> {
    return std::make_unique<FileMetaDataIterator>(files);
}

auto GetFileIterator(void *arg, const std::string &file_value) -> std::unique_ptr<Iterator> {
    auto table_cache = reinterpret_cast<TableCache*>(arg);
    auto file_meta_data = std::make_shared<FileMetaData>(file_value);
    return table_cache->NewTableIterator(file_meta_data);
}


}  // namespace LindormContest