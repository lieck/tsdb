#include "db/file_meta_data.h"

namespace ljdb {

class FileMetaDataIterator : public Iterator {
public:
    explicit FileMetaDataIterator(const std::vector<FileMetaData *> &files) : files_(files) {}

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
        return files_[index_]->GetLargest().Encode();
    }

    auto Valid() -> bool override {
        return index_ < files_.size();
    }

    auto Next() -> void override {
        index_++;
    }

private:
    std::vector<FileMetaData *> files_;
    size_t index_{0};
};

auto NewFileMetaDataIterator(const std::vector<FileMetaData *> &files) -> std::unique_ptr<Iterator> {
    return std::make_unique<FileMetaDataIterator>(files);
}



} // namespace ljdb