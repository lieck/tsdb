#include <memory>
#include "db/meta_data.h"


namespace ljdb {




auto TableMetaData::TotalFileSize(int level) -> uint64_t {
    uint64_t total_file_size = 0;
    for(auto& file : files_[level]) {
        total_file_size += file->GetFileSize();
    }
    return total_file_size;
}

void TableMetaData::GetOverlappingInputs(int level, const InternalKey *begin, const InternalKey *end,
                                         std::vector<FileMetaData *> *inputs) {
    for(auto file : files_[level]) {
        if(file->GetSmallest() > *end && *begin > file->GetLargest()) {
            continue;
        }
        inputs->push_back(file);
    }

}

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

}; // namespace ljdb