#include "db/meta_data.h"



namespace ljdb {


auto TableMetaData::TotalFileSize(int level) -> uint64_t {
    uint64_t total_file_size = 0;
    for(auto& file : files_[level]) {
        total_file_size += file.GetFileSize();
    }
    return total_file_size;
}

void TableMetaData::GetOverlappingInputs(int level, const InternalKey *begin, const InternalKey *end,
                                         std::vector<FileMetaData *> *inputs) {
    auto iter = files_[level].lower_bound(FileMetaData(0, *begin, InternalKey()));
    while(iter != files_[level].end()) {
        auto& file = *iter;
        if(*end < file.GetSmallest() || file.GetLargest() > *begin) {
            continue;
        }
        inputs->push_back(const_cast<FileMetaData*>(&file));
        ++iter;
    }

}


}; // namespace ljdb