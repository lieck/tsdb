#include <memory>
#include <utility>
#include "db/table_meta_data.h"


namespace ljdb {

const constexpr int TABLE_META_DATA_MAGIC = 0x02341678;


auto TableMetaData::TotalFileSize(int level) -> uint64_t {
    uint64_t total_file_size = 0;
    for(auto& file : files_[level]) {
        total_file_size += file->GetFileSize();
    }
    return total_file_size;
}

void TableMetaData::GetOverlappingInputs(int level, const InternalKey *begin, const InternalKey *end,
                                         std::vector<FileMetaDataPtr> *inputs) {
    for(const auto& file : files_[level]) {
        if(file->GetSmallest() > *end && *begin > file->GetLargest()) {
            continue;
        }
        inputs->push_back(file);
    }

}

void TableMetaData::Finalize() {
    // l0 compaction
    int best_level = 0;
    double best_score = static_cast<double>(GetFileMetaData(0).size()) /
                 static_cast<double>(K_L0_COMPACTION_TRIGGER);

    // l1 ~ l6 compaction
    for(int level = 1; level < K_NUM_LEVELS - 1; level++) {
        double score = static_cast<double>(TotalFileSize(level)) /
                       MaxBytesForLevel(level);
        if(score > best_score) {
            best_score = score;
            best_level = level;
        }
    }

    size_compaction_level_ = best_level;
    size_compaction_score_ = best_score;
}

void TableMetaData::SeekCompaction(int level, FileMetaDataPtr meta_data) {
    if(seek_compaction_level_ != -1) {
        return;
    }

    seek_compaction_level_ = level;
    seek_compaction_file_ = std::move(meta_data);
}

auto TableMetaData::GenerateCompactionTask() -> CompactionTask* {
    const bool size_compaction = size_compaction_score_ >= 1;
    const bool seek_compaction = seek_compaction_level_ != -1;

    auto task = new CompactionTask();

    auto &files = GetFileMetaData(size_compaction_level_);

    // 确定压缩文件的开始位置
    size_t start_idx = 0;
    if(size_compaction) {
        task->level_ = size_compaction_level_;
        task->type_ = CompactionType::SizeCompaction;

        for(size_t i = 0; i < files.size(); i++) {
            if(files[i]->GetSmallest() > size_compaction_pointer_[size_compaction_level_]) {
                start_idx = i;
                break;
            }
        }
    } else if(seek_compaction) {
        task->level_ = seek_compaction_level_;
        task->type_ = CompactionType::SeekCompaction;

        for(size_t i = 0; i < files.size(); i++) {
            if(files[i]->GetFileNumber() == seek_compaction_file_->GetFileNumber()) {
                start_idx = i;
                break;
            }
        }
    } else {
        delete task;
        return nullptr;
    }

    // 确定压缩文件的结束位置
    uint64_t compaction_size = 0;
    for(size_t i = start_idx; i < files.size(); i++) {
        if(compaction_size + files[i]->GetFileSize() > MAX_FILE_SIZE) {
            break;
        }

        task->input_files_[0].emplace_back(files[i]);
        compaction_size += files[i]->GetFileSize();
    }

    if(task->input_files_[0].empty()) {
        delete task;
        return nullptr;
    }

    // 确定 level + 1 的文件
    auto start_key = task->input_files_[0][0]->GetSmallest();
    auto end_key = task->input_files_[0].back()->GetLargest();
    GetOverlappingInputs(size_compaction_level_ + 1, &start_key, &end_key, &task->input_files_[1]);

    return task;
}

void TableMetaData::AddFileMetaData(int32_t level, const std::vector<std::shared_ptr<FileMetaData>> &file) {
    files_[level].insert(files_[level].end(), file.begin(), file.end());
    std::sort(files_[level].begin(), files_[level].end());
}

void TableMetaData::RemoveFileMetaData(int32_t level, const std::vector<std::shared_ptr<FileMetaData>> &file) {
    for(auto &f : file) {
        auto it = std::find(files_[level].begin(), files_[level].end(), f);
        if(it != files_[level].end()) {
            files_[level].erase(it);
        }
    }
}

void TableMetaData::ClearCompaction() {
    seek_compaction_level_ = -1;
    seek_compaction_file_ = nullptr;

    size_compaction_level_ = -1;
    size_compaction_score_ = 0;
}

auto TableMetaData::WriteMetaData(std::ofstream &file) const -> void {
    char buffer[FILE_META_DATA_SIZE];

    file << TABLE_META_DATA_MAGIC;

    // table name
    CodingUtil::PutUint32(buffer, table_name_.size());
    file.write(buffer, CodingUtil::LENGTH_SIZE);
    file.write(table_name_.data(), static_cast<long long>(table_name_.size()));

    // file number
    for(const auto& level : files_) {
        CodingUtil::PutUint32(buffer, level.size());
        file.write(buffer, CodingUtil::LENGTH_SIZE);

        for(auto &file_meta_data : level) {
            std::string str;
            file_meta_data->EncodeTo(&str);
            file.write(buffer, FILE_META_DATA_SIZE);
        }
    }
}

auto TableMetaData::ReadMetaData(std::ifstream &file) const -> void {
    char buffer[FILE_META_DATA_SIZE];
    uint32_t magic;
    file >> magic;
    if(magic != TABLE_META_DATA_MAGIC) {
        throw std::runtime_error("table meta data magic error");
    }

    // table name
    uint32_t table_name_size;
    file.read(buffer, CodingUtil::LENGTH_SIZE);

    table_name_size = CodingUtil::DecodeUint32(buffer);
    file.read(buffer, table_name_size);

    table_name_ = std::string(buffer, table_name_size);

    // file number
    for(auto &level : files_) {
        uint32_t file_size;
        file.read(buffer, CodingUtil::LENGTH_SIZE);

        file_size = CodingUtil::DecodeUint32(buffer);

        for(uint32_t i = 0; i < file_size; i++) {
            file.read(buffer, FILE_META_DATA_SIZE);
            auto file_meta_data = new FileMetaData();
            file_meta_data->DecodeFrom(buffer);
            level.push_back(file_meta_data);
        }
    }
}


} // namespace ljdb