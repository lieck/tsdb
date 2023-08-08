#include <memory>
#include "db/compaction.h"
#include "db/meta_data.h"
#include "sstable/sstable.h"
#include "db/merger_iterator.h"
#include "db/two_level_iterator.h"
#include "sstable/sstable_builder.h"


namespace ljdb {

static auto GetFileIterator(void *arg, const std::string &file_value) -> std::unique_ptr<Iterator> {
    auto table_cache = reinterpret_cast<TableCache*>(arg);
    FileMetaData file_meta_data(file_value);
    auto sstable = table_cache->OpenSSTable(file_meta_data.GetFileNumber());
    return sstable->NewIterator();
}

void Compaction::Finalize() {
    int best_level = -1;
    double best_score = -1;

    // l0 compaction
    best_level = 0;
    best_score = static_cast<double>(table_meta_data_->GetFileMetaData(0).size()) /
            static_cast<double>(K_L0_COMPACTION_TRIGGER);

    // l1 ~ l6 compaction
    for(int level = 1; level < K_NUM_LEVELS - 1; level++) {
        double score = static_cast<double>(table_meta_data_->TotalFileSize(level)) /
                MaxBytesForLevel(level);
        if(score > best_score) {
            best_score = score;
            best_level = level;
        }
    }

    if(size_compaction_score_ < best_score) {
        size_compaction_level_ = best_level;
        size_compaction_score_ = best_score;
    }
}

void Compaction::SeekCompaction(int level, FileMetaData *meta_data) {
    if(seek_compaction_level_ != -1) {
        return;
    }

    seek_compaction_level_ = level;
    seek_compaction_file_ = meta_data;
}

auto Compaction::GenerateCompactionTask() -> CompactionTask* {
    const bool size_compaction = size_compaction_score_ >= 1;
    const bool seek_compaction = seek_compaction_level_ != -1;

    auto task = new CompactionTask();

    auto &files = table_meta_data_->GetFileMetaData(size_compaction_level_);

    // 确定压缩文件的开始位置
    size_t start_idx = 0;
    if(size_compaction) {
        task->level_ = size_compaction_level_;

        for(size_t i = 0; i < files.size(); i++) {
            if(files[i]->GetSmallest() > size_compaction_pointer_[size_compaction_level_]) {
                start_idx = i;
                break;
            }
        }
    } else if(seek_compaction) {
        task->level_ = seek_compaction_level_;

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
    table_meta_data_->GetOverlappingInputs(size_compaction_level_ + 1, &start_key, &end_key, &task->input_files_[1]);

    return task;
}

void Compaction::DoManualCompaction(CompactionTask *task) {
    std::vector<std::unique_ptr<Iterator>> input_iters(2);

    // 生成迭代器
    for(size_t i = 0; i < 2; i++) {
        if(task->level_ == 0 && i == 0) {
            std::vector<std::unique_ptr<Iterator>> iters;
            for(auto &file : task->input_files_[i]) {
                auto table = table_cache_->OpenSSTable(file->GetFileNumber());
                iters.emplace_back(table->NewIterator());
            }
            input_iters[0] = NewMergingIterator(iters);
        } else {
            auto file_iter = NewFileMetaDataIterator(task->input_files_[i]);
            input_iters[i] = NewTwoLevelIterator(std::move(file_iter), GetFileIterator, table_cache_);
        }
    }

    auto input_iter = NewMergingIterator(input_iters);
    if(input_iter == nullptr) {
        return;
    }
    input_iter->SeekToFirst();

    SStableBuilder *builder = nullptr;
    FileMetaData *file_meta_data = nullptr;
    InternalKey largest;

    while(input_iter->Valid()) {
        if(builder == nullptr || builder->EstimatedSize() >= MAX_FILE_SIZE) {
            if(builder != nullptr) {
                builder->Builder();

                file_meta_data->largest_ = largest;
                file_meta_data->file_size_ = builder->EstimatedSize();
                task->output_files_.emplace_back(file_meta_data);

                delete builder;
            }

            auto file_number = table_meta_data_->NextFileNumber();
            builder = new SStableBuilder(file_number);
            file_meta_data = new FileMetaData();
            file_meta_data->file_number_ = file_number;
            file_meta_data->smallest_ = input_iter->GetKey();
        }

        largest = input_iter->GetKey();
        auto value = input_iter->GetValue();
        builder->Add(largest, value);

        input_iter->Next();
    }

    if(builder != nullptr) {
        builder->Builder();

        file_meta_data->largest_ = largest;
        file_meta_data->file_size_ = builder->EstimatedSize();
        task->output_files_.emplace_back(file_meta_data);

        delete builder;
    }
}

auto Compaction::CompactMemTable(const std::shared_ptr<MemTable>& mem) -> FileMetaData * {
    auto file_number = table_meta_data_->NextFileNumber();

    SStableBuilder builder(file_number);
    auto iter = mem->NewIterator();
    iter->SeekToFirst();

    InternalKey start_key = iter->GetKey();
    InternalKey end_key;

    while(iter->Valid()) {
        end_key = iter->GetKey();
        std::string value = iter->GetValue();
        builder.Add(iter->GetKey(), value);
        iter->Next();
    }

    auto sstable = builder.Builder();

    auto file_meta_data = new FileMetaData(file_number, start_key, end_key);
    return file_meta_data;
}



} // namespace ljdb
