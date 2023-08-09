
#include <fstream>
#include "db/table.h"
#include "util/coding.h"
#include "common/config.h"
#include "disk/disk_manager.h"
#include "sstable/sstable_builder.h"
#include "db/background.h"
#include "common/merger_iterator.h"
#include "common/two_level_iterator.h"

namespace ljdb {




Table::Table(std::string &tableName, const Schema &schema, BackgroundTask *bgTask) : table_name_(tableName), schema_(schema), bg_task_(bgTask) {
}



auto Table::Upsert(const WriteRequest &wReq) -> int {
    std::unique_lock<std::mutex> lock(mutex_);

    // 若其他线程正在写入, 则等待
    write_queue_.push(&wReq);
    while(!write_queue_.empty() && write_queue_.front() != &wReq) {
        cv_.wait(lock);
    }
    write_queue_.pop();

    // 将数据写入 memtable
    for(auto &row : wReq.rows) {
        // 判断 mem 是否写满
        if(mem_->ApproximateSize() >= K_MEM_TABLE_SIZE_THRESHOLD) {
            imm_.push_back(mem_);
            mem_ = std::make_shared<MemTable>();

            MaybeScheduleCompaction();
        }

        mem_->Insert(row);
    }

    return 0;
}

auto Table::ExecuteLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int {

    return 0;
}

auto Table::ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
    auto query = RangeQueryRequest(&trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                   &trReadReq.requestedColumns, &trReadRes);


    return 0;
}


auto Table::MemTableQuery(Table::QueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
    auto vin_iter = req.vin_.begin();
    auto iter = mem->NewIterator();
    iter->Seek(InternalKey(*vin_iter, MAX_TIMESTAMP));

    while(iter->Valid() && vin_iter != req.vin_.end()) {
        if(iter->GetKey().vin_ == *vin_iter) {
            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
            row.vin = iter->GetKey().vin_;
            row.timestamp = iter->GetKey().timestamp_;
            req.result_->push_back(row);

            auto tmp = vin_iter;
            ++vin_iter;
            req.vin_.erase(tmp);
        }

        iter->Next();
    }
}

auto Table::MemTableRangeQuery(Table::RangeQueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
    auto iter = mem->NewIterator();
    iter->Seek(InternalKey(*req.vin_, req.time_upper_bound_));
    while(iter->Valid()) {
        auto key = iter->GetKey();
        if(key.vin_ != *req.vin_ || key.timestamp_ < req.time_lower_bound_) {
            break;
        }

        if(req.time_set_.count(key.timestamp_) == 0) {
            req.time_set_.insert(key.timestamp_);

            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
            row.vin = key.vin_;
            row.timestamp = key.timestamp_;
            req.result_->push_back(row);
        }

        iter->Next();
    }
}

auto Table::SSTableQuery(size_t level, Table::QueryRequest &req) -> void {


    // 搜索 L0
    if(level == 0) {
        // TODO 默认搜索全部 L0 文件, 但可以搜索指定的 L0 文件
        std::vector<FileMetaData*> l0_set = table_meta_data_.GetFileMetaData(0);
        if(l0_set.empty()) {
            return;
        }

    }
}


auto Table::FileTableQuery(FileMetaData *fileMetaData, Table::QueryRequest &req) -> void {
    auto iter = table_cache_->NewTableIterator(fileMetaData->file_number_);
    for(const auto& r : req.vin_) {
        iter->Seek(InternalKey(r, MAX_TIMESTAMP));
        if(iter->Valid()) {
            auto key = iter->GetKey();
            if(key.vin_ == r) {
                Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
                row.vin = key.vin_;
                row.timestamp = key.timestamp_;
                req.result_->push_back(row);
            }
            iter->Next();
        }
    }
}

void Table::FileTableRangeQuery(FileMetaData *fileMetaData, Table::RangeQueryRequest &req) {
    auto iter = table_cache_->NewTableIterator(fileMetaData->file_number_);
    iter->Seek(InternalKey(*req.vin_, req.time_upper_bound_));
    while(iter->Valid()) {
        auto key = iter->GetKey();
        if(key.vin_ != *req.vin_ || key.timestamp_ < req.time_lower_bound_) {
            break;
        }

        if(req.time_set_.count(key.timestamp_) == 0) {
            req.time_set_.insert(key.timestamp_);

            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, req.columns_);
            row.vin = key.vin_;
            row.timestamp = key.timestamp_;
            req.result_->push_back(row);
        }

        iter->Next();
    }
}

void Table::MaybeScheduleCompaction() {
    if(!imm_.empty() && table_meta_data_.ExistCompactionTask()) {
        db_meta_data_->GetBackgroundTask()->Schedule(&Table::BGWork, this);
    }
}

void Table::BGWork(void *table) {
    reinterpret_cast<Table*>(table)->BackgroundCall();
}

void Table::BackgroundCall() {
    std::unique_lock<std::mutex> lock(mutex_);
    BackgroundCompaction(lock);
    table_meta_data_.Finalize();

    MaybeScheduleCompaction();
}

auto Table::BackgroundCompaction(std::unique_lock<std::mutex> &lock) -> void {
    // 若有正在进行的压缩任务, 则等待
    while(is_compaction_running_) {
        lock.unlock();
        cv_.wait(lock);
        lock.lock();
    }

    // Minor Compaction
    if(!imm_.empty()) {
        auto compaction_mem = imm_[0];
        is_compaction_running_ = true;
        lock.unlock();

        // 开始压缩
        auto file_meta = CompactMemTable(compaction_mem);

        lock.lock();
        is_compaction_running_ = false;
        cv_.notify_all();

        // 更新元数据
        imm_.erase(imm_.begin());
        table_meta_data_.AddFileMetaData(0, {file_meta});
        return;
    }

    // Manual Compaction
    auto task = table_meta_data_.GenerateCompactionTask();
    if(task == nullptr) {
        return;
    }

    is_compaction_running_ = true;
    lock.unlock();

    auto result = DoManualCompaction(task);

    lock.lock();
    is_compaction_running_ = false;
    cv_.notify_all();

    if(result) {
        table_meta_data_.RemoveFileMetaData(task->level_, task->input_files_[0]);
        table_meta_data_.RemoveFileMetaData(task->level_ + 1, task->input_files_[1]);
        table_meta_data_.AddFileMetaData(task->level_ + 1, task->output_files_);

        if(task->type_ == CompactionType::SeekCompaction) {
            table_meta_data_.ClearSeekCompaction();
        }
    }
}

auto Table::DoManualCompaction(CompactionTask *task) -> bool {
    if(task->input_files_[1].empty()) {
        task->output_files_ = task->input_files_[1];
        return true;
    }

    std::vector<std::unique_ptr<Iterator>> input_iters(2);

    // 生成迭代器
    for(size_t i = 0; i < 2; i++) {
        if(task->level_ == 0 && i == 0) {
            std::vector<std::unique_ptr<Iterator>> iters;
            for(auto &file : task->input_files_[i]) {
                auto table = db_meta_data_->GetTableCache()->OpenSSTable(file->GetFileNumber());
                iters.emplace_back(table->NewIterator());
            }
            input_iters[0] = NewMergingIterator(iters);
        } else {
            auto file_iter = NewFileMetaDataIterator(task->input_files_[i]);
            input_iters[i] = NewTwoLevelIterator(std::move(file_iter), GetFileIterator, db_meta_data_->GetTableCache());
        }
    }

    auto input_iter = NewMergingIterator(input_iters);
    if(input_iter == nullptr) {
        return false;
    }
    input_iter->SeekToFirst();

    SStableBuilder *builder = nullptr;
    FileMetaData *file_meta_data = nullptr;
    InternalKey largest;

    while(input_iter->Valid()) {
        if(builder == nullptr || builder->EstimatedSize() >= MAX_FILE_SIZE) {
            if(builder != nullptr) {
                // TODO 将 SSTable 放入 TableCaz
                auto sstable = builder->Builder();

                file_meta_data->largest_ = largest;
                file_meta_data->file_size_ = builder->EstimatedSize();
                task->output_files_.emplace_back(file_meta_data);

                delete builder;
            }

            auto file_number = db_meta_data_->NextFileNumber();
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

    return true;
}

auto Table::CompactMemTable(const std::shared_ptr<MemTable> &mem) -> FileMetaData * {
    auto file_number = db_meta_data_->NextFileNumber();

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
    table_cache_->AddSSTable(std::move(sstable));

    auto file_meta_data = new FileMetaData(file_number, start_key, end_key);
    return file_meta_data;
}




} // namespace ljdb