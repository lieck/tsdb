
#include <fstream>
#include <utility>
#include "db/table.h"
#include "util/coding.h"
#include "common/config.h"
#include "sstable/sstable_builder.h"
#include "db/background.h"
#include "common/merger_iterator.h"
#include "common/two_level_iterator.h"

namespace LindormContest {

    const constexpr int TABLE_META_DATA_MAGIC = 0x02341678;

Table::Table(std::string tableName, Schema schema, DBOptions *options) :
table_name_(std::move(tableName)), schema_(std::move(schema)), options_(options), table_cache_(options->table_cache_) {

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
    auto query = QueryRequest(pReadReq.vins, pReadReq.requestedColumns, &pReadRes);

    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<MemTable>> mem_table;
    mem_table.push_back(mem_);
    for(auto iter = imm_.rbegin(); iter != imm_.rend(); ++iter) {
        mem_table.push_back(mem_);
    }

    std::vector<std::shared_ptr<FileMetaData>> sstable[K_NUM_LEVELS];
    for(int i = 0; i < K_NUM_LEVELS; ++i) {
        sstable[i] = table_meta_data_.GetFileMetaData(i);
    }
    lock.unlock();

    // 搜索 l0
    for(auto &mem : mem_table) {
        MemTableQuery(query, mem);
    }

    // 搜索 sstable
    for(auto & i : sstable) {
        for(const auto& f : i) {
            FileTableQuery(f, query);
        }
    }


    return 0;
}

auto Table::ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
    auto query = RangeQueryRequest(&trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                   &trReadReq.requestedColumns, &trReadRes);

    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<MemTable>> mem_table;
    mem_table.push_back(mem_);
    for(auto iter = imm_.rbegin(); iter != imm_.rend(); ++iter) {
        mem_table.push_back(mem_);
    }

    std::vector<std::shared_ptr<FileMetaData>> sstable[K_NUM_LEVELS];
    for(int i = 0; i < K_NUM_LEVELS; ++i) {
        sstable[i] = table_meta_data_.GetFileMetaData(i);
    }
    lock.unlock();

    // 搜索 l0
    for(auto &mem : mem_table) {
        MemTableRangeQuery(query, mem);
    }

    // 搜索 sstable
    for(auto & i : sstable) {
        for(const auto& f : i) {
            FileTableRangeQuery(f, query);
        }
    }

    return 0;
}

auto Table::MemTableQuery(Table::QueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
    auto vin_iter = req.vin_.begin();
    auto iter = mem->NewIterator();
    iter->Seek(InternalKey(*vin_iter, MAX_TIMESTAMP));

    while(iter->Valid() && vin_iter != req.vin_.end()) {
        if(iter->GetKey().vin_ == *vin_iter) {
            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, &req.columns_);
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

auto Table::FileTableQuery(const FileMetaDataPtr& fileMetaData, Table::QueryRequest &req) -> void {
    if(req.vin_.empty()) {
        return;
    }

    auto iter = table_cache_->NewTableIterator(fileMetaData);
    for(const auto& r : req.vin_) {
        InternalKey internal_key(r, MAX_TIMESTAMP);
        if(internal_key < fileMetaData->smallest_ || internal_key > fileMetaData->largest_) {
            continue;
        }

        iter->Seek(internal_key);
        if(iter->Valid()) {
            auto key = iter->GetKey();
            if(key.vin_ == r) {
                Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, &req.columns_);
                row.vin = key.vin_;
                row.timestamp = key.timestamp_;
                req.result_->push_back(row);
            }
            iter->Next();
        }
    }
}

void Table::FileTableRangeQuery(const FileMetaDataPtr& fileMetaData, Table::RangeQueryRequest &req) {
    InternalKey internal_key(*req.vin_, req.time_upper_bound_);
    if(fileMetaData->smallest_ > internal_key || fileMetaData->largest_ < internal_key) {
        return;
    }

    auto iter = table_cache_->NewTableIterator(fileMetaData);
    iter->Seek(internal_key);
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
        options_->bg_task_->Schedule(&Table::BGWork, this);
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
            table_meta_data_.ClearCompaction();
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
                auto iter = table_cache_->NewTableIterator(file);
                iters.emplace_back(std::move(iter));
            }
            input_iters[0] = NewMergingIterator(iters);
        } else {
            auto file_iter = NewFileMetaDataIterator(task->input_files_[i]);
            input_iters[i] = NewTwoLevelIterator(std::move(file_iter), GetFileIterator, options_->table_cache_);
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

            auto file_number = options_->NextFileNumber();
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

auto Table::CompactMemTable(const std::shared_ptr<MemTable> &mem) -> FileMetaDataPtr {
    auto file_number = options_->NextFileNumber();

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

    auto file_meta_data = std::make_shared<FileMetaData>(file_number, start_key, end_key, sstable->GetFileSize());
    table_cache_->AddSSTable(std::move(sstable));

    return file_meta_data;
}

auto Table::Shutdown() -> int {
    is_shutting_down_.store(true, std::memory_order_release);
    return 0;
}

    auto Table::WriteMetaData(std::ofstream &file) const -> void {
        char buffer[FILE_META_DATA_SIZE];

        file << TABLE_META_DATA_MAGIC;

        // table name
        CodingUtil::PutUint32(buffer, table_name_.size());
        file.write(buffer, CodingUtil::LENGTH_SIZE);
        file.write(table_name_.data(), static_cast<int64_t>(table_name_.size()));

        // schema
        std::string schema_str = CodingUtil::SchemaToBytes(schema_);
        CodingUtil::PutUint32(buffer, schema_str.size());
        file.write(schema_str.data(), static_cast<int64_t>(schema_str.size()));

        // file number
        for(const auto& level : table_meta_data_.files_) {
            CodingUtil::PutUint32(buffer, level.size());
            file.write(buffer, CodingUtil::LENGTH_SIZE);

            for(auto &file_meta_data : level) {
                std::string str;
                file_meta_data->EncodeTo(&str);
                file.write(buffer, FILE_META_DATA_SIZE);
            }
        }
    }

    auto Table::ReadMetaData(std::ifstream &file) -> void {
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
        if(FILE_META_DATA_SIZE >= table_name_size) {
            std::string name(table_name_size, ' ');
            file.read(name.data(), table_name_size);
            table_name_ = name;
        } else {
            file.read(buffer, table_name_size);
            buffer[table_name_size] = '\0';
            table_name_ = std::string(buffer);
        }

        // schema
        uint32_t schema_size;
        file.read(buffer, CodingUtil::LENGTH_SIZE);
        schema_size = CodingUtil::DecodeUint32(buffer);
        std::string schema_str(schema_size, ' ');
        file.read(schema_str.data(), schema_size);
        schema_ = CodingUtil::BytesToSchema(schema_str);

        // file number
        for(auto & i : table_meta_data_.files_) {
            uint32_t file_size;
            file.read(buffer, CodingUtil::LENGTH_SIZE);
            file_size = CodingUtil::DecodeUint32(buffer);
            for(uint32_t j = 0; j < file_size; j++) {
                file.read(buffer, FILE_META_DATA_SIZE);
                i.emplace_back(std::make_shared<FileMetaData>(buffer));
            }
        }
    }


} // namespace ljdb