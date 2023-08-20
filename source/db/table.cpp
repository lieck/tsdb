
#include <fstream>
#include <utility>
#include "db/table.h"
#include "util/coding.h"
#include "common/config.h"
#include "sstable/sstable_builder.h"
#include "db/background.h"
#include "common/merger_iterator.h"
#include "common/two_level_iterator.h"
#include "db/file_meta_data.h"
#include "common/logger.h"

namespace LindormContest {

const std::string MANIFEST_FILE_MAGIC = "LJDB";

Table::Table(std::string tableName, Schema schema, DBOptions *options) :
table_name_(std::move(tableName)), schema_(std::move(schema)), options_(options), table_cache_(options->table_cache_) {

}


auto Table::Upsert(const WriteRequest &wReq) -> int {
    std::unique_lock<std::mutex> lock(mutex_);

    // 若其他线程正在写入, 则等待
    write_queue_.push(&wReq);
    while(write_queue_.front() != &wReq) {
        cv_.wait(lock);
    }

    if(is_shutting_down_.load(std::memory_order_acquire)) {
        cv_.notify_all();
        lock.unlock();
        return -1;
    }

    if(mem_ == nullptr) {
        mem_ = std::make_shared<MemTable>();
    }
    lock.unlock();

    mem_->Lock();

    // 将数据写入 memtable
    for(auto &row : wReq.rows) {
        // 判断 mem 是否写满
        if(mem_->ApproximateSize() >= K_MEM_TABLE_SIZE_THRESHOLD) {
            lock.lock();
            mem_->Unlock();

            LOG_INFO("memtable is full, flush to immtable");
            imm_.push_back(mem_);
            mem_ = std::make_shared<MemTable>();
            mem_->Lock();

            MaybeScheduleCompaction();
            lock.unlock();
        }

        mem_->Insert(row);
    }

    mem_->Unlock();

    lock.lock();
    write_queue_.pop();
    cv_.notify_all();
    return 0;
}

auto Table::ExecuteLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) -> int {
    auto query = QueryRequest(pReadReq.vins, pReadReq.requestedColumns, &pReadRes);

    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<MemTable>> imm_table;
    auto mem_table = mem_;
    for(auto iter = imm_.rbegin(); iter != imm_.rend(); ++iter) {
        imm_table.push_back(*iter);
    }

    std::vector<std::shared_ptr<FileMetaData>> sstable[K_NUM_LEVELS];
    for(int i = 0; i < K_NUM_LEVELS; ++i) {
        sstable[i] = table_meta_data_.GetFileMetaData(i);
    }
    lock.unlock();

    // mem 需要加锁
    if(mem_table != nullptr) {
        mem_table->Lock();
        MemTableQuery(query, mem_table);
        mem_table->Unlock();
    }

    // 搜索 imm
    for(auto &imm : imm_table) {
        MemTableQuery(query, imm);
    }

    // 搜索 sstable
    for(auto & i : sstable) {
        for(const auto& f : i) {
            FileTableQuery(f, query);
        }
    }

    for(auto &row : query.vin_) {
        if(row.second.timestamp != 0) {
            pReadRes.push_back(row.second);
        }
    }

    return 0;
}

auto Table::ExecuteTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) -> int {
    auto query = RangeQueryRequest(&trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                                   &trReadReq.requestedColumns, &trReadRes);

    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<MemTable>> imm_table;
    auto mem_table = mem_;
    for(auto iter = imm_.rbegin(); iter != imm_.rend(); ++iter) {
        imm_table.push_back(*iter);
    }

    std::vector<std::shared_ptr<FileMetaData>> sstable[K_NUM_LEVELS];
    for(int i = 0; i < K_NUM_LEVELS; ++i) {
        sstable[i] = table_meta_data_.GetFileMetaData(i);
    }
    lock.unlock();

    // mem 需要加锁
    if(mem_table != nullptr) {
        mem_table->Lock();
        MemTableRangeQuery(query, mem_table);
        mem_table->Unlock();
    }

    for(auto &imm : imm_table) {
        MemTableRangeQuery(query, imm);
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
    auto iter = mem->NewIterator();
    iter->SeekToFirst();
    while(iter->Valid()) {
        if(req.vin_.count(iter->GetKey().vin_) == 1) {
            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, &req.columns_);
            row.vin = iter->GetKey().vin_;
            row.timestamp = iter->GetKey().timestamp_;

            if(req.vin_[row.vin].timestamp < row.timestamp) {
                req.vin_[row.vin] = row;
            }
        }
        iter->Next();
    }
}

auto Table::MemTableRangeQuery(Table::RangeQueryRequest &req, const std::shared_ptr<MemTable>& mem) -> void {
    auto iter = mem->NewIterator();
    iter->Seek(InternalKey(*req.vin_, req.time_upper_bound_ - 1));
    while(iter->Valid()) {
        auto key = iter->GetKey();
        if(key.vin_ != *req.vin_ || key.timestamp_ < req.time_lower_bound_) {
            break;
        }

        if(key.timestamp_ < req.time_lower_bound_ || key.timestamp_ > req.time_upper_bound_) {
            iter->Next();
            continue;
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
    iter->SeekToFirst();

    while(iter->Valid()) {
        if(req.vin_.count(iter->GetKey().vin_) == 1) {
            Row row = CodingUtil::DecodeRow(iter->GetValue().data(), schema_, &req.columns_);
            row.vin = iter->GetKey().vin_;
            row.timestamp = iter->GetKey().timestamp_;

            if(req.vin_[row.vin].timestamp < row.timestamp) {
                req.vin_[row.vin] = row;
            }
        }
        iter->Next();
    }
}

void Table::FileTableRangeQuery(const FileMetaDataPtr& fileMetaData, Table::RangeQueryRequest &req) {
    auto iter = table_cache_->NewTableIterator(fileMetaData);
    iter->Seek(InternalKey(*req.vin_, req.time_upper_bound_ - 1));
    while(iter->Valid()) {
        auto key = iter->GetKey();
        if(key.vin_ != *req.vin_ || key.timestamp_ < req.time_lower_bound_) {
            break;
        }

        if(key.timestamp_ < req.time_lower_bound_ || key.timestamp_ > req.time_upper_bound_) {
            iter->Next();
            continue;
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
    if(!imm_.empty() || (table_meta_data_.ExistCompactionTask() && !is_shutting_down_.load(std::memory_order_acquire))) {
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
        cv_.wait(lock);
    }

    // Minor Compaction
    if(!imm_.empty()) {
        // TODO(lieck) 假设存在多个 immtable 应该同时压缩
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
        LOG_DEBUG("Minor Compaction Done");
        return;
    }

    // 退出时只允许压缩 memtable
    if(is_shutting_down_.load(std::memory_order_acquire)) {
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
    LOG_DEBUG("DoManualCompaction level: %d", task->level_);

    if(task->input_files_[1].empty()) {
        task->output_files_ = task->input_files_[0];
        LOG_DEBUG("DoManualCompaction level: %d, no need to compact", task->level_);
        return true;
    }

    std::vector<std::unique_ptr<Iterator>> input_iters(2);

    // 生成迭代器
    for(size_t i = 0; i < 2; i++) {
        if(task->level_ == 0 && i == 0) {
            std::vector<std::unique_ptr<Iterator>> iters;
            for(auto &file : task->input_files_[i]) {
                iters.emplace_back(table_cache_->NewTableIterator(file));
            }
            input_iters[0] = NewMergingIterator(std::move(iters));
        } else {
            auto file_iter = NewFileMetaDataIterator(task->input_files_[i]);
            input_iters[i] = NewTwoLevelIterator(std::move(file_iter), GetFileIterator, options_->table_cache_);
        }
    }

    auto input_iter = NewMergingIterator(std::move(input_iters));
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
                // TODO(lieck): 灏� SSTable 鏀惧叆 TableCache
                auto sstable = builder->Builder();

                file_meta_data->largest_ = largest;
                file_meta_data->file_size_ = sstable->GetFileSize();
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
        auto sstable = builder->Builder();

        file_meta_data->largest_ = largest;
        file_meta_data->file_size_ = sstable->GetFileSize();
        task->output_files_.emplace_back(file_meta_data);

        delete builder;
    }
    LOG_DEBUG("DoManualCompaction level: %d, output_files size: %zu", task->level_, task->output_files_.size());

    return true;
}

auto Table::CompactMemTable(const std::shared_ptr<MemTable> &mem) -> FileMetaDataPtr {
    LOG_INFO("Compact MemTable table");
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

    {
        // check
        auto file_iter = sstable->NewIterator();
        iter->SeekToFirst();
        auto mem_iter = mem->NewIterator();
        mem_iter->SeekToFirst();
        while(file_iter->Valid() && mem_iter->Valid()) {
            if(file_iter->GetKey() != mem_iter->GetKey()) {
                LOG_ERROR("Compact MemTable table error");
                exit(1);
            }
            if(file_iter->GetValue() != mem_iter->GetValue()) {
                LOG_ERROR("Compact MemTable table error");
                exit(1);
            }
            file_iter->Next();
            mem_iter->Next();
        }
        if(file_iter->Valid() || mem_iter->Valid()) {
            LOG_ERROR("Compact MemTable table error");
            exit(1);
        }
    }

    auto file_meta_data = std::make_shared<FileMetaData>(file_number, start_key, end_key, sstable->GetFileSize());
    // table_cache_->AddSSTable(std::move(sstable));

    return file_meta_data;
}

auto Table::Shutdown() -> int {
    is_shutting_down_.store(true, std::memory_order_release);

    std::unique_lock<std::mutex> lock(mutex_);

    // 等待写入队列为空
    while(!write_queue_.empty()) {
        cv_.wait(lock);
    }

    if(mem_ != nullptr) {
        imm_.push_back(mem_);
        mem_ = nullptr;
        MaybeScheduleCompaction();
    }

    return 0;
}

    auto Table::WriteMetaData(std::ofstream &file) const -> void {
        char buffer[FILE_META_DATA_SIZE];

        file << MANIFEST_FILE_MAGIC;

        // table name
        CodingUtil::PutUint32(buffer, table_name_.size());
        file.write(buffer, CodingUtil::LENGTH_SIZE);
        file.write(table_name_.data(), static_cast<int64_t>(table_name_.size()));

        // schema
        std::string schema_str = CodingUtil::SchemaToBytes(schema_);
        CodingUtil::PutUint32(buffer, schema_str.size());
        file.write(buffer, CodingUtil::LENGTH_SIZE);
        file.write(schema_str.data(), static_cast<int64_t>(schema_str.size()));

        // file number
        for(const auto& level : table_meta_data_.files_) {
            CodingUtil::PutUint32(buffer, level.size());
            file.write(buffer, CodingUtil::LENGTH_SIZE);

            for(auto &file_meta_data : level) {
                std::string str;
                file_meta_data->EncodeTo(&str);
                file.write(str.data(), FILE_META_DATA_SIZE);
            }
        }
    }

    auto Table::ReadMetaData(std::ifstream &file) -> void {
        char buffer[FILE_META_DATA_SIZE];
        std::string magic;
        magic.resize(MANIFEST_FILE_MAGIC.size());
        file.read(magic.data(), MANIFEST_FILE_MAGIC.size());

        if(magic != MANIFEST_FILE_MAGIC) {
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
                // file.read(buffer, FILE_META_DATA_SIZE);

                std::string temp;
                temp.resize(FILE_META_DATA_SIZE);
                file.read(temp.data(), FILE_META_DATA_SIZE);

                i.emplace_back(std::make_shared<FileMetaData>(temp));
            }
        }
    }


}  // namespace LindormContest