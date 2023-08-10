#pragma once

#include <atomic>
#include <utility>
#include <algorithm>
#include <mutex>
#include <condition_variable>

#include "common/config.h"
#include "format.h"
#include "common/iterator.h"
#include "mem_table/mem_table.h"
#include "db_meta_data.h"
#include "compaction.h"


namespace ljdb {

// 返回每层的最大文件大小
static auto MaxBytesForLevel(int level) -> double {
    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

enum CompactionType { SizeCompaction, SeekCompaction };

struct CompactionTask {
    // 压缩的目标层数
    int level_;

    // 压缩类型
    CompactionType type_;

    // 输出文件的最大大小
    uint64_t max_output_file_size_;

    // 压缩的输入文件
    std::vector<FileMetaData*> input_files_[2];

    // 压缩的输出文件
    std::vector<FileMetaData*> output_files_;
};

class TableMetaData {
public:
    auto GetFileMetaData(size_t level) const -> const std::vector<FileMetaData*>& {
        return files_[level];
    }

    // 添加 SSTable
    void AddFileMetaData(int32_t level, const std::vector<FileMetaData*> &file);

    // 删除 SSTable
    void RemoveFileMetaData(int32_t level, const std::vector<FileMetaData*> &file);

    // 返回 level 层与 [begin, end] 存在重叠的文件
    void GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs);

    // 返回 level 层的文件总大小
    auto TotalFileSize(int level) -> uint64_t;

    auto NextFileNumber() -> file_number_t  {
        return db_meta_data_->NextFileNumber();
    }

    // 是否存在压缩计划
    auto ExistCompactionTask() -> bool {
        return size_compaction_score_ >= 1 || seek_compaction_level_ != -1;
    }

    // 确定下一次 Size Compaction 计划
    // 需要获取 TableMetaData 的锁
    void Finalize();

    // Seek Compaction 计划
    // 需要获取 TableMetaData 的锁
    void SeekCompaction(int level, FileMetaData *meta_data);

    void ClearSeekCompaction();

    // 生成压缩计划
    auto GenerateCompactionTask() -> CompactionTask*;


private:
    int32_t table_number_;

    DBMetaData *db_meta_data_;

    // sstable
    std::vector<FileMetaData*> files_[K_NUM_LEVELS];

    // Size Compaction 的压缩分数
    int size_compaction_level_{-1};
    double size_compaction_score_{-1};

    // Size Compaction 上次压缩结束的位置
    InternalKey size_compaction_pointer_[K_NUM_LEVELS];

    // Seek Compaction 的压缩信息
    int seek_compaction_level_{-1};
    FileMetaData *seek_compaction_file_{nullptr};
};


} // namespace ljdb