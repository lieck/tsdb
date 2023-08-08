#include "meta_data.h"
#include "cache/table_cache.h"
#include "mem_table/mem_table.h"

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

static auto MaxCompactionBytesForLevel(int level) -> double {

}


struct CompactionTask {
    // 压缩的目标层数
    int level_;

    // 输出文件的最大大小
    uint64_t max_output_file_size_;

    // 压缩的输入文件
    std::vector<FileMetaData*> input_files_[2];

    // 压缩的输出文件
    std::vector<FileMetaData*> output_files_;
};


class Compaction {
public:
    explicit Compaction(TableMetaData *table_meta_data, TableCache *table_cache) : table_meta_data_(table_meta_data), table_cache_(table_cache) {}

    // 确定下一次 Size Compaction 计划
    // 需要获取 TableMetaData 的锁
    void Finalize();

    // Seek Compaction 计划
    // 需要获取 TableMetaData 的锁
    void SeekCompaction(int level, FileMetaData *meta_data);

    // 生成压缩计划
    auto GenerateCompactionTask() -> CompactionTask*;

    // 执行 Manual Compaction
    void DoManualCompaction(CompactionTask* task);

    // 执行 Minor Compaction
    auto CompactMemTable(const std::shared_ptr<MemTable>& mem) -> FileMetaData*;

private:
    TableMetaData *table_meta_data_;
    TableCache *table_cache_;

    // Size Compaction 的压缩分数
    int size_compaction_level_{-1};
    double size_compaction_score_{-1};

    // Size Compaction 上次压缩结束的位置
    InternalKey size_compaction_pointer_[K_NUM_LEVELS];

    // Seek Compaction 的压缩信息
    int seek_compaction_level_{-1};
    FileMetaData *seek_compaction_file_{nullptr};
};



}; // namespace ljdb