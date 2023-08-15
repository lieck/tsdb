#ifndef LJDB_DISK_DISK_MANAGER_H
#define LJDB_DISK_DISK_MANAGER_H

#include "common/config.h"

namespace LindormContest {

class DiskManager {
public:

    static auto OpenSSTableFile(file_number_t file_number) -> std::ifstream;

    static auto CreateSSTableFile(file_number_t file_number) -> std::ofstream;

    static auto CreakWritableFile(const std::string& filename) -> std::ofstream;

    static auto OpenFile(const std::string& filename) -> std::ifstream;

    static auto RemoveFile(const std::string& filename) -> bool;

    static void ReadBlock(std::ifstream& file, char* data, uint32_t size, uint32_t offset);

    static void ReadBlock(file_number_t file_number, char* data, uint32_t size, uint32_t offset);

    static void WriteBlock(std::ofstream& file, const char* data, uint32_t size);

    static auto GetFileSize(std::ifstream& file) -> uint64_t;
};


}; // namespace ljdb

#endif //LJDB_DISK_DISK_MANAGER_H