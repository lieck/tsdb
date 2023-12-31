
#include <string>
#include <fstream>
#include "disk/disk_manager.h"
#include "common/exception.h"
#include "common/macros.h"

namespace LindormContest {

std::string db_directory = "./";

void SetDatabaseDirectory(const std::string &directory) {
    db_directory = directory;
}

auto DiskManager::CreateSSTableFile(file_number_t file_number) -> std::ofstream {
    std::string file_name = GET_SSTABLE_NAME(file_number);
    return CreakWritableFile(file_name);
}

auto DiskManager::OpenSSTableFile(file_number_t file_number) -> std::ifstream {
    std::string file_name = GET_SSTABLE_NAME(file_number);
    return OpenFile(file_name);
}

auto DiskManager::CreakWritableFile(const std::string& filename) -> std::ofstream {
    std::ofstream file(db_directory + filename);
    if(!file.is_open()) {
        throw Exception(ExceptionType::IO, "Could not create file: " + db_directory + filename);
    }
    return file;
}

auto DiskManager::OpenFile(const std::string& filename) -> std::ifstream {
    std::ifstream file(db_directory + filename);
    if(!file.is_open()) {
        throw Exception(ExceptionType::IO, "Could not open file: " + db_directory + filename);
    }
    return file;
}

auto DiskManager::RemoveFile(const std::string& filename) -> bool {
    return std::remove((db_directory + filename).c_str()) == 0;
}

void DiskManager::ReadBlock(std::ifstream &file, char *data, uint32_t size, uint32_t offset) {
    file.seekg(offset);
    file.read(data, size);
    if(file.bad()) {
        throw Exception(ExceptionType::IO, "I/O error while reading file");
    }
}

void DiskManager::WriteBlock(std::ofstream &file, const char *data, uint32_t size) {
    file.write(data, size);
    if(file.bad()) {
        throw Exception(ExceptionType::IO, "I/O error while writing file");
    }
}

auto DiskManager::GetFileSize(std::ifstream &file) -> uint64_t {
    file.seekg(0, std::ios::end);
    std::streampos file_size = file.tellg();
    if(file.bad()) {
        throw Exception(ExceptionType::IO, "I/O error while reading file");
    }
    return file_size;
}

void DiskManager::ReadBlock(file_number_t file_number, char *data, uint32_t size, uint32_t offset) {
    std::ifstream file = OpenSSTableFile(file_number);
    ReadBlock(file, data, size, offset);
}

auto DiskManager::RemoveSSTableFile(file_number_t file_number) -> bool {
    std::string file_name = GET_SSTABLE_NAME(file_number);
    return RemoveFile(file_name);
}


}  // namespace LindormContest