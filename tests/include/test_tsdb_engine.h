
#include "TSDBEngine.hpp"

#include <fstream>
#include <sstream>
#include <filesystem>


/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace LindormContest {

    struct VinHasher {
        size_t operator()(const Vin &c) const;
        bool operator()(const Vin &c1, const Vin &c2) const;
    };

    struct RowHasher {
        size_t operator()(const Row &c) const;
        bool operator()(const Row &c1, const Row &c2) const;
    };

    inline size_t VinHasher::operator()(const Vin &c) const {
        // Apple Clang does not support this function, use g++-12 to take place Apple Clang.
        return std::_Hash_impl::hash((void *) c.vin, VIN_LENGTH);
    }

    inline bool VinHasher::operator()(const Vin &c1, const Vin &c2) const {
        return c1 == c2;
    }

    inline size_t RowHasher::operator()(const Row &c) const {
        VinHasher vinHasher;
        return vinHasher(c.vin);
    }

    inline bool RowHasher::operator()(const Row &c1, const Row &c2) const {
        return c1 == c2;
    }

    class TestTSDBEngine : public TSDBEngine {
    public:
        /**
         * This constructor's function signature should not be modified.
         * Our evaluation program will call this constructor.
         * The function's body can be modified.
         */
        explicit TestTSDBEngine(const std::string &dataDirPath);

        int connect() override;

        int createTable(const std::string &tableName, const Schema &schema) override;

        int shutdown() override;

        int upsert(const WriteRequest &wReq) override;

        int executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) override;

        int executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) override;

        ~TestTSDBEngine() override = default;

    private:
        // Protect global map, such as outFiles, vinMutex defined below.
        std::mutex globalMutex;
        // Append new written row to this file. Cache the output stream for each file. One file for a vin.
        std::unordered_map<Vin, std::ofstream*, VinHasher, VinHasher> outFiles;
        // One locker for a vin. Lock the vin's locker when reading or writing process touches the vin.
        std::unordered_map<Vin, std::mutex*, VinHasher, VinHasher> vinMutex;
        // How many columns is defined in schema for the sole table.
        int columnsNum;
        // The column's type for each column.
        ColumnType *columnsType;
        // The column's name for each column.
        std::string *columnsName;

    private:
        std::mutex& getMutexForVin(const Vin &vin);

        // Must be protected by vin's mutex.
        // The gotten stream is shared by all caller, and should not be closed manually by caller.
        std::ofstream& getFileOutForVin(const Vin &vin);

        // Must be protected by vin's mutex.
        // The returned ifstream is exclusive for each caller, and must be closed by caller.
        int getFileInForVin(const Vin &vin, std::ifstream &fin);

        int getLatestRow(const Vin &vin, const std::set<std::string> &requestedColumns, Row &result);

        void getRowsFromTimeRange(const Vin &vin, int64_t lowerInclusive, int64_t upperExclusive,
                                  const std::set<std::string> &requestedColumns, std::vector<Row> &results);

        // Get the file path for this vin, there should be only one file for a vin.
        std::string getVinFilePath(const Vin &vin);

        // Must be protected by vin's mutex.
        // Read the row from this fin. The offset of this fin should be set at the start position for the row.
        // Return 0 means success, -1 means the fin is at eof.
        int readRowFromStream(const Vin &vin, std::ifstream &fin, Row &row);

        // Must be protected by vin's mutex.
        // Append the row to the tail of the file.
        void appendRowToFile(std::ofstream &fout, const Row &row);
    }; // End class TSDBEngineImpl.



    inline std::string int64ToStr(int64_t num) {
        std::ostringstream oss;
        oss << num;
        return oss.str();
    }

    inline std::ostream& operator<< (std::ostream& os, const Vin &vin) {
        std::string vinStr(vin.vin, vin.vin + VIN_LENGTH);
        os << vinStr;
        return os;
    }

    inline void swapRow(Row &lhs, Row &rhs) {
        std::swap(lhs.vin, rhs.vin);
        std::swap(lhs.timestamp, rhs.timestamp);
        lhs.columns.swap(rhs.columns);
    }

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    TestTSDBEngine::TestTSDBEngine(const std::string &dataDirPath)
            : TSDBEngine(dataDirPath), columnsNum(-1), columnsName(nullptr), columnsType(nullptr) {
    }

    int TestTSDBEngine::connect() {
        // Read schema.
        std::ifstream schemaFin;
        schemaFin.open(getDataPath() + "/schema", std::ios::in);
        if (!schemaFin.is_open() || !schemaFin.good()) {
            std::cout << "Connect new database with empty pre-written data" << std::endl;
            schemaFin.close();
            return 0;
        }

        schemaFin >> columnsNum;
        if (columnsNum <= 0) {
            std::cerr << "Unexpected columns' num: [" << columnsNum << "]" << std::endl;
            schemaFin.close();
            throw std::exception();
        }
        std::cout << "Found pre-written data with columns' num: [" << columnsNum << "]" << std::endl;

        columnsType = new ColumnType[columnsNum];
        columnsName = new std::string[columnsNum];

        for (int i = 0; i < columnsNum; ++i) {
            schemaFin >> columnsName[i];
            int32_t columnTypeInt;
            schemaFin >> columnTypeInt;
            columnsType[i] = (ColumnType) columnTypeInt;
        }

        return 0;
    }

    int TestTSDBEngine::createTable(const std::string &tableName, const Schema &schema) {
        columnsNum = (int32_t) schema.columnTypeMap.size();
        columnsName = new std::string[columnsNum];
        columnsType = new ColumnType[columnsNum];
        int i = 0;
        for (auto it = schema.columnTypeMap.cbegin(); it != schema.columnTypeMap.cend(); ++it) {
            columnsName[i] = it->first;
            columnsType[i++] = it->second;
        }
        return 0;
    }

    int TestTSDBEngine::shutdown() {
        // Close all resources, assuming all writing and reading process has finished.
        // No mutex is fetched by assumptions.

        for (const auto& pair : outFiles) {
            pair.second->close();
            delete pair.second;
        }

        for (const auto& pair : vinMutex) {
            delete pair.second;
        }

        // Persist the schema.
        if (columnsNum > 0) {
            std::ofstream schemaFout;
            schemaFout.open(getDataPath() + "/schema", std::ios::out);
            schemaFout << columnsNum;
            schemaFout << " ";
            for (int i = 0; i < columnsNum; ++i) {
                schemaFout << columnsName[i] << " ";
                schemaFout << (int32_t) columnsType[i] << " ";
            }
            schemaFout.close();
        }

        if (columnsType != nullptr) {
            delete []columnsType;
        }

        if (columnsName != nullptr) {
            delete []columnsName;
        }

        return 0;
    }

    int TestTSDBEngine::upsert(const WriteRequest &writeRequest) {

        for (const Row &row : writeRequest.rows) {
            const Vin &vin = row.vin;
            auto &mutexForVin = getMutexForVin(row.vin);
            mutexForVin.lock();

            // Append to file.
            std::ofstream &fileOutForVin = getFileOutForVin(vin);
            appendRowToFile(fileOutForVin, row);

            mutexForVin.unlock();
        }

        return 0;
    }

    int TestTSDBEngine::executeLatestQuery(const LatestQueryRequest &pReadReq, std::vector<Row> &pReadRes) {
        for (const auto &vin : pReadReq.vins) {
            Row row;
            int ret = getLatestRow(vin, pReadReq.requestedColumns, row);
            if (ret == 0) {
                pReadRes.push_back(std::move(row));
            }
        }
        return 0;
    }

    int TestTSDBEngine::executeTimeRangeQuery(const TimeRangeQueryRequest &trReadReq, std::vector<Row> &trReadRes) {
        getRowsFromTimeRange(trReadReq.vin, trReadReq.timeLowerBound, trReadReq.timeUpperBound,
                             trReadReq.requestedColumns, trReadRes);
        return 0;
    }

    std::mutex &TestTSDBEngine::getMutexForVin(const Vin &vin) {
        std::mutex *pMutex;
        globalMutex.lock();
        const auto it = vinMutex.find(vin);
        if (it != vinMutex.cend()) {
            pMutex = it->second;
        } else {
            pMutex = new std::mutex();
            vinMutex.insert(std::make_pair(vin, pMutex));
        }
        globalMutex.unlock();
        return *pMutex;
    }

    std::ofstream &TestTSDBEngine::getFileOutForVin(const Vin &vin) {
        // Must be protected by vin's mutex.

        // Try getting from already opened set.
        globalMutex.lock();
        auto it = outFiles.find(vin);
        if (it != outFiles.cend()) {
            auto pFileOut = it->second;
            globalMutex.unlock();
            return *pFileOut;
        }
        globalMutex.unlock();

        // The first time we open the file out stream for this vin, open a new stream and put it into opened set.
        std::string vinFilePath = getVinFilePath(vin);
        std::ofstream *pFileOut = new std::ofstream();
        pFileOut->open(vinFilePath, std::ios::out | std::ios::app | std::ios::binary | std::ios::ate);
        if (!pFileOut->is_open() || !pFileOut->good()) {
            std::cerr << "Cannot open write stream for vin file: [" << vinFilePath << "]" << std::endl;
            delete pFileOut;
            throw std::exception();
        }

        globalMutex.lock();
        outFiles.insert(std::make_pair(vin, pFileOut));
        globalMutex.unlock();

        return *pFileOut;
    }

    int TestTSDBEngine::getLatestRow(const Vin &vin, const std::set<std::string> &requestedColumns, Row &result) {
        std::mutex &mutexForCurVin = getMutexForVin(vin);
        mutexForCurVin.lock();

        std::ifstream fin;
        int ret = getFileInForVin(vin, fin);
        if (ret != 0) {
            // No such vin written.
            mutexForCurVin.unlock();
            return -1;
        }

        Row latestRow;
        int rowNums = 0;
        for (; !fin.eof();) {
            Row nextRow;
            ret = readRowFromStream(vin, fin, nextRow);
            if (ret != 0) {
                // EOF reached, no more row.
                break;
            }
            if (rowNums == 0 || nextRow.timestamp >= latestRow.timestamp) {
                swapRow(nextRow, latestRow);
            }
            ++rowNums;
        }

        fin.close();
        mutexForCurVin.unlock();

        if (rowNums == 0) {
            return -1;
        }

        result.vin = vin;
        result.timestamp = latestRow.timestamp;
        for (const auto & requestedColumn : requestedColumns) {
            result.columns.insert(std::make_pair(requestedColumn, latestRow.columns.at(requestedColumn)));
        }
        return 0;
    }

    void
    TestTSDBEngine::getRowsFromTimeRange(const Vin &vin, int64_t lowerInclusive, int64_t upperExclusive,
                                         const std::set<std::string> &requestedColumns,
                                         std::vector<Row> &results) {
        std::mutex &mutexForCurVin = getMutexForVin(vin);
        mutexForCurVin.lock();

        std::ifstream fin;
        int ret = getFileInForVin(vin, fin);
        if (ret != 0) {
            // No such vin written.
            mutexForCurVin.unlock();
            return;
        }

        for (; !fin.eof();) {
            Row nextRow;
            ret = readRowFromStream(vin, fin, nextRow);
            if (ret != 0) {
                // EOF reached, no more row.
                break;
            }
            if (nextRow.timestamp >= lowerInclusive && nextRow.timestamp < upperExclusive) {
                Row resultRow;
                resultRow.vin = vin;
                resultRow.timestamp = nextRow.timestamp;
                for (const auto & requestedColumn : requestedColumns) {
                    resultRow.columns.insert(std::make_pair(requestedColumn, nextRow.columns.at(requestedColumn)));
                }
                results.push_back(std::move(resultRow));
            }
        }

        fin.close();
        mutexForCurVin.unlock();
    }

    std::string TestTSDBEngine::getVinFilePath(const Vin &vin) {
        std::string vinStr(vin.vin, vin.vin + VIN_LENGTH);
        int32_t folderNum = (int32_t) VinHasher()(vin) % 100;
        std::string folderStr = getDataPath() + "/" + int64ToStr(folderNum);
        bool dirExist = std::filesystem::is_directory(folderStr);
        if (!dirExist) {
            bool created = std::filesystem::create_directories(folderStr);
            if (!created && !std::filesystem::is_directory(folderStr)) {
                std::cerr << "Cannot create directory: [" << folderStr << "]" << std::endl;
                throw std::exception();
            }
        }
        return folderStr + "/" + vinStr;
    }

    int TestTSDBEngine::readRowFromStream(const Vin &vin, std::ifstream &fin, Row &row) {
        // Must be protected by vin's mutex.

        if (fin.eof()) {
            return -1;
        }

        int64_t timestamp;
        fin.read((char *) &timestamp, sizeof(int64_t));
        if (fin.fail() || fin.eof()) {
            return -1;
        }

        row.vin = vin;
        row.timestamp = timestamp;

        for (int cI = 0; cI < columnsNum; ++cI) {
            std::string &cName = columnsName[cI];
            ColumnType cType = columnsType[cI];
            ColumnValue *cVal;
            switch (cType) {
                case COLUMN_TYPE_INTEGER: {
                    int32_t intVal;
                    fin.read((char *) &intVal, sizeof(int32_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(intVal);
                    break;
                }
                case COLUMN_TYPE_DOUBLE_FLOAT: {
                    double_t doubleVal;
                    fin.read((char *) &doubleVal, sizeof(double_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(doubleVal);
                    break;
                }
                case COLUMN_TYPE_STRING: {
                    int32_t strLen;
                    fin.read((char *) &strLen, sizeof(int32_t));
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        throw std::exception();
                    }
                    char *strBuff = new char[strLen];
                    fin.read(strBuff, strLen);
                    if (fin.fail()) {
                        std::cerr << "Premature eof in file for vin: [" << vin
                                  << "]. The timestamp is read but no enough data attached" << std::endl;
                        delete []strBuff;
                        throw std::exception();
                    }
                    cVal = new ColumnValue(strBuff, strLen);
                    delete []strBuff;
                    break;
                }
                default: {
                    std::cerr << "Undefined column type, this is not expected" << std::endl;
                    throw std::exception();
                }
            }
            row.columns.insert(std::make_pair(cName, std::move(*cVal)));
            delete cVal;
        }

        return 0;
    }

    void TestTSDBEngine::appendRowToFile(std::ofstream &fout, const Row &row) {
        // Must be protected by vin's mutex.

        if (row.columns.size() != columnsNum) {
            std::cerr << "Cannot write a non-complete row with columns' num: [" << row.columns.size() << "]. ";
            std::cerr << "There is [" << columnsNum << "] rows in total" << std::endl;
            throw std::exception();
        }

        fout.write((const char *) &row.timestamp, sizeof(int64_t));
        for (int i = 0; i < columnsNum; ++i) {
            std::string &cName = columnsName[i];
            const ColumnValue &cVal = row.columns.at(cName);
            int32_t rawSize = cVal.getRawDataSize();
            fout.write(cVal.columnData, rawSize);
        }

        fout.flush();
    }

    int TestTSDBEngine::getFileInForVin(const Vin &vin, std::ifstream &fin) {
        // Must be protected by vin's mutex.

        std::string vinFilePath = getVinFilePath(vin);
        std::ifstream vinFin;
        vinFin.open(vinFilePath, std::ios::in | std::ios::binary);
        if (!vinFin.is_open() || !vinFin.good()) {
            std::cout << "Cannot get vin file input-stream for vin: [" << vin << "]. No such file" << std::endl;
            return -1;
        }
        fin = std::move(vinFin);
        return 0;
    }
}