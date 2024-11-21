#include <iostream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include "libraries/rapidcsv.hpp"
#include "libraries/json.hpp"

using namespace std;
using json = nlohmann::json;

struct Node {
    string data; // имя столбца
    Node* next;
};

struct Table {
    string name; // имя таблицы
    Node* column;
    Table* next;
};

struct TableJson {
    int strCount; 
    string scheme; // имя схемы
    Table* head = nullptr;
};

void copyColNames(const filesystem::path& file_read, const filesystem::path& file_write);
void insert(string command, TableJson& jstab);

void strDelete(string command, TableJson& jstab);

void select(string command, TableJson& jstab);

Table* createOrGetTable(Table*& head, Table*& tail, const std::string& tableName);
void initializeLockFile(const filesystem::path& tableDir, const std::string& tableName);
void initializePrimaryKeyFile(const filesystem::path& tableDir, const std::string& pkColumn);
bool initializeCsvFile(const filesystem::path& csvFilePath, const std::string& pkColumn);
void updateColumns(Node*& headColumn, Node*& tailColumn, const json& columns, std::ofstream& csv, bool isNewCsv);
void createOrUpdateFiles(const filesystem::path& schemePath, const json& jsonStruct, TableJson& jstable);
void parser(TableJson& jstab);

bool isLocked(string table, string scheme);
void tableLocker(string table, string scheme);
bool tableExist(string table, Table* head);
bool columnExist(string table, string columnName, Table* head);
void strSplit(string message, string& table, string& column);
void crossJoin(string table1, string column1, string table2, string column2, TableJson& jstab);
void crossJoinDepend(string table1, string column1, string table2, string column2, TableJson& jstab);
void filterTable(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2);
void filterStr(TableJson& jstab, string tablefilter1, string table, string columnfilter1, string sfilter);
void filterAnd(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2, string tablefilter3, string columnfilter3, string sfilter);
void filterOr(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2, string tablefilter3, string columnfilter3, string sfilter);
bool containsDot(string message);
bool compareTableStr(string table, string column, string tablefilter, string columnfilter, size_t csvNum, size_t row, TableJson& jstab);
bool matchesColumnValue(string table, string column, string sfilter, size_t csvNum, size_t row, TableJson& jstab);




