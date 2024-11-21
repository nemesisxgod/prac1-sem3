#include "header.h"

bool isLocked(string table, string scheme) {
    filesystem::path filePath = filesystem::current_path() / scheme / table / (table + "_lock.txt");
    ifstream file(filePath);
    if (!file.is_open()) {
        throw runtime_error("cannot open file.");
    }
    string lock;
    file >> lock;
    file.close();
    if (lock == "locked") return true;
    return false;
}

void tableLocker(string table, string scheme) {
    filesystem::path filePath = filesystem::current_path() / scheme / table / (table + "_lock.txt");
    ifstream file(filePath);
    if (!file.is_open()) {
        throw runtime_error("cannot open file.");
    }
    string lock;
    file >> lock;
    file.close();
    ofstream fileOut(filePath);
    if (!fileOut.is_open()) {
        throw runtime_error("cannot open file.");
    }
    if (lock == "locked") {
        fileOut << "unlocked";
    }
    else {
        fileOut << "locked";
    }
    fileOut.close();
}

bool tableExist(string table, Table* head) {
    Table* current = head;
    while (current != nullptr) {
        if (current->name == table) {
            return true;
        }
        current = current->next;
    }
    return false;
}

bool columnExist(string table, string columnName, Table* head) {
    Table* current = head;
    while (current != nullptr) {
        if (current->name == table) {
            Node* currentColumn = current->column;
            while (current != nullptr) {
                if (currentColumn->data == columnName) {
                    return true;
                }
                currentColumn = currentColumn->next;
            }
        }
        current = current->next;
    }
    return false;
}

void strSplit(string message, string& table, string& column) {
    bool containsDot = false;
    for (size_t i=0; i < message.size(); i++) {
        if (message[i] == '.') {
            containsDot = true;
            continue;
        }
        if (message[i] == ',') {
            continue;
        }
        if (containsDot == false) table += message[i];
        else column += message[i];
    }
    if (containsDot == false) {
        throw runtime_error("invalid data entry.");
    }
}

void crossJoin(string table1, string column1, string table2, string column2, TableJson& jstab) {
    size_t csvCount1 = 1;
    while (true) {
        filesystem::path pathCSV1 = filesystem::current_path() / jstab.scheme / table1 / (to_string(csvCount1) + ".csv");
        ifstream file1(pathCSV1);
        if (!file1.is_open()) {
            break;
        }
        file1.close();
        csvCount1++;
    }
    size_t csvCount2 = 1;
    while (true) {
        filesystem::path pathCSV2 = filesystem::current_path() / jstab.scheme / table2 / (to_string(csvCount2) + ".csv");
        ifstream file2(pathCSV2);
        if (!file2.is_open()) {
            break;
        }
        file2.close();
        csvCount2++;
    }
    cout << "|" << setw(10) << left << (table1) << " |" << setw(10) << left << column1 << " |";
    cout << setw(10) << left << (table2) << " |" << setw(10) << left << column2 << " |" << endl;
    for (size_t csvi1 = 1; csvi1 < csvCount1; csvi1++) {
        filesystem::path pathCSV1 = filesystem::current_path() / jstab.scheme / table1 / (to_string(csvi1) + ".csv");
        rapidcsv::Document documentCSV1(pathCSV1.string());
        int idxColumn1 = documentCSV1.GetColumnIdx(column1);
        size_t strCount1 = documentCSV1.GetRowCount();
        for (size_t i=0; i < strCount1; i++) {
            for (size_t csvi2 = 1; csvi2 < csvCount2; csvi2++) {
                filesystem::path pathCSV2 = filesystem::current_path() / jstab.scheme / table2 / (to_string(csvi2) + ".csv");
                rapidcsv::Document documentCSV2(pathCSV2.string());
                int idxColumn2 = documentCSV2.GetColumnIdx(column2);
                size_t strCount2 = documentCSV2.GetRowCount();
                for (size_t j=0; j < strCount2; j++) {
                    cout << "|" << setw(10) << left << documentCSV1.GetCell<string>(0, i) << " |" << setw(10) << left << documentCSV1.GetCell<string>(idxColumn1, i) << " |";
                    cout << setw(10) << left << documentCSV2.GetCell<string>(0, j) << " |" << setw(10) << left << documentCSV2.GetCell<string>(idxColumn2, j) << " |" << endl;
                }
            }
        }
    }
}

void crossJoinDepend(string table1, string column1, string table2, string column2, TableJson& jstab) {
    size_t csvCount1 = 1;
    while (true) {
        filesystem::path pathCSV1 = filesystem::current_path() / jstab.scheme / table1 / ("res_" + to_string(csvCount1) + ".csv");
        ifstream file1(pathCSV1);
        if (!file1.is_open()) {
            break;
        }
        file1.close();
        csvCount1++;
    }
    size_t csvCount2 = 1;
    while (true) {
        filesystem::path pathCSV2 = filesystem::current_path() / jstab.scheme / table2 / ("res_" + to_string(csvCount2) + ".csv");
        ifstream file2(pathCSV2);
        if (!file2.is_open()) {
            break;
        }
        file2.close();
        csvCount2++;
    }
    cout << "|" << setw(10) << left << (table1) << " |" << setw(10) << left << column1 << " |";
    cout << setw(10) << left << (table2) << " |" << setw(10) << left << column2 << " |" << endl;
    for (size_t csvi1 = 1; csvi1 < csvCount1; csvi1++) {
        filesystem::path pathCSV1 = filesystem::current_path() / jstab.scheme / table1 / ("res_" + to_string(csvi1) + ".csv");
        rapidcsv::Document documentCSV1(pathCSV1.string());
        int idxColumn1 = documentCSV1.GetColumnIdx(column1);
        size_t strCount1 = documentCSV1.GetRowCount();
        for (size_t i=0; i < strCount1; i++) {
            for (size_t csvi2 = 1; csvi2 < csvCount2; csvi2++) {
                filesystem::path pathCSV2 = filesystem::current_path() / jstab.scheme / table2 / ("res_" + to_string(csvi2) + ".csv");
                rapidcsv::Document documentCSV2(pathCSV2.string());
                int idxColumn2 = documentCSV2.GetColumnIdx(column2);
                size_t strCount2 = documentCSV2.GetRowCount();
                for (size_t j=0; j < strCount2; j++) {
                    cout << "|" << setw(10) << left << documentCSV1.GetCell<string>(0, i) << " |" << setw(10) << left << documentCSV1.GetCell<string>(idxColumn1, i) << " |";
                    cout << setw(10) << left << documentCSV2.GetCell<string>(0, j) << " |" << setw(10) << left << documentCSV2.GetCell<string>(idxColumn2, j) << " |" << endl;
                }
            }
        }
    }
}

void filterTable(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2) {
    size_t i = 0;
    while (true) {
        size_t icsv = i/jstab.strCount + 1;
        size_t row = i%jstab.strCount;
        filesystem::path pathCSVfilter1 = filesystem::current_path() / jstab.scheme / tablefilter1 / (to_string(icsv) + ".csv");
        filesystem::path pathCSVfilter2 = filesystem::current_path() / jstab.scheme / tablefilter2 / (to_string(icsv) + ".csv");
        ifstream filefilter1(pathCSVfilter1);
        ifstream filefilter2(pathCSVfilter2);
        if (!filefilter1.is_open() || !filefilter2.is_open()) {
            break;
        }
        filefilter1.close();
        filefilter2.close();
        filesystem::path pathCSVRes1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        filesystem::path pathCSVRes2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        ifstream fileRes1(pathCSVRes1);
        if (!fileRes1.is_open()) {
            ofstream fileRes1Out(pathCSVRes1);
            if (!fileRes1Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes1Out.close();
        }
        else {
            fileRes1.close();
        }

        ifstream fileRes2(pathCSVRes2);
        if (!fileRes2.is_open()) {
            ofstream fileRes2Out(pathCSVRes2);
            if (!fileRes2Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes2Out.close();
        }
        else {
            fileRes2.close();
        }
        filesystem::path csvFirstPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / "1.csv";
        filesystem::path csvEndPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string());
        if (doc1.GetRowCount() == 0) {
            copyColNames(csvFirstPath1, csvEndPath1);
        }
        filesystem::path csvFirstPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / "1.csv";
        filesystem::path csvEndPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string());
        if (doc2.GetRowCount() == 0) {
            copyColNames(csvFirstPath2, csvEndPath2);
        }
        if (compareTableStr(tablefilter1, columnfilter1, tablefilter2, columnfilter2, icsv, row, jstab)) {
            rapidcsv::Document docfilter1(pathCSVfilter1.string());
            size_t columnsCount1 = docfilter1.GetColumnCount();
            ofstream csvResFile1(pathCSVRes1, ios::app);
            if (!csvResFile1.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount1; j++) {
                if (j+1 != columnsCount1) {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell;
                }
            }
            csvResFile1 << endl;
            csvResFile1.close();
            rapidcsv::Document docfilter2(pathCSVfilter2.string());
            size_t columnsCount2 = docfilter2.GetColumnCount();
            ofstream csvResFile2(pathCSVRes2, ios::app);
            if (!csvResFile2.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount2; j++) {
                if (j+1 != columnsCount2) {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell;
                }
            }
            csvResFile2 << endl;
            csvResFile2.close();
        }
        i++;
    }
}
// проверка условий
void filterStr(TableJson& jstab, string tablefilter1, string table, string columnfilter1, string sfilter) {
    size_t i = 0;
    while (true) {
        size_t icsv = i/jstab.strCount + 1;
        size_t row = i%jstab.strCount;
        filesystem::path pathCSVfilter1 = filesystem::current_path() / jstab.scheme / tablefilter1 / (to_string(icsv) + ".csv");
        ifstream filefilter1(pathCSVfilter1);
        if (!filefilter1.is_open()) {
            break;
        }
        filefilter1.close();
        filesystem::path pathCSVRes1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        filesystem::path pathCSVRes2 = filesystem::current_path() / jstab.scheme / table / ("res_" + to_string(icsv) + ".csv");
        ifstream fileRes1(pathCSVRes1);
        if (!fileRes1.is_open()) {
            ofstream fileRes1Out(pathCSVRes1);
            if (!fileRes1Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes1Out.close();
        }
        else {
            fileRes1.close();
        }

        ifstream fileRes2(pathCSVRes2);
        if (!fileRes2.is_open()) {
            ofstream fileRes2Out(pathCSVRes2);
            if (!fileRes2Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes2Out.close();
        }
        else {
            fileRes2.close();
        }
        filesystem::path csvFirstPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / "1.csv";
        filesystem::path csvEndPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string());
        if (doc1.GetRowCount() == 0) {
            copyColNames(csvFirstPath1, csvEndPath1);
        }
        filesystem::path csvFirstPath2 = filesystem::current_path() / jstab.scheme / table / "1.csv";
        filesystem::path csvEndPath2 = filesystem::current_path() / jstab.scheme / table / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string());
        if (doc2.GetRowCount() == 0) {
            copyColNames(csvFirstPath2, csvEndPath2);
        }
        if (matchesColumnValue(tablefilter1, columnfilter1, sfilter, icsv, row, jstab)) {
            rapidcsv::Document docfilter1(pathCSVfilter1.string());
            size_t columnsCount1 = docfilter1.GetColumnCount();
            ofstream csvResFile1(pathCSVRes1, ios::app);
            if (!csvResFile1.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount1; j++) {
                if (j+1 != columnsCount1) {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell;
                }
            }
            csvResFile1 << endl;
            csvResFile1.close();
            filesystem::path pathCSV2 = filesystem::current_path() / jstab.scheme / table / (to_string(icsv) + ".csv");
            rapidcsv::Document docfilter2(pathCSV2.string());
            size_t columnsCount2 = docfilter2.GetColumnCount();
            ofstream csvResFile2(pathCSVRes2, ios::app);
            if (!csvResFile2.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount2; j++) {
                if (j+1 != columnsCount2) {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell;
                }
            }
            csvResFile2 << endl;
            csvResFile2.close();
        }
        i++;
    }
}

// проверка условий
void filterAnd(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2, string tablefilter3, string columnfilter3, string sfilter) {
    size_t i = 0;
    while (true) {
        size_t icsv = i/jstab.strCount + 1;
        size_t row = i%jstab.strCount;
        filesystem::path pathCSVfilter1 = filesystem::current_path() / jstab.scheme / tablefilter1 / (to_string(icsv) + ".csv");
        filesystem::path pathCSVfilter2 = filesystem::current_path() / jstab.scheme / tablefilter2 / (to_string(icsv) + ".csv");
        ifstream filefilter1(pathCSVfilter1);
        ifstream filefilter2(pathCSVfilter2);
        if (!filefilter1.is_open() || !filefilter2.is_open()) {
            break;
        }
        filefilter1.close();
        filefilter2.close();
        filesystem::path pathCSVRes1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        filesystem::path pathCSVRes2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        ifstream fileRes1(pathCSVRes1);
        if (!fileRes1.is_open()) {
            ofstream fileRes1Out(pathCSVRes1);
            if (!fileRes1Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes1Out.close();
        }
        else {
            fileRes1.close();
        }

        ifstream fileRes2(pathCSVRes2);
        if (!fileRes2.is_open()) {
            ofstream fileRes2Out(pathCSVRes2);
            if (!fileRes2Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes2Out.close();
        }
        else {
            fileRes2.close();
        }
        filesystem::path csvFirstPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / "1.csv";
        filesystem::path csvEndPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string());
        if (doc1.GetRowCount() == 0) {
            copyColNames(csvFirstPath1, csvEndPath1);
        }
        filesystem::path csvFirstPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / "1.csv";
        filesystem::path csvEndPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string());
        if (doc2.GetRowCount() == 0) {
            copyColNames(csvFirstPath2, csvEndPath2);
        }
        if (compareTableStr(tablefilter1, columnfilter1, tablefilter2, columnfilter2, icsv, row, jstab) && matchesColumnValue(tablefilter3, columnfilter3, sfilter, icsv, row, jstab)) {
            rapidcsv::Document docfilter1(pathCSVfilter1.string());
            size_t columnsCount1 = docfilter1.GetColumnCount();
            ofstream csvResFile1(pathCSVRes1, ios::app);
            if (!csvResFile1.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount1; j++) {
                if (j+1 != columnsCount1) {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell;
                }
            }
            csvResFile1 << endl;
            csvResFile1.close();
            rapidcsv::Document docfilter2(pathCSVfilter2.string());
            size_t columnsCount2 = docfilter2.GetColumnCount();
            ofstream csvResFile2(pathCSVRes2, ios::app);
            if (!csvResFile2.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount2; j++) {
                if (j+1 != columnsCount2) {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell;
                }
            }
            csvResFile2 << endl;
            csvResFile2.close();
        }
        i++;
    }
}

// проверка условий
void filterOr(TableJson& jstab, string tablefilter1, string tablefilter2, string columnfilter1, string columnfilter2, string tablefilter3, string columnfilter3, string sfilter) {
    size_t i = 0;
    while (true) {
        size_t icsv = i/jstab.strCount + 1;
        size_t row = i%jstab.strCount;
        filesystem::path pathCSVfilter1 = filesystem::current_path() / jstab.scheme / tablefilter1 / (to_string(icsv) + ".csv");
        filesystem::path pathCSVfilter2 = filesystem::current_path() / jstab.scheme / tablefilter2 / (to_string(icsv) + ".csv");
        ifstream filefilter1(pathCSVfilter1);
        ifstream filefilter2(pathCSVfilter2);
        if (!filefilter1.is_open() || !filefilter2.is_open()) {
            break;
        }
        filefilter1.close();
        filefilter2.close();
        filesystem::path pathCSVRes1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        filesystem::path pathCSVRes2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        ifstream fileRes1(pathCSVRes1);
        if (!fileRes1.is_open()) {
            ofstream fileRes1Out(pathCSVRes1);
            if (!fileRes1Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes1Out.close();
        }
        else {
            fileRes1.close();
        }

        ifstream fileRes2(pathCSVRes2);
        if (!fileRes2.is_open()) {
            ofstream fileRes2Out(pathCSVRes2);
            if (!fileRes2Out.is_open()) {
                throw runtime_error("cannot open file.");
            }
            fileRes2Out.close();
        }
        else {
            fileRes2.close();
        }
        filesystem::path csvFirstPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / "1.csv";
        filesystem::path csvEndPath1 = filesystem::current_path() / jstab.scheme / tablefilter1 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc1(csvEndPath1.string());
        if (doc1.GetRowCount() == 0) {
            copyColNames(csvFirstPath1, csvEndPath1);
        }
        filesystem::path csvFirstPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / "1.csv";
        filesystem::path csvEndPath2 = filesystem::current_path() / jstab.scheme / tablefilter2 / ("res_" + to_string(icsv) + ".csv");
        rapidcsv::Document doc2(csvEndPath2.string());
        if (doc2.GetRowCount() == 0) {
            copyColNames(csvFirstPath2, csvEndPath2);
        }
        if (compareTableStr(tablefilter1, columnfilter1, tablefilter2, columnfilter2, icsv, row, jstab) || matchesColumnValue(tablefilter3, columnfilter3, sfilter, icsv, row, jstab)) {
            rapidcsv::Document docfilter1(pathCSVfilter1.string());
            size_t columnsCount1 = docfilter1.GetColumnCount();
            ofstream csvResFile1(pathCSVRes1, ios::app);
            if (!csvResFile1.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount1; j++) {
                if (j+1 != columnsCount1) {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter1.GetCell<string>(j, row);
                    csvResFile1 << currentCell;
                }
            }
            csvResFile1 << endl;
            csvResFile1.close();
            rapidcsv::Document docfilter2(pathCSVfilter2.string());
            size_t columnsCount2 = docfilter2.GetColumnCount();
            ofstream csvResFile2(pathCSVRes2, ios::app);
            if (!csvResFile2.is_open()) {
                throw runtime_error("cannot open file.");
            }
            for (size_t j=0; j < columnsCount2; j++) {
                if (j+1 != columnsCount2) {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell << ",";
                }
                else {
                    string currentCell = docfilter2.GetCell<string>(j, row);
                    csvResFile2 << currentCell;
                }
            }
            csvResFile2 << endl;
            csvResFile2.close();
        }
        i++;
    }
}

// проверка на точку в сообщении
bool containsDot(string message) {
    for (size_t i = 0; i < message[i]; i++) {
        if (message[i] == '.') {
            return true;
        }
    }
    return false;
}

// сравнение значений ячеек таблиц
bool compareTableStr(string table, string column, string tablefilter, string columnfilter, size_t csvNum, size_t row, TableJson& jstab) {
    filesystem::path pathCSV = filesystem::current_path() / jstab.scheme / table / (to_string(csvNum) + ".csv");
    filesystem::path pathCSVfilter = filesystem::current_path() / jstab.scheme / tablefilter / (to_string(csvNum) + ".csv");
    rapidcsv::Document doc(pathCSV.string());
    rapidcsv::Document docfilter(pathCSVfilter.string());
    int idxColumn = doc.GetColumnIdx(column);
    int idxColumnfilter = docfilter.GetColumnIdx(columnfilter);
    int strCount = doc.GetRowCount();
    int strCountfilter = docfilter.GetRowCount();
    if (row >= strCount || row >= strCountfilter) return false;
    if (doc.GetCell<string>(idxColumn, row) == docfilter.GetCell<string>(idxColumnfilter, row)) return true;
    else return false;
}

// сравнение ячейки с введенным значением
bool matchesColumnValue(string table, string column, string sfilter, size_t csvNum, size_t row, TableJson& jstab) {
    filesystem::path pathCSV = filesystem::current_path() / jstab.scheme / table / (to_string(csvNum) + ".csv");
    rapidcsv::Document doc(pathCSV.string());
    int idxColumn = doc.GetColumnIdx(column);
    int strCount = doc.GetRowCount();
    if (row >= strCount) return false;
    if (doc.GetCell<string>(idxColumn, row) == sfilter) return true;
    else return false;
}