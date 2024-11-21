#include "header.h"

// создает таблицу (если нету)
Table* createOrGetTable(Table*& head, Table*& tail, const std::string& tableName) {
    Table* current = head;
    while (current && current->name != tableName) {
        current = current->next;
    }

    if (!current) {
        Table* newTable = new Table{tableName, nullptr, nullptr};
        if (!head) {
            head = newTable;
            tail = newTable;
        } else {
            tail->next = newTable;
            tail = newTable;
        }
        return newTable;
    }
    return current;
}

// создает локер
void initializeLockFile(const filesystem::path& tableDir, const std::string& tableName) {
    filesystem::path lockFile = tableDir / (tableName + "_lock.txt");
    if (!filesystem::exists(lockFile)) {
        std::ofstream lock(lockFile);
        lock << "unlocked";
        lock.close();
    }
}

// создает файл первичного ключа
void initializePrimaryKeyFile(const filesystem::path& tableDir, const std::string& pkColumn) {
    filesystem::path sequenceFile = tableDir / (pkColumn + "_pk_sequence.txt");
    if (!filesystem::exists(sequenceFile)) {
        std::ofstream seq(sequenceFile);
        seq << "0";
        seq.close();
    }
}

// создает новый csv
bool initializeCsvFile(const filesystem::path& csvFilePath, const std::string& pkColumn) {
    if (!filesystem::exists(csvFilePath)) {
        std::ofstream csv(csvFilePath);
        if (csv.is_open()) {
            csv << pkColumn << ",";
            return true; 
        }
    }
    return false; //если файл уже был
}

// обновляет столбцы
void updateColumns(Node*& headColumn, Node*& tailColumn, const json& columns, std::ofstream& csv, bool isNewCsv) {
    for (size_t i = 0; i < columns.size(); ++i) {
        std::string columnName = columns[i].get<std::string>();

        // проверка на существование
        Node* currentColumn = headColumn;
        while (currentColumn && currentColumn->data != columnName) {
            currentColumn = currentColumn->next;
        }

        if (!currentColumn) {
            // добавляем новый столбец
            Node* newColumn = new Node{columnName, nullptr};
            if (!headColumn) {
                headColumn = newColumn;
                tailColumn = newColumn;
            } else {
                tailColumn->next = newColumn;
                tailColumn = newColumn;
            }

            // в новый csv вставляем заголовок
            if (isNewCsv) {
                csv << columnName;
                if (i < columns.size() - 1) {
                    csv << ",";
                }
            }
        }
    }

    if (isNewCsv) {
        csv << std::endl;
    }
}

void createOrUpdateFiles(const filesystem::path& schemePath, const json& jsonStruct, TableJson& jstable) {
    Table* head = jstable.head;
    Table* tail = jstable.head;

    // поиск конца списка
    while (tail && tail->next != nullptr) {
        tail = tail->next;
    }

    for (const auto& table : jsonStruct.items()) {
        filesystem::path tableDir = schemePath / table.key();

        // создание директории
        if (!filesystem::exists(tableDir)) {
            filesystem::create_directory(tableDir);
        }

        // создание таблицы
        Table* currentTable = createOrGetTable(head, tail, table.key());

        // вызываем создание файлов
        initializeLockFile(tableDir, table.key());
        std::string pkColumn = table.key();
        initializePrimaryKeyFile(tableDir, pkColumn);

        // обновляем столбцы
        Node* headColumn = currentTable->column;
        Node* tailColumn = nullptr;
        if (headColumn) {
            tailColumn = headColumn;
            while (tailColumn->next != nullptr) {
                tailColumn = tailColumn->next;
            }
        }

        filesystem::path csvFile = tableDir / "1.csv";
        bool isNewCsv = initializeCsvFile(csvFile, pkColumn);

        // если файл новый обновляем заголовки
        std::ofstream csv;
        if (isNewCsv) {
            csv.open(csvFile, std::ios::app);
        }
        updateColumns(headColumn, tailColumn, table.value(), csv, isNewCsv);

        currentTable->column = headColumn;
    }

    jstable.head = head;
}

void parser(TableJson& jstab) {
    ifstream file("schema.json");
    if (!file.is_open()) {
        throw runtime_error("cannot open file.");
    }

    json jspars;
    try {
        file >> jspars;
    } catch (const json::parse_error& e) {
        throw runtime_error(string("json parsing error: ") + e.what());
    }
    file.close();

    if (!jspars.contains("name") || !jspars.contains("structure") || !jspars.contains("tuples_limit")) {
        throw runtime_error("missing required keys");
        return;
    }

    jstab.scheme = jspars["name"];
    filesystem::path schemePath = filesystem::current_path() / jstab.scheme;

    if (!filesystem::exists(schemePath)) {
        filesystem::create_directory(schemePath);
    }

    createOrUpdateFiles(schemePath, jspars["structure"], jstab);
    jstab.strCount = jspars["tuples_limit"];
}