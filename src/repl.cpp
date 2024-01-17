#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

enum class MetaCmdResult { success, unrecognized };

MetaCmdResult do_meta_cmd(std::string input) {
    if (input == ".exit") {
        exit(EXIT_SUCCESS);
    } else {
        return MetaCmdResult::unrecognized;
    }
}

void read_input(std::string& str) {
    try {
        std::getline(std::cin, str);
        if (std::cin.eof()) {
            do_meta_cmd(".exit");
        }
    } catch (std::exception e) {
        std::cout << "error reading input\n";
        std::cout << e.what() << '\n';
    }
}

enum class CmdPrepareResult {
    success,
    unrecognized,
    syntax_error,
    string_too_long,
    id_out_of_range
};

enum class StatementType { insert, select };

enum class ExecuteResult { success, table_full };

struct Row {
    static const size_t COLUMN_USERNAME_SIZE = 32;
    static const size_t COLUMN_EMAIL_SIZE = 255;
    static const uint32_t ID_SIZE = sizeof(uint32_t);
    static const uint32_t USERNAME_SIZE =
        sizeof(char) * (COLUMN_USERNAME_SIZE + 1);
    static const uint32_t EMAIL_SIZE = sizeof(char) * (COLUMN_EMAIL_SIZE + 1);

    static const uint32_t ID_OFFSET = 0;
    static const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    static const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
    static const uint32_t SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    void serialize(void* destination) const {
        std::memcpy((char*)destination + ID_OFFSET, &id, ID_SIZE);
        std::memcpy((char*)destination + USERNAME_OFFSET, &username,
                    USERNAME_SIZE);
        std::memcpy((char*)destination + EMAIL_OFFSET, &email, EMAIL_SIZE);
    }

    void deserialize(const void* source) {
        std::memcpy(&id, (char*)source + ID_OFFSET, ID_SIZE);
        std::memcpy(&username, (char*)source + USERNAME_OFFSET, USERNAME_SIZE);
        std::memcpy(&email, (char*)source + EMAIL_OFFSET, EMAIL_SIZE);
    }
};

struct Cursor;

struct Table {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;
    const static uint32_t ROWS_PER_PAGE = PAGE_SIZE / Row::SIZE;
    const static uint32_t MAX_ROWS = ROWS_PER_PAGE * MAX_PAGES;

    uint32_t num_rows;
    void* pages[MAX_PAGES];

    Cursor start();

    Cursor end();

    Table() {
        for (size_t i = 0; i < MAX_PAGES; ++i) {
            pages[i] = nullptr;
        }
    }

    ~Table() {
        for (int i = 0; pages[i] != nullptr; ++i) {
            free(pages[i]);
        }
    }
};

struct Cursor {
    Table& table;
    uint32_t row_num;
    bool end_of_table;

    Cursor(Table& table, uint32_t row_num, bool end_of_table)
        : table{table}, row_num{row_num}, end_of_table{end_of_table} {
    }

    void* value() {
        uint32_t page_num = row_num / table.ROWS_PER_PAGE;
        void* page = table.pages[page_num];
        if (page == nullptr) {
            // Allocate memory only when we try to access page
            page = table.pages[page_num] = new int8_t[Table::PAGE_SIZE];
        }
        uint32_t row_offset = row_num % Table::ROWS_PER_PAGE;
        uint32_t byte_offset = row_offset * Row::SIZE;
        return (int8_t*)page + byte_offset;
    }

    void advance() {
        row_num += 1;
        if (row_num >= table.num_rows) {
            end_of_table = true;
        }
    }
};

Cursor Table::start() {
    return Cursor{*this, 0, false};
}

Cursor Table::end() {
    return Cursor{*this, num_rows, true};
}
struct Statement {
    StatementType type;
    Row row_to_insert;

    CmdPrepareResult prepare(std::string input) {
        if (input.starts_with("insert")) {
            type = StatementType::insert;
            std::stringstream stream(input);
            int64_t id;
            std::string w, username, email;
            stream >> w >> id >> username >> email;
            if (username.size() > Row::COLUMN_USERNAME_SIZE ||
                email.size() > Row::COLUMN_EMAIL_SIZE) {
                return CmdPrepareResult::string_too_long;
            }
            if (id < 0 || id > std::numeric_limits<uint32_t>::max()) {
                return CmdPrepareResult::id_out_of_range;
            }
            if (std::cin.fail()) {
                return CmdPrepareResult::syntax_error;
            }
            row_to_insert.id = id;
            strncpy(row_to_insert.username, username.c_str(),
                    sizeof(row_to_insert.username));
            strncpy(row_to_insert.email, email.c_str(),
                    sizeof(row_to_insert.email));
            return CmdPrepareResult::success;
        }
        if (input.starts_with("select")) {
            type = StatementType::select;
            return CmdPrepareResult::success;
        }
        return CmdPrepareResult::unrecognized;
    }

    ExecuteResult execute_insert(Table& table) {
        if (table.num_rows >= Table::MAX_ROWS) {
            return ExecuteResult::table_full;
        }

        row_to_insert.serialize(table.end().value());
        ++table.num_rows;

        return ExecuteResult::success;
    }

    ExecuteResult execute_select(Table& table) const {
        Row row;
        Cursor cursor = table.start();
        for (uint32_t i = 0; i < table.num_rows; i++) {
            row.deserialize(cursor.value());
            std::cout << row.id << ' ' << row.username << ' ' << row.email
                      << '\n';
            cursor.advance();
        }
        return ExecuteResult::success;
    }

    ExecuteResult execute(Table& table) {
        switch (type) {
            case (StatementType::insert):
                return execute_insert(table);
            case (StatementType::select):
                return execute_select(table);
        }
    }
};

int main() {
    Table table;
    std::string input;
    while (true) {
        std::cout << "modeldb > ";
        read_input(input);
        if (input[0] == '.') {
            switch (do_meta_cmd(input)) {
                case MetaCmdResult::success:
                    continue;
                case MetaCmdResult::unrecognized:
                    std::cout << "Unrecognized command \'" << input << "\'.\n";
                    continue;
            }
        } else {
            Statement statement;
            switch (statement.prepare(input)) {
                case (CmdPrepareResult::success):
                    break;
                case (CmdPrepareResult::id_out_of_range):
                    std::cout << "Id out of range\n";
                    continue;
                case (CmdPrepareResult::string_too_long):
                    std::cout << "String is too long.\n";
                    continue;
                case (CmdPrepareResult::syntax_error):
                    std::cout << "Syntax error. Could not parse statement.\n";
                    continue;
                case (CmdPrepareResult::unrecognized):
                    std::cout << "Unrecognized keyword at start of \'" << input
                              << "\'.\n";
                    continue;
            }
            switch (statement.execute(table)) {
                case (ExecuteResult::success):
                    std::cout << "Executed.\n";
                    break;
                case (ExecuteResult::table_full):
                    std::cout << "Error: Table full.\n";
                    break;
            }
        }
    }
    return EXIT_FAILURE;
}