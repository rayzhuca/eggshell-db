#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

void read_input(std::string& str) {
    try {
        std::getline(std::cin, str);
    } catch (std::ifstream::failure e) {
        std::cout << "error reading input\n";
        std::cout << e.what() << '\n';
    }
}

enum class MetaCmdResult { success, unrecognized };

MetaCmdResult do_meta_cmd(std::string input) {
    if (input == ".exit") {
        exit(EXIT_SUCCESS);
    } else {
        return MetaCmdResult::unrecognized;
    }
}

enum class CmdPrepareResult { success, unrecognized, syntax_error };

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

struct Table {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;
    const static uint32_t ROWS_PER_PAGE = PAGE_SIZE / Row::SIZE;
    const static uint32_t MAX_ROWS = ROWS_PER_PAGE * MAX_PAGES;

    uint32_t num_rows;
    void* pages[MAX_PAGES];

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

    void* row_slot(uint32_t row_num) {
        uint32_t page_num = row_num / ROWS_PER_PAGE;
        void* page = pages[page_num];
        if (page == nullptr) {
            // Allocate memory only when we try to access page
            page = pages[page_num] = new uint8_t[PAGE_SIZE];
        }
        uint32_t row_offset = row_num % ROWS_PER_PAGE;
        uint32_t byte_offset = row_offset * Row::SIZE;
        return (char*)page + byte_offset;
    }
};
struct Statement {
    StatementType type;
    Row row_to_insert;

    CmdPrepareResult prepare(std::string input) {
        if (input.starts_with("insert")) {
            type = StatementType::insert;
            std::stringstream stream(input);
            std::string w;
            stream >> w >> row_to_insert.id >> row_to_insert.username >>
                row_to_insert.email;
            if (std::cin.fail()) {
                return CmdPrepareResult::syntax_error;
            }
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

        row_to_insert.serialize(table.row_slot(table.num_rows));
        ++table.num_rows;

        return ExecuteResult::success;
    }

    ExecuteResult execute_select(Table& table) const {
        Row row;
        for (uint32_t i = 0; i < table.num_rows; i++) {
            row.deserialize(table.row_slot(i));
            std::cout << row.id << ' ' << row.username << ' ' << row.email
                      << '\n';
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
                case (CmdPrepareResult::syntax_error):
                    printf("Syntax error. Could not parse statement.\n");
                    continue;
                case (CmdPrepareResult::unrecognized):
                    std::cout << "Unrecognized keyword at start of \'" << input
                              << "\'.\n";
                    continue;
            }
            switch (statement.execute(table)) {
                case (ExecuteResult::success):
                    printf("Executed.\n");
                    break;
                case (ExecuteResult::table_full):
                    printf("Error: Table full.\n");
                    break;
            }
        }
    }
    return EXIT_FAILURE;
}