#include <fstream>
#include <iostream>
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

    void serialize(void* destination) {
        std::memcpy(destination + ID_OFFSET, &id, ID_SIZE);
        std::memcpy(destination + USERNAME_OFFSET, &username, USERNAME_SIZE);
        std::memcpy(destination + EMAIL_OFFSET, &email, EMAIL_SIZE);
    }

    void deserialize(void* source) {
        std::memcpy(&id, source + ID_OFFSET, ID_SIZE);
        std::memcpy(&username, source + USERNAME_OFFSET, USERNAME_SIZE);
        std::memcpy(&email, source + EMAIL_OFFSET, EMAIL_SIZE);
    }
};

struct Table {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;
    const static uint32_t ROWS_PER_PAGE = PAGE_SIZE / Row::SIZE;
    const static uint32_t MAX_ROWS = ROWS_PER_PAGE * MAX_PAGES;

    uint32_t num_rows;
    void* pages[MAX_PAGES];
};
struct Statement {
    StatementType type;
    Row row_to_insert;
};

CmdPrepareResult prepare_statement(std::string input, Statement& statement) {
    if (input.starts_with("insert")) {
        statement.type = StatementType::insert;
        std::cin >> statement.row_to_insert.id >>
            statement.row_to_insert.username >> statement.row_to_insert.email;
        if (std::cin.fail()) {
            return CmdPrepareResult::syntax_error;
        }
        return CmdPrepareResult::success;
    }
    if (input.starts_with("select")) {
        statement.type = StatementType::select;
        return CmdPrepareResult::success;
    }
    return CmdPrepareResult::unrecognized;
}

void execute_statement(const Statement& statement) {
    switch (statement.type) {
        case (StatementType::insert):
            std::cout << "insert placeholder\n";
        case (StatementType::select):
            std::cout << "select placeholder\n";
            break;
    }
}

int main() {
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
            switch (prepare_statement(input, statement)) {
                case (CmdPrepareResult::success):
                    break;
                case (CmdPrepareResult::unrecognized):
                    std::cout << "Unrecognized keyword at start of \'" << input
                              << "\'\n";
                    continue;
            }

            execute_statement(statement);
            std::cout << "Executed.\n";
        }
    }
    return EXIT_FAILURE;
}