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

enum class CmdPrepareResult { success, unrecognized };

enum class StatementType { insert, select };
struct Statement {
    StatementType type;
};

CmdPrepareResult prepare_statement(std::string input, Statement& statement) {
    if (input.starts_with("insert")) {
        statement.type = StatementType::insert;
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
            break;
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