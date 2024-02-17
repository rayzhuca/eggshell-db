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

enum class NodeType { internal, leaf };

namespace Node {

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

};  // namespace Node

namespace LeafNode {

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = Node::COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    Node::COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = Row::SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS =
    Table::PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* num_cells(void* node) {
    return (uint32_t*)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* key(void* node, uint32_t cell_num) {
    return (uint32_t*)cell(node, cell_num);
}

void* value(void* node, uint32_t cell_num) {
    return cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void init(void* node) {
    *num_cells(node) = 0;
}

};  // namespace LeafNode

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

struct Pager {
    std::fstream file;
    uint32_t file_length;
    void* pages[Table::MAX_PAGES];

    Pager(std::string filename)
        : file{filename, file.in | file.out | file.trunc} {
        if (file.fail()) {
            printf("Unable to open file\n");
            std::exit(EXIT_FAILURE);
        }

        file.ignore(std::numeric_limits<std::streamsize>::max());
        file_length = file.gcount();

        for (uint32_t i = 0; i < Table::MAX_PAGES; i++) {
            pages[i] = nullptr;
        }
    }

    void* get(uint32_t page_num) {
        if (page_num > Table::MAX_PAGES) {
            std::cout << "Tried to fetch page number out of bounds." << page_num
                      << " >= " << Table::MAX_PAGES << "\n";
            exit(EXIT_FAILURE);
        }

        if (pages[page_num] == nullptr) {
            // Cache miss. Allocate memory and load from file.
            void* page = new char[Table::PAGE_SIZE];
            uint32_t num_pages = file_length / Table::PAGE_SIZE;

            // We might save a partial page at the end of the file
            if (file_length % Table::PAGE_SIZE) {
                num_pages += 1;
            }

            if (page_num <= num_pages) {
                file.seekg(page_num * Table::PAGE_SIZE, file.beg);
                file.read((char*)page, Table::PAGE_SIZE);
                if (!file.good()) {
                    std::cout << "Error reading file: " << strerror(errno)
                              << "\n";
                    exit(EXIT_FAILURE);
                }
            }
            pages[page_num] = page;
        }

        return pages[page_num];
    }

    void flush(uint32_t page_num, uint32_t size) {
        if (pages[page_num] == NULL) {
            std::cout << "Tried to flush null page\n";
            exit(EXIT_FAILURE);
        }

        file.seekg(page_num * Table::PAGE_SIZE, file.beg);

        if (!file.good()) {
            std::cout << "Error seeking: " << strerror(errno) << "\n";
            exit(EXIT_FAILURE);
        }

        file.write(reinterpret_cast<char*>(pages[page_num]), size);

        if (!file.good()) {
            std::cout << "Error writing: " << errno << "\n";
            exit(EXIT_FAILURE);
        }
    }
};

struct Table {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;
    const static uint32_t ROWS_PER_PAGE = PAGE_SIZE / Row::SIZE;
    const static uint32_t MAX_ROWS = ROWS_PER_PAGE * MAX_PAGES;

    Pager pager;
    uint32_t num_rows;

    Cursor start();

    Cursor end();

    Table(std::string filename)
        : pager{filename}, num_rows{pager.file_length / Row::SIZE} {
    }

    ~Table() {
        uint32_t num_full_pages = num_rows / ROWS_PER_PAGE;

        for (uint32_t i = 0; i < num_full_pages; i++) {
            if (pager.pages[i] == nullptr) continue;
            pager.flush(i, PAGE_SIZE);
            delete pager.pages[i];
            pager.pages[i] = nullptr;
        }

        // There may be a partial page to write to the end of the file
        // This should not be needed after we switch to a B-tree
        uint32_t num_additional_rows = num_rows % ROWS_PER_PAGE;
        if (num_additional_rows > 0) {
            uint32_t page_num = num_full_pages;
            if (pager.pages[page_num]) {
                pager.flush(page_num, num_additional_rows * Row::SIZE);
                delete pager.pages[page_num];
                pager.pages[page_num] = nullptr;
            }
        }

        pager.file.close();
        if (pager.file.fail()) {
            std::cout << "Error closing db file.\n";
            exit(EXIT_FAILURE);
        }
        for (uint32_t i = 0; i < Table::MAX_PAGES; i++) {
            void* page = pager.pages[i];
            if (page) {
                free(page);
                pager.pages[i] = nullptr;
            }
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
        void* page = table.pager.get(page_num);
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
    Table table("test.db");
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