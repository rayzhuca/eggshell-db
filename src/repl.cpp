#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

enum class MetaCmdResult { success, unrecognized, exit };

void read_input(std::string& str) {
    try {
        std::getline(std::cin, str);
        if (std::cin.eof()) {
            str = ".exit";
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

enum class ExecuteResult { success, table_full, duplicate_key };

enum class NodeType { internal, leaf };

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

    void serialize(char* destination) const {
        std::memcpy(destination + ID_OFFSET, &id, ID_SIZE);
        std::memcpy(destination + USERNAME_OFFSET, &username, USERNAME_SIZE);
        std::memcpy(destination + EMAIL_OFFSET, &email, EMAIL_SIZE);
    }

    void deserialize(const char* source) {
        std::memcpy(&id, source + ID_OFFSET, ID_SIZE);
        std::memcpy(&username, source + USERNAME_OFFSET, USERNAME_SIZE);
        std::memcpy(&email, source + EMAIL_OFFSET, EMAIL_SIZE);
    }
};

struct Table;

struct Cursor {
    Table& table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;

    Cursor(Table& table, uint32_t page_num, uint32_t cell_num,
           bool end_of_table);

    Cursor(Table&& table, uint32_t page_num, uint32_t cell_num,
           bool end_of_table) = delete;

    char* value();
    void advance();
};

struct Pager {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;

    std::fstream file;
    uint32_t file_length;
    uint32_t num_pages;
    char* pages[MAX_PAGES];

    Pager(std::string filename)
        : file{filename, file.in | file.out | file.binary} {
        if (file.fail()) {
            printf("Unable to open file\n");
            std::exit(EXIT_FAILURE);
        }

        file.seekg(0, file.end);
        file_length = file.tellg();
        num_pages = file_length / PAGE_SIZE;

        if (file_length % PAGE_SIZE != 0) {
            std::cout
                << "Db file is not a whole number of pages. Corrupt file\n";
            exit(EXIT_FAILURE);
        }

        for (uint32_t i = 0; i < MAX_PAGES; i++) {
            pages[i] = nullptr;
        }
    }

    char* get(uint32_t page_num) {
        if (page_num > MAX_PAGES) {
            std::cout << "Tried to fetch page number out of bounds. "
                      << page_num << " >= " << MAX_PAGES << "\n";
            exit(EXIT_FAILURE);
        }

        if (pages[page_num] == nullptr) {
            // Cache miss. Allocate memory and load from file.
            char* page = new char[PAGE_SIZE];
            // uint32_t num_pages = file_length / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if (file_length % PAGE_SIZE) {
                num_pages += 1;
            }
            if (page_num <= num_pages) {
                file.clear();
                file.seekg(page_num * PAGE_SIZE, file.beg);
                file.clear();
                file.read(page, PAGE_SIZE);
                file.clear();
                if (!file) {
                    std::cout << "Error reading file: " << strerror(errno)
                              << "\n";
                    exit(EXIT_FAILURE);
                }
            }
            pages[page_num] = page;

            if (page_num >= num_pages) {
                num_pages = page_num + 1;
            }
        }
        return pages[page_num];
    }

    void flush(uint32_t page_num) {
        if (pages[page_num] == NULL) {
            std::cout << "Tried to flush null page\n";
            exit(EXIT_FAILURE);
        }
        file.seekg(page_num * PAGE_SIZE, file.beg);
        file.clear();

        if (!file) {
            std::cout << "Error seeking: " << strerror(errno) << "\n";
            exit(EXIT_FAILURE);
        }

        file.write(pages[page_num], PAGE_SIZE);

        if (!file) {
            std::cout << "Error writing: " << errno << "\n";
            exit(EXIT_FAILURE);
        }
    }
};

namespace Node {

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

NodeType get_node_type(char* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return static_cast<NodeType>(value);
}

void set_node_type(char* node, NodeType type) {
    uint8_t value = static_cast<uint8_t>(type);
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

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
    Pager::PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* num_cells(char* node) {
    return (uint32_t*)node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t* cell(char* node, uint32_t cell_num) {
    return (uint32_t*)node + LEAF_NODE_HEADER_SIZE +
           cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* key(char* node, uint32_t cell_num) {
    return (uint32_t*)cell(node, cell_num);
}

char* value(char* node, uint32_t cell_num) {
    return (char*)cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void init(char* node) {
    Node::set_node_type(node, NodeType::leaf);
    *num_cells(node) = 0;
}

void insert(const Cursor& cursor, uint32_t key, Row& value);

Cursor find(Table& table, uint32_t page_num, uint32_t key);

};  // namespace LeafNode

void print_constants() {
    printf("ROW_SIZE: %d\n", Row::SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", Node::COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNode::LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNode::LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n",
           LeafNode::LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNode::LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(char* node) {
    uint32_t num_cells = *LeafNode::num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *LeafNode::key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

struct Table {
    Pager pager;
    uint32_t root_page_num;

    Table(std::string filename) : pager{filename}, root_page_num{0} {
        if (pager.num_pages == 0) {
            // New database file. Initialize page 0 as leaf node.
            char* root_node = pager.get(0);
            LeafNode::init(root_node);
        }
    }

    ~Table() {
        for (uint32_t i = 0; i < pager.num_pages; i++) {
            if (pager.pages[i] == nullptr) continue;
            pager.flush(i);
            delete[] pager.pages[i];
            pager.pages[i] = nullptr;
        }

        pager.file.close();
        if (!pager.file) {
            std::cout << "Error closing db file.\n";
            exit(EXIT_FAILURE);
        }
        for (uint32_t i = 0; i < Pager::MAX_PAGES; i++) {
            char* page = pager.pages[i];
            if (page) {
                delete[] page;
                pager.pages[i] = nullptr;
            }
        }
    }

    Cursor start();

    Cursor find(uint32_t key) {
        char* root_node = pager.get(root_page_num);

        if (Node::get_node_type(root_node) == NodeType::leaf) {
            return LeafNode::find(*this, root_page_num, key);
        } else {
            std::cout << "Need to implement searching an internal node\n";
            exit(EXIT_FAILURE);
        }
    }
};

Cursor::Cursor(Table& table, uint32_t page_num, uint32_t cell_num,
               bool end_of_table)
    : table{table},
      page_num{page_num},
      cell_num{cell_num},
      end_of_table{end_of_table} {
}

char* Cursor::value() {
    char* page = table.pager.get(page_num);
    return LeafNode::value(page, cell_num);
}

void Cursor::advance() {
    char* node = table.pager.get(page_num);
    cell_num += 1;
    if (cell_num >= *LeafNode::num_cells(node)) {
        end_of_table = true;
    }
}

Cursor Table::start() {
    char* root_node = pager.get(root_page_num);
    uint32_t num_cells = *LeafNode::num_cells(root_node);
    return Cursor{*this, root_page_num, 0, num_cells == 0};
}

void LeafNode::insert(const Cursor& cursor, uint32_t key, Row& value) {
    char* node = cursor.table.pager.get(cursor.page_num);

    uint32_t num_cells = *LeafNode::num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        std::cout << "Need to implement splitting a leaf node.\n";
        exit(EXIT_FAILURE);
    }

    if (cursor.cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor.cell_num; i--) {
            memcpy(LeafNode::cell(node, i), LeafNode::cell(node, i - 1),
                   LEAF_NODE_CELL_SIZE);
        }
    }

    *LeafNode::num_cells(node) += 1;
    *LeafNode::key(node, cursor.cell_num) = key;
    value.serialize(LeafNode::value(node, cursor.cell_num));
}

Cursor LeafNode::find(Table& table, uint32_t page_num, uint32_t key) {
    char* node = table.pager.get(page_num);
    uint32_t num_cells = *LeafNode::num_cells(node);

    Cursor cursor{table, page_num, uint32_t(-1), false};

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *LeafNode::key(node, index);
        if (key == key_at_index) {
            cursor.cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor.cell_num = min_index;
    return cursor;
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
        char* node = table.pager.get(table.root_page_num);
        uint32_t num_cells = *LeafNode::num_cells(node);
        if (num_cells >= LeafNode::LEAF_NODE_MAX_CELLS) {
            return ExecuteResult::table_full;
        }

        uint32_t key_to_insert = row_to_insert.id;
        Cursor cursor = table.find(key_to_insert);
        if (cursor.cell_num < num_cells) {
            uint32_t key_at_index = *LeafNode::key(node, cursor.cell_num);
            if (key_at_index == key_to_insert) {
                return ExecuteResult::duplicate_key;
            }
        }

        LeafNode::insert(cursor, row_to_insert.id, row_to_insert);

        return ExecuteResult::success;
    }

    ExecuteResult execute_select(Table& table) const {
        Row row;
        Cursor cursor = table.start();

        while (!cursor.end_of_table) {
            row.deserialize(cursor.value());
            std::cout << "(" << row.id << ", " << row.username << ", "
                      << row.email << ")\n";
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

MetaCmdResult do_meta_cmd(std::string input, Table& table) {
    if (input == ".exit") {
        return MetaCmdResult::exit;
    } else if (input == ".constants") {
        print_constants();
        return MetaCmdResult::success;
    } else if (input == ".btree") {
        std::cout << "Tree:\n";
        print_leaf_node(table.pager.get(0));
        return MetaCmdResult::success;
    } else {
        return MetaCmdResult::unrecognized;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Must supply a database filename.\n";
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table table(filename);
    std::string input;

    while (true) {
        std::cout << "modeldb > ";
        read_input(input);
        if (input[0] == '.') {
            switch (do_meta_cmd(input, table)) {
                case MetaCmdResult::success:
                    continue;
                case MetaCmdResult::unrecognized:
                    std::cout << "Unrecognized command \'" << input << "\'.\n";
                    continue;
                case MetaCmdResult::exit:
                    break;
            }
            break;
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
                case (ExecuteResult::duplicate_key):
                    std::cout << "Error: Duplicate key.\n";
                    break;
                case (ExecuteResult::table_full):
                    std::cout << "Error: Table full.\n";
                    break;
            }
        }
    }
    return EXIT_SUCCESS;
}