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

struct Pager;
struct Cursor;

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

    /*
    Until we start recycling free pages, new pages will always
    go onto the end of the database file
    */
    uint32_t get_unused_page_num() {
        return num_pages;
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

struct Table {
    Pager pager;
    uint32_t root_page_num;
    Table(std::string filename);
    ~Table();
    Cursor start();
    Cursor find(uint32_t key);
};

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

bool is_node_root(char* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(char* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t get_node_max_key(char* node);

void create_new_root(Table& table, uint32_t right_child_page_num);
};  // namespace Node

namespace LeafNode {

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = Node::COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = Node::COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

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

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

uint32_t* num_cells(char* node) {
    return (uint32_t*)(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

char* cell(char* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* key(char* node, uint32_t cell_num) {
    return (uint32_t*)cell(node, cell_num);
}

char* value(char* node, uint32_t cell_num) {
    return cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* next_leaf(char* node) {
    return (uint32_t*)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void init(char* node) {
    Node::set_node_type(node, NodeType::leaf);
    Node::set_node_root(node, false);
    *num_cells(node) = 0;
    *next_leaf(node) = 0;
}

void insert(const Cursor& cursor, uint32_t key, Row& value);

void split_and_insert(const Cursor& cursor, uint32_t key, Row& value) {
    /*
    Create a new node and move half the cells over.
    Insert the new value in one of the two nodes.
    Update parent or create a new parent.
    */

    char* old_node = cursor.table.pager.get(cursor.page_num);
    uint32_t new_page_num = cursor.table.pager.get_unused_page_num();
    char* new_node = cursor.table.pager.get(new_page_num);
    LeafNode::init(new_node);
    *next_leaf(new_node) = *next_leaf(old_node);
    *next_leaf(old_node) = new_page_num;

    /*
    All existing keys plus new key should be divided
    evenly between old (left) and new (right) nodes.
    Starting from the right, move each key to correct position.
    */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        char* destination_node;
        if (i >= int32_t(LeafNode::LEAF_NODE_LEFT_SPLIT_COUNT)) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LeafNode::LEAF_NODE_LEFT_SPLIT_COUNT;
        char* destination =
            (char*)LeafNode::cell(destination_node, index_within_node);

        if (uint32_t(i) == cursor.cell_num) {
            value.serialize(
                LeafNode::value(destination_node, index_within_node));
            *LeafNode::key(destination_node, index_within_node) = key;
        } else if (uint32_t(i) > cursor.cell_num) {
            memcpy(destination, LeafNode::cell(old_node, i - 1),
                   LeafNode::LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, LeafNode::cell(old_node, i),
                   LeafNode::LEAF_NODE_CELL_SIZE);
        }

        /* Update cell count on both leaf nodes */
        *LeafNode::num_cells(old_node) = LeafNode::LEAF_NODE_LEFT_SPLIT_COUNT;
        *LeafNode::num_cells(new_node) = LeafNode::LEAF_NODE_RIGHT_SPLIT_COUNT;
        if (Node::is_node_root(old_node)) {
            return Node::create_new_root(cursor.table, new_page_num);
        } else {
            std::cout << "Need to implement updating parent after split\n";
            exit(EXIT_FAILURE);
        }
    }
}

Cursor find(Table& table, uint32_t page_num, uint32_t key);

};  // namespace LeafNode

namespace InternalNode {

/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = Node::COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = Node::COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

uint32_t* num_keys(char* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* right_child(char* node) {
    return (uint32_t*)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* cell(char* node, uint32_t cell_num) {
    return (uint32_t*)(node + INTERNAL_NODE_HEADER_SIZE +
                       cell_num * INTERNAL_NODE_CELL_SIZE);
}

void init(char* node) {
    Node::set_node_type(node, NodeType::internal);
    Node::set_node_root(node, false);
    *num_keys(node) = 0;
}

uint32_t* child(char* node, uint32_t child_num) {
    uint32_t num_keys_val = *num_keys(node);
    if (child_num > num_keys_val) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num,
               num_keys_val);
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys_val) {
        return right_child(node);
    } else {
        return cell(node, child_num);
    }
}

uint32_t* key(char* node, uint32_t key_num) {
    return (uint32_t*)(cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

Cursor find(Table& table, uint32_t page_num, uint32_t key) {
    char* node = table.pager.get(page_num);
    uint32_t num_keys_val = *num_keys(node);

    /* Binary search to find index of child to search */
    uint32_t min_index = 0;
    uint32_t max_index = num_keys_val; /* there is one more child than key */

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *InternalNode::key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    uint32_t child_num = *child(node, min_index);
    char* child = table.pager.get(child_num);
    switch (Node::get_node_type(child)) {
        case NodeType::leaf:
            return LeafNode::find(table, child_num, key);
        case NodeType::internal:
            return find(table, child_num, key);
    }
}

};  // namespace InternalNode

uint32_t Node::get_node_max_key(char* node) {
    switch (get_node_type(node)) {
        case NodeType::internal:
            return *InternalNode::key(node, *InternalNode::num_keys(node) - 1);
        case NodeType::leaf:
            return *LeafNode::key(node, *LeafNode::num_cells(node) - 1);
    }
}

void Node::create_new_root(Table& table, uint32_t right_child_page_num) {
    /*
    Handle splitting the root.
    Old root copied to new page, becomes left child.
    Address of right child passed in.
    Re-initialize root page to contain the new root node.
    New root node points to two children.
    */

    char* root = table.pager.get(table.root_page_num);
    char* right_child = table.pager.get(right_child_page_num);
    uint32_t left_child_page_num = table.pager.get_unused_page_num();
    char* left_child = table.pager.get(left_child_page_num);

    /* Left child has data copied from old root */
    memcpy(left_child, root, Pager::PAGE_SIZE);
    Node::set_node_root(left_child, false);

    /* Root node is a new internal node with one key and two children */
    InternalNode::init(root);
    Node::set_node_root(root, true);
    *InternalNode::num_keys(root) = 1;
    *InternalNode::child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = Node::get_node_max_key(left_child);
    *InternalNode::key(root, 0) = left_child_max_key;
    *InternalNode::right_child(root) = right_child_page_num;
}

void print_constants() {
    printf("ROW_SIZE: %d\n", Row::SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", Node::COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNode::LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNode::LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n",
           LeafNode::LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNode::LEAF_NODE_MAX_CELLS);
}

Table::Table(std::string filename) : pager{filename}, root_page_num{0} {
    if (pager.num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        char* root_node = pager.get(0);
        LeafNode::init(root_node);
        Node::set_node_root(root_node, true);
    }
}

Table::~Table() {
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

Cursor Table::start() {
    Cursor cursor = find(0);

    char* node = pager.get(cursor.page_num);
    uint32_t num_cells = *LeafNode::num_cells(node);
    cursor.end_of_table = num_cells == 0;

    return cursor;
}

Cursor Table::find(uint32_t key) {
    char* root_node = pager.get(root_page_num);

    if (Node::get_node_type(root_node) == NodeType::leaf) {
        return LeafNode::find(*this, root_page_num, key);
    } else {
        return InternalNode::find(*this, root_page_num, key);
    }
}

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
        /* Advance to next leaf node */
        uint32_t next_page_num = *LeafNode::next_leaf(node);
        if (next_page_num == 0) {
            /* This was rightmost leaf */
            end_of_table = true;
        } else {
            page_num = next_page_num;
            cell_num = 0;
        }
    }
}

void LeafNode::insert(const Cursor& cursor, uint32_t key, Row& value) {
    char* node = cursor.table.pager.get(cursor.page_num);

    uint32_t num_cells = *LeafNode::num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        LeafNode::split_and_insert(cursor, key, value);
        return;
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

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        printf("  ");
    }
}

void print_tree(Pager& pager, uint32_t page_num, uint32_t indentation_level) {
    char* node = pager.get(page_num);
    uint32_t num_keys, child;

    switch (Node::get_node_type(node)) {
        case (NodeType::leaf):
            num_keys = *LeafNode::num_cells(node);
            indent(indentation_level);
            printf("- leaf (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                printf("- %d\n", *LeafNode::key(node, i));
            }
            break;
        case (NodeType::internal):
            num_keys = *InternalNode::num_keys(node);
            indent(indentation_level);
            printf("- internal (size %d)\n", num_keys);
            for (uint32_t i = 0; i < num_keys; i++) {
                child = *InternalNode::child(node, i);
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                printf("- key %d\n", *InternalNode::key(node, i));
            }
            child = *InternalNode::right_child(node);
            print_tree(pager, child, indentation_level + 1);
            break;
    }
}

MetaCmdResult do_meta_cmd(std::string input, Table& table) {
    if (input == ".exit") {
        return MetaCmdResult::exit;
    } else if (input == ".constants") {
        print_constants();
        return MetaCmdResult::success;
    } else if (input == ".btree") {
        std::cout << "Tree:\n";
        print_tree(table.pager, 0, 0);
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