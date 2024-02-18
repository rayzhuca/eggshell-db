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

uint32_t* node_parent(char* node) {
    return (uint32_t*)(node + PARENT_POINTER_OFFSET);
}

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
uint32_t get_node_max_key(Pager& pager, char* node);

void create_new_root(Table& table, uint32_t right_child_page_num);
};  // namespace Node

namespace InternalNode {

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key);
void insert(Table& table, uint32_t parent_page_num, uint32_t child_page_num);

}  // namespace InternalNode

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
    uint32_t old_max = Node::get_node_max_key(old_node);
    uint32_t new_page_num = cursor.table.pager.get_unused_page_num();
    char* new_node = cursor.table.pager.get(new_page_num);
    LeafNode::init(new_node);
    *Node::node_parent(new_node) = *Node::node_parent(old_node);
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
            uint32_t parent_page_num = *Node::node_parent(old_node);
            uint32_t new_max = Node::get_node_max_key(old_node);
            char* parent = cursor.table.pager.get(parent_page_num);

            InternalNode::update_internal_node_key(parent, old_max, new_max);
            InternalNode::insert(cursor.table, parent_page_num, new_page_num);
            return;
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
const uint32_t INVALID_PAGE_NUM = UINT32_MAX;

/* Keep this small for testing */
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

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

uint32_t* key(char* node, uint32_t key_num) {
    return (uint32_t*)((char*)cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

void init(char* node) {
    Node::set_node_type(node, NodeType::internal);
    Node::set_node_root(node, false);
    *num_keys(node) = 0;
    /*
    Necessary because the root page number is 0; by not initializing an internal
    node's right child to an invalid page number when initializing the node, we
    may end up with 0 as the node's right child, which makes the node a parent
    of the root
    */
    *right_child(node) = INVALID_PAGE_NUM;
}

uint32_t* child(char* node, uint32_t child_num) {
    uint32_t num_keys_val = *num_keys(node);
    if (child_num > num_keys_val) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num,
               num_keys_val);
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys_val) {
        uint32_t* right_child_p = right_child(node);
        if (*right_child_p == INVALID_PAGE_NUM) {
            printf(
                "Tried to access right child of node, but was invalid page\n");
            exit(EXIT_FAILURE);
        }
        return right_child_p;
    } else {
        uint32_t* child = cell(node, child_num);
        if (*child == INVALID_PAGE_NUM) {
            printf("Tried to access child %d of node, but was invalid page\n",
                   child_num);
            exit(EXIT_FAILURE);
        }
        return child;
    }
}

uint32_t find_child(char* node, uint32_t key) {
    /*
    Return the index of the child which should contain
    the given key.
    */
    uint32_t num_keys_val = *num_keys(node);

    /* Binary search */
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

    return min_index;
}

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = find_child(node, old_key);
    *key(node, old_child_index) = new_key;
}

Cursor find(Table& table, uint32_t page_num, uint32_t key) {
    char* node = table.pager.get(page_num);

    uint32_t child_index = find_child(node, key);
    uint32_t child_num = *child(node, child_index);
    char* child = table.pager.get(child_num);
    switch (Node::get_node_type(child)) {
        case NodeType::leaf:
            return LeafNode::find(table, child_num, key);
        case NodeType::internal:
            return find(table, child_num, key);
    }
}

void internal_node_split_and_insert(Table& table, uint32_t parent_page_num,
                                    uint32_t child_page_num);

void insert(Table& table, uint32_t parent_page_num, uint32_t child_page_num) {
    /*
    Add a new child/key pair to parent that corresponds to child
    */

    char* parent = table.pager.get(parent_page_num);
    char* child = table.pager.get(child_page_num);
    uint32_t child_max_key = Node::get_node_max_key(table.pager, child);
    uint32_t index = find_child(parent, child_max_key);

    uint32_t original_num_keys = *num_keys(parent);

    if (original_num_keys >= InternalNode::INTERNAL_NODE_MAX_CELLS) {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    uint32_t right_child_page_num = *right_child(parent);
    /*
    An internal node with a right child of INVALID_PAGE_NUM is empty
    */
    if (right_child_page_num == INVALID_PAGE_NUM) {
        *right_child(parent) = child_page_num;
        return;
    }

    char* right_child = table.pager.get(right_child_page_num);
    /*
    If we are already at the max number of cells for a node, we cannot increment
    before splitting. Incrementing without inserting a new key/child pair
    and immediately calling internal_node_split_and_insert has the effect
    of creating a new key at (max_cells + 1) with an uninitialized value
    */
    *num_keys(parent) = original_num_keys + 1;

    if (child_max_key > Node::get_node_max_key(table.pager, right_child)) {
        /* Replace right child */
        *InternalNode::child(parent, original_num_keys) = right_child_page_num;
        *key(parent, original_num_keys) =
            Node::get_node_max_key(table.pager, right_child);
        *InternalNode::right_child(parent) = child_page_num;
    } else {
        /* Make room for the new cell */
        for (uint32_t i = original_num_keys; i > index; i--) {
            uint32_t* destination = cell(parent, i);
            uint32_t* source = cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *InternalNode::child(parent, index) = child_page_num;
        *key(parent, index) = child_max_key;
    }
}

};  // namespace InternalNode

void InternalNode::internal_node_split_and_insert(Table& table,
                                                  uint32_t parent_page_num,
                                                  uint32_t child_page_num) {
    uint32_t old_page_num = parent_page_num;
    char* old_node = table.pager.get(parent_page_num);
    uint32_t old_max = Node::get_node_max_key(table.pager, old_node);

    char* child_p = table.pager.get(child_page_num);
    uint32_t child_max = Node::get_node_max_key(table.pager, child_p);

    uint32_t new_page_num = table.pager.get_unused_page_num();

    /*
    Declaring a flag before updating pointers which
    records whether this operation involves splitting the root -
    if it does, we will insert our newly created node during
    the step where the table's new root is created. If it does
    not, we have to insert the newly created node into its parent
    after the old node's keys have been transferred over. We are not
    able to do this if the newly created node's parent is not a newly
    initialized root node, because in that case its parent may have existing
    keys aside from our old node which we are splitting. If that is true, we
    need to find a place for our newly created node in its parent, and we
    cannot insert it at the correct index if it does not yet have any keys
    */
    uint32_t splitting_root = Node::is_node_root(old_node);

    char* parent;
    char* new_node;
    if (splitting_root) {
        Node::create_new_root(table, new_page_num);
        parent = table.pager.get(table.root_page_num);
        /*
        If we are splitting the root, we need to update old_node to point
        to the new root's left child, new_page_num will already point to
        the new root's right child
        */
        old_page_num = *child(parent, 0);
        old_node = table.pager.get(old_page_num);
    } else {
        parent = table.pager.get(*Node::node_parent(old_node));
        new_node = table.pager.get(new_page_num);
        init(new_node);
    }

    uint32_t* old_num_keys = num_keys(old_node);

    uint32_t cur_page_num = *right_child(old_node);
    char* cur = table.pager.get(cur_page_num);

    /*
    First put right child into new node and set right child of old node to
    invalid page number
    */
    insert(table, new_page_num, cur_page_num);
    *Node::node_parent(cur) = new_page_num;
    *right_child(old_node) = INVALID_PAGE_NUM;
    /*
    For each key until you get to the middle key, move the key and the child to
    the new node
    */
    for (int i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2;
         i--) {
        cur_page_num = *child(old_node, i);
        cur = table.pager.get(cur_page_num);

        insert(table, new_page_num, cur_page_num);
        *Node::node_parent(cur) = new_page_num;

        (*old_num_keys)--;
    }

    /*
    Set child before middle key, which is now the highest key, to be node's
    right child, and decrement number of keys
    */
    *right_child(old_node) = *child(old_node, *old_num_keys - 1);
    (*old_num_keys)--;

    /*
    Determine which of the two nodes after the split should contain the child to
    be inserted, and insert the child
    */
    uint32_t max_after_split = Node::get_node_max_key(table.pager, old_node);

    uint32_t destination_page_num =
        child_max < max_after_split ? old_page_num : new_page_num;

    insert(table, destination_page_num, child_page_num);
    *Node::node_parent(child_p) = destination_page_num;

    update_internal_node_key(parent, old_max,
                             Node::get_node_max_key(table.pager, old_node));

    if (!splitting_root) {
        insert(table, *Node::node_parent(old_node), new_page_num);
        *Node::node_parent(new_node) = *Node::node_parent(old_node);
    }
}

uint32_t Node::get_node_max_key(char* node) {
    switch (get_node_type(node)) {
        case NodeType::internal:
            return *InternalNode::key(node, *InternalNode::num_keys(node) - 1);
        case NodeType::leaf:
            return *LeafNode::key(node, *LeafNode::num_cells(node) - 1);
    }
}

uint32_t Node::get_node_max_key(Pager& pager, char* node) {
    if (get_node_type(node) == NodeType::leaf) {
        return *LeafNode::key(node, *LeafNode::num_cells(node) - 1);
    }
    char* right_child = pager.get(*InternalNode::right_child(node));
    return get_node_max_key(pager, right_child);
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
    if (get_node_type(root) == NodeType::internal) {
        InternalNode::init(right_child);
        InternalNode::init(left_child);
    }

    /* Left child has data copied from old root */
    memcpy(left_child, root, Pager::PAGE_SIZE);
    Node::set_node_root(left_child, false);

    if (get_node_type(left_child) == NodeType::internal) {
        char* child;
        for (int i = 0; i < *InternalNode::num_keys(left_child); i++) {
            child = table.pager.get(*InternalNode::child(left_child, i));
            *node_parent(child) = left_child_page_num;
        }
        child = table.pager.get(*InternalNode::right_child(left_child));
        *node_parent(child) = left_child_page_num;
    }

    /* Root node is a new internal node with one key and two children */
    InternalNode::init(root);
    Node::set_node_root(root, true);
    *InternalNode::num_keys(root) = 1;
    *InternalNode::child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(table.pager, left_child);
    *InternalNode::key(root, 0) = left_child_max_key;
    *InternalNode::right_child(root) = right_child_page_num;
    *node_parent(left_child) = table.root_page_num;
    *node_parent(right_child) = table.root_page_num;
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
            if (num_keys > 0) {
                for (uint32_t i = 0; i < num_keys; i++) {
                    child = *InternalNode::child(node, i);
                    print_tree(pager, child, indentation_level + 1);

                    indent(indentation_level + 1);
                    printf("- key %d\n", *InternalNode::key(node, i));
                }
                child = *InternalNode::right_child(node);
                print_tree(pager, child, indentation_level + 1);
            }
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