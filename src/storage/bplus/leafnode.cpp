#include "eggshell/storage/bplus/leafnode.hpp"

#include "eggshell/storage/bplus/internalnode.hpp"
#include "eggshell/storage/bplus/node.hpp"

const uint32_t LeafNode::LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LeafNode::LEAF_NODE_NUM_CELLS_OFFSET =
    Node::COMMON_NODE_HEADER_SIZE;
const uint32_t LeafNode::LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LeafNode::LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LeafNode::LEAF_NODE_HEADER_SIZE =
    Node::COMMON_NODE_HEADER_SIZE + LeafNode::LEAF_NODE_NUM_CELLS_SIZE +
    LeafNode::LEAF_NODE_NEXT_LEAF_SIZE;
const uint32_t LeafNode::LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LeafNode::LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LeafNode::LEAF_NODE_VALUE_SIZE = Row::SIZE;
const uint32_t LeafNode::LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LeafNode::LEAF_NODE_CELL_SIZE =
    LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LeafNode::LEAF_NODE_SPACE_FOR_CELLS =
    Pager::PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LeafNode::LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const uint32_t LeafNode::LEAF_NODE_RIGHT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LeafNode::LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

uint32_t* LeafNode::num_cells(char* node) {
    return (uint32_t*)(node + LEAF_NODE_NUM_CELLS_OFFSET);
}

char* LeafNode::cell(char* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* LeafNode::key(char* node, uint32_t cell_num) {
    return (uint32_t*)cell(node, cell_num);
}

char* LeafNode::value(char* node, uint32_t cell_num) {
    return cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* LeafNode::next_leaf(char* node) {
    return (uint32_t*)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

void LeafNode::init(char* node) {
    Node::set_node_type(node, NodeType::leaf);
    Node::set_node_root(node, false);
    *num_cells(node) = 0;
    *next_leaf(node) = 0;
}

void LeafNode::split_and_insert(const Cursor& cursor, uint32_t key,
                                Row& value) {
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
