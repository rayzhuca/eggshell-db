#include "eggshell/storage/bplus/node.hpp"

#include "eggshell/storage/bplus/internalnode.hpp"
#include "eggshell/storage/bplus/leafnode.hpp"

const uint32_t Node::NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t Node::NODE_TYPE_OFFSET = 0;
const uint32_t Node::IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t Node::IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t Node::PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t Node::PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t Node::COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

uint32_t* Node::node_parent(char* node) {
    return (uint32_t*)(node + PARENT_POINTER_OFFSET);
}

NodeType Node::get_node_type(char* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return static_cast<NodeType>(value);
}

void Node::set_node_type(char* node, NodeType type) {
    uint8_t value = static_cast<uint8_t>(type);
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool Node::is_node_root(char* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void Node::set_node_root(char* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
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