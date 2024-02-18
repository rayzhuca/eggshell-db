#include "modeldb/storage/bplus/internalnode.hpp"

#include "modeldb/storage/bplus/leafnode.hpp"
#include "modeldb/storage/bplus/node.hpp"
#include "modeldb/storage/cursor.hpp"

/*
 * Internal Node Header Layout
 */
const uint32_t InternalNode::INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t InternalNode::INTERNAL_NODE_NUM_KEYS_OFFSET =
    Node::COMMON_NODE_HEADER_SIZE;
const uint32_t InternalNode::INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t InternalNode::INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t InternalNode::INTERNAL_NODE_HEADER_SIZE =
    Node::COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE +
    INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const uint32_t InternalNode::INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t InternalNode::INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t InternalNode::INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t InternalNode::INVALID_PAGE_NUM = UINT32_MAX;

/* Keep this small for testing */
const uint32_t InternalNode::INTERNAL_NODE_MAX_CELLS = 3;

uint32_t* InternalNode::num_keys(char* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* InternalNode::right_child(char* node) {
    return (uint32_t*)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* InternalNode::cell(char* node, uint32_t cell_num) {
    return (uint32_t*)(node + INTERNAL_NODE_HEADER_SIZE +
                       cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* InternalNode::key(char* node, uint32_t key_num) {
    return (uint32_t*)((char*)cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

void InternalNode::init(char* node) {
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

uint32_t* InternalNode::child(char* node, uint32_t child_num) {
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

uint32_t InternalNode::find_child(char* node, uint32_t key) {
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

void InternalNode::update_internal_node_key(char* node, uint32_t old_key,
                                            uint32_t new_key) {
    uint32_t old_child_index = find_child(node, old_key);
    *key(node, old_child_index) = new_key;
}

Cursor InternalNode::find(Table& table, uint32_t page_num, uint32_t key) {
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

void InternalNode::internal_node_split_and_insert(Table& table,
                                                  uint32_t parent_page_num,
                                                  uint32_t child_page_num);

void InternalNode::insert(Table& table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
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