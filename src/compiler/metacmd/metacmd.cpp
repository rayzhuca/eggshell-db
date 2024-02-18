#include "modeldb/compiler/metacmd/metacmd.hpp"

#include <iostream>

#include "modeldb/compiler/metacmd/metacmdresult.hpp"
#include "modeldb/storage/bplus/internalnode.hpp"
#include "modeldb/storage/bplus/leafnode.hpp"
#include "modeldb/storage/bplus/node.hpp"
#include "modeldb/storage/row.hpp"

void print_constants() {
    printf("ROW_SIZE: %d\n", Row::SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", Node::COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNode::LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNode::LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n",
           LeafNode::LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNode::LEAF_NODE_MAX_CELLS);
}

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