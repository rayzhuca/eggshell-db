#pragma once

#include <cstdint>

#include "modeldb/storage/bplus/nodetype.hpp"
#include "modeldb/storage/cursor.hpp"
#include "modeldb/storage/table.hpp"

namespace Node {

extern const uint32_t NODE_TYPE_SIZE;
extern const uint32_t NODE_TYPE_OFFSET;
extern const uint32_t IS_ROOT_SIZE;
extern const uint32_t IS_ROOT_OFFSET;
extern const uint32_t PARENT_POINTER_SIZE;
extern const uint32_t PARENT_POINTER_OFFSET;
extern const uint8_t COMMON_NODE_HEADER_SIZE;

uint32_t* node_parent(char* node);

NodeType get_node_type(char* node);

void set_node_type(char* node, NodeType type);

bool is_node_root(char* node);

void set_node_root(char* node, bool is_root);

uint32_t get_node_max_key(char* node);

uint32_t get_node_max_key(Pager& pager, char* node);

void create_new_root(Table& table, uint32_t right_child_page_num);
};  // namespace Node