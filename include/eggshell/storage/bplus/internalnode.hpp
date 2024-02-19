#pragma once

#include <cstdint>

#include "eggshell/storage/table.hpp"

namespace InternalNode {

/*
 * Internal Node Header Layout
 */
extern const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE;
extern const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET;
extern const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE;
extern const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET;
extern const uint32_t INTERNAL_NODE_HEADER_SIZE;

/*
 * Internal Node Body Layout
 */
extern const uint32_t INTERNAL_NODE_KEY_SIZE;
extern const uint32_t INTERNAL_NODE_CHILD_SIZE;
extern const uint32_t INTERNAL_NODE_CELL_SIZE;
extern const uint32_t INVALID_PAGE_NUM;

/* Keep this small for testing */
extern const uint32_t INTERNAL_NODE_MAX_CELLS;

uint32_t* num_keys(char* node);

uint32_t* right_child(char* node);
uint32_t* cell(char* node, uint32_t cell_num);

uint32_t* key(char* node, uint32_t key_num);

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key);
void insert(Table& table, uint32_t parent_page_num, uint32_t child_page_num);

void init(char* node);

uint32_t* child(char* node, uint32_t child_num);

uint32_t find_child(char* node, uint32_t key);

void update_internal_node_key(char* node, uint32_t old_key, uint32_t new_key);

Cursor find(Table& table, uint32_t page_num, uint32_t key);

void internal_node_split_and_insert(Table& table, uint32_t parent_page_num,
                                    uint32_t child_page_num);

void insert(Table& table, uint32_t parent_page_num, uint32_t child_page_num);
}  // namespace InternalNode