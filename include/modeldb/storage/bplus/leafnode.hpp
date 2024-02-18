#pragma once

#include <cstdint>

#include "modeldb/storage/cursor.hpp"
#include "modeldb/storage/row.hpp"

namespace LeafNode {

extern const uint32_t LEAF_NODE_NUM_CELLS_SIZE;
extern const uint32_t LEAF_NODE_NUM_CELLS_OFFSET;
extern const uint32_t LEAF_NODE_NEXT_LEAF_SIZE;
extern const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET;
extern const uint32_t LEAF_NODE_HEADER_SIZE;

extern const uint32_t LEAF_NODE_KEY_SIZE;
extern const uint32_t LEAF_NODE_KEY_OFFSET;
extern const uint32_t LEAF_NODE_VALUE_SIZE;
extern const uint32_t LEAF_NODE_VALUE_OFFSET;
extern const uint32_t LEAF_NODE_CELL_SIZE;
extern const uint32_t LEAF_NODE_SPACE_FOR_CELLS;
extern const uint32_t LEAF_NODE_MAX_CELLS;
extern const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT;
extern const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT;

uint32_t* num_cells(char* node);

char* cell(char* node, uint32_t cell_num);

uint32_t* key(char* node, uint32_t cell_num);

char* value(char* node, uint32_t cell_num);

uint32_t* next_leaf(char* node);

void init(char* node);
void insert(const Cursor& cursor, uint32_t key, Row& value);

void split_and_insert(const Cursor& cursor, uint32_t key, Row& value);

Cursor find(Table& table, uint32_t page_num, uint32_t key);

};  // namespace LeafNode