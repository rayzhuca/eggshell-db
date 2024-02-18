#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "modeldb/storage/table.hpp"

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