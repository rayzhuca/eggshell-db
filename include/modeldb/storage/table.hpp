#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "modeldb/storage/cursor.hpp"
#include "modeldb/storage/pager.hpp"

struct Cursor;

struct Table {
    Pager pager;
    uint32_t root_page_num;
    Table(std::string filename);
    ~Table();
    Cursor start();
    Cursor find(uint32_t key);
};