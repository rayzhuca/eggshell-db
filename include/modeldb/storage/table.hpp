#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "modeldb/storage/cursor.hpp"
#include "modeldb/storage/pager.hpp"

struct Cursor;

class Table {
   public:
    Pager pager;
    uint32_t root_page_num;
    std::mutex mutex;

    Table(std::string filename);

    ~Table();

    bool flush();

    Cursor start();

    Cursor find(uint32_t key);
};