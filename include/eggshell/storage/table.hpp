#pragma once

#include <cstdint>
#include <fstream>
#include <shared_mutex>
#include <string>

#include "eggshell/storage/cursor.hpp"
#include "eggshell/storage/pager.hpp"

struct Cursor;

class Table {
   public:
    Pager pager;
    uint32_t root_page_num;
    std::shared_mutex mutex;

    Table(std::string filename);

    ~Table();

    bool flush();

    Cursor start();

    Cursor find(uint32_t key);
};