#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>

struct Pager {
    const static size_t PAGE_SIZE = 4096;
    const static size_t MAX_PAGES = 100;

    std::fstream file;
    uint32_t file_length;
    uint32_t num_pages;
    char* pages[MAX_PAGES];
    std::map<size_t, char*> previous_pages;

    Pager(std::string filename);

    char* get(uint32_t page_num);

    uint32_t get_unused_page_num();

    void flush(uint32_t page_num);

    void log_transaction(uint32_t page_num, std::fstream& file);
};