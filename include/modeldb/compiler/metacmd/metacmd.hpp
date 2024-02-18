#pragma once

#include <cstdint>
#include <string>

#include "modeldb/compiler/metacmd/metacmdresult.hpp"
#include "modeldb/storage/pager.hpp"
#include "modeldb/storage/table.hpp"

void print_constants();

void indent(uint32_t level);

void print_tree(Pager& pager, uint32_t page_num, uint32_t indentation_level);

MetaCmdResult do_meta_cmd(std::string input, Table& table);