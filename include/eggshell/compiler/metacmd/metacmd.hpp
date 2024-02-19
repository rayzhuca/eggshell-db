#pragma once

#include <cstdint>
#include <string>

#include "eggshell/compiler/metacmd/metacmdresult.hpp"
#include "eggshell/storage/pager.hpp"
#include "eggshell/storage/table.hpp"

void print_constants();

void indent(uint32_t level);

void print_tree(Pager& pager, uint32_t page_num, uint32_t indentation_level);

MetaCmdResult do_meta_cmd(std::string input, Table& table);