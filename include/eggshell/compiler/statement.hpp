#pragma once

#include <string>

#include "eggshell/compiler/executeresult.hpp"
#include "eggshell/compiler/prepareresult.hpp"
#include "eggshell/storage/row.hpp"
#include "eggshell/storage/table.hpp"

enum class StatementType { insert, select };

struct Statement {
    StatementType type;
    Row row_to_insert;

    CmdPrepareResult prepare(std::string input);

    ExecuteResult execute_insert(Table& table);

    ExecuteResult execute_select(Table& table) const;

    ExecuteResult execute(Table& table);
};