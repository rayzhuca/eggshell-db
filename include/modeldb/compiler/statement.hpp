#pragma once

#include <string>

#include "modeldb/compiler/executeresult.hpp"
#include "modeldb/compiler/prepareresult.hpp"
#include "modeldb/storage/row.hpp"
#include "modeldb/storage/table.hpp"

enum class StatementType { insert, select };

struct Statement {
    StatementType type;
    Row row_to_insert;

    CmdPrepareResult prepare(std::string input);

    ExecuteResult execute_insert(Table& table);

    ExecuteResult execute_select(Table& table) const;

    ExecuteResult execute(Table& table);
};