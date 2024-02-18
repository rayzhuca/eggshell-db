#include "modeldb/compiler/statement.hpp"

#include <cstring>
#include <shared_mutex>
#include <sstream>

#include "modeldb/storage/bplus/leafnode.hpp"

CmdPrepareResult Statement::prepare(std::string input) {
    if (input.starts_with("insert")) {
        type = StatementType::insert;
        std::stringstream stream(input);
        int64_t id;
        std::string w, username, email;
        stream >> w >> id >> username >> email;
        if (username.size() > Row::COLUMN_USERNAME_SIZE ||
            email.size() > Row::COLUMN_EMAIL_SIZE) {
            return CmdPrepareResult::string_too_long;
        }
        if (id < 0 || id > std::numeric_limits<uint32_t>::max()) {
            return CmdPrepareResult::id_out_of_range;
        }
        if (std::cin.fail()) {
            return CmdPrepareResult::syntax_error;
        }
        row_to_insert.id = id;
        strncpy(row_to_insert.username, username.c_str(),
                sizeof(row_to_insert.username));
        strncpy(row_to_insert.email, email.c_str(),
                sizeof(row_to_insert.email));
        return CmdPrepareResult::success;
    }
    if (input.starts_with("select")) {
        type = StatementType::select;
        return CmdPrepareResult::success;
    }
    return CmdPrepareResult::unrecognized;
}

ExecuteResult Statement::execute_insert(Table& table) {
    std::unique_lock lock(table.mutex);
    char* node = table.pager.get(table.root_page_num);
    uint32_t num_cells = *LeafNode::num_cells(node);

    uint32_t key_to_insert = row_to_insert.id;
    Cursor cursor = table.find(key_to_insert);
    if (cursor.cell_num < num_cells) {
        uint32_t key_at_index = *LeafNode::key(node, cursor.cell_num);
        if (key_at_index == key_to_insert) {
            return ExecuteResult::duplicate_key;
        }
    }

    LeafNode::insert(cursor, row_to_insert.id, row_to_insert);

    return ExecuteResult::success;
}

ExecuteResult Statement::execute_select(Table& table) const {
    std::shared_lock lock(table.mutex);
    Row row;
    Cursor cursor = table.start();

    while (!cursor.end_of_table) {
        row.deserialize(cursor.value());
        std::cout << "(" << row.id << ", " << row.username << ", " << row.email
                  << ")\n";
        cursor.advance();
    }
    return ExecuteResult::success;
}

ExecuteResult Statement::execute(Table& table) {
    switch (type) {
        case (StatementType::insert):
            return execute_insert(table);
        case (StatementType::select):
            return execute_select(table);
    }
}