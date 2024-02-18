#include "modeldb/storage/cursor.hpp"

#include "modeldb/storage/bplus/leafnode.hpp"

Cursor::Cursor(Table& table, uint32_t page_num, uint32_t cell_num,
               bool end_of_table)
    : table{table},
      page_num{page_num},
      cell_num{cell_num},
      end_of_table{end_of_table} {
}

char* Cursor::value() {
    char* page = table.pager.get(page_num);
    return LeafNode::value(page, cell_num);
}

void Cursor::advance() {
    char* node = table.pager.get(page_num);
    cell_num += 1;
    if (cell_num >= *LeafNode::num_cells(node)) {
        /* Advance to next leaf node */
        uint32_t next_page_num = *LeafNode::next_leaf(node);
        if (next_page_num == 0) {
            /* This was rightmost leaf */
            end_of_table = true;
        } else {
            page_num = next_page_num;
            cell_num = 0;
        }
    }
}