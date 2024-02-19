#include "eggshell/storage/table.hpp"

#include <fstream>

#include "eggshell/storage/bplus/internalnode.hpp"
#include "eggshell/storage/bplus/leafnode.hpp"
#include "eggshell/storage/bplus/node.hpp"

Table::Table(std::string filename) : pager{filename}, root_page_num{0} {
    if (pager.num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        char* root_node = pager.get(0);
        LeafNode::init(root_node);
        Node::set_node_root(root_node, true);
    }
}

bool Table::flush() {
    std::fstream logfile{"temp.log",
                         logfile.binary | logfile.trunc | logfile.out};
    for (const auto& [key, value] : pager.previous_pages) {
        pager.log_transaction(key, logfile);
        pager.flush(key);
    }
    // if transaction finished, then we don't need log
    // TODO: finish
    pager.previous_pages.clear();
    return false;
}

Table::~Table() {
    for (uint32_t i = 0; i < pager.num_pages; i++) {
        if (pager.pages[i] == nullptr) continue;
        pager.flush(i);
        delete[] pager.pages[i];
        pager.pages[i] = nullptr;
    }

    pager.file.close();
    if (!pager.file) {
        std::cout << "Error closing db file.\n";
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < Pager::MAX_PAGES; i++) {
        char* page = pager.pages[i];
        if (page) {
            delete[] page;
            pager.pages[i] = nullptr;
        }
    }
}

Cursor Table::start() {
    Cursor cursor = find(0);

    char* node = pager.get(cursor.page_num);
    uint32_t num_cells = *LeafNode::num_cells(node);
    cursor.end_of_table = num_cells == 0;

    return cursor;
}

Cursor Table::find(uint32_t key) {
    char* root_node = pager.get(root_page_num);

    if (Node::get_node_type(root_node) == NodeType::leaf) {
        return LeafNode::find(*this, root_page_num, key);
    } else {
        return InternalNode::find(*this, root_page_num, key);
    }
}