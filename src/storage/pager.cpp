#include "eggshell/storage/pager.hpp"

Pager::Pager(std::string filename)
    : file{filename, file.in | file.out | file.binary} {
    if (file.fail()) {
        printf("Unable to open file\n");
        std::exit(EXIT_FAILURE);
    }

    file.seekg(0, file.end);
    file_length = file.tellg();
    num_pages = file_length / PAGE_SIZE;

    if (file_length % PAGE_SIZE != 0) {
        std::cout << "Db file is not a whole number of pages. Corrupt file\n";
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < MAX_PAGES; i++) {
        pages[i] = nullptr;
    }
}

char* Pager::get(uint32_t page_num) {
    if (page_num > MAX_PAGES) {
        std::cout << "Tried to fetch page number out of bounds. " << page_num
                  << " >= " << MAX_PAGES << "\n";
        exit(EXIT_FAILURE);
    }

    if (pages[page_num] == nullptr) {
        // Cache miss. Allocate memory and load from file.
        char* page = new char[PAGE_SIZE];
        // uint32_t num_pages = file_length / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        if (page_num <= num_pages) {
            file.clear();
            file.seekg(page_num * PAGE_SIZE, file.beg);
            file.clear();
            file.read(page, PAGE_SIZE);
            file.clear();
            if (!file) {
                std::cout << "Error reading file: " << strerror(errno) << "\n";
                exit(EXIT_FAILURE);
            }
        }
        pages[page_num] = page;

        if (page_num >= num_pages) {
            num_pages = page_num + 1;
        }
    }

    previous_pages[page_num] = new char[PAGE_SIZE];
    memcpy(previous_pages[page_num], pages[page_num], PAGE_SIZE);

    return pages[page_num];
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
uint32_t Pager::get_unused_page_num() {
    return num_pages;
}

void Pager::flush(uint32_t page_num) {
    if (pages[page_num] == nullptr) {
        std::cout << "Tried to flush null page\n";
        exit(EXIT_FAILURE);
    }
    file.seekg(page_num * PAGE_SIZE, file.beg);
    file.clear();

    if (!file) {
        std::cout << "Error seeking: " << strerror(errno) << "\n";
        exit(EXIT_FAILURE);
    }

    file.write(pages[page_num], PAGE_SIZE);

    if (!file) {
        std::cout << "Error writing: " << errno << "\n";
        exit(EXIT_FAILURE);
    }
}

void Pager::log_transaction(uint32_t page_num, std::fstream& file) {
    if (pages[page_num] == nullptr) {
        std::cout << "Tried to flush null page\n";
        exit(EXIT_FAILURE);
    }
    file.write(pages[page_num], PAGE_SIZE);
    char success[] = {1};
    file.write(success, 1);

    if (!file) {
        std::cout << "Error writing: " << errno << "\n";
        exit(EXIT_FAILURE);
    }
}