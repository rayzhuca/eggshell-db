#include <fstream>
#include <iostream>
#include <string>

void print_prompt() {
    std::cout << "db > ";
}

void read_input(std::string& str) {
    try {
        std::getline(std::cin, str);
    } catch (std::ifstream::failure e) {
        std::cout << "error reading input\n";
        std::cout << e.what() << '\n';
    }
}

int main() {
    std::string input;
    while (true) {
        print_prompt();
        read_input(input);
        if (input == ".exit") {
            exit(EXIT_SUCCESS);
        } else {
            std::cout << "Unrecognized command \'" << input << "\'.\n";
        }
    }
}