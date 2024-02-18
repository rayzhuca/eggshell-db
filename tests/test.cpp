#include <fstream>
#include <iostream>
using namespace std;

char a[] = {'a', 'b', 'c'};
int main() {
    fstream file;
    file.open("test.bin", file.in | file.out | file.binary);
    file.seekg(1231, file.beg);
    if (!file) {
        std::cout << "?\n";
    }
    std::cout << file.eof() << "\n";
    file.write(a, sizeof(a));
}