#include "ibplustree.hpp"

#include "bplusnode.hpp"

// resources: https://shachaf.net/w/b-trees

constexpr int PAGE_SIZE = 1 << 12;
// constexpr int

class IBPlusTree {
    BPlusNode* root;
    IBPlusTree() {
    }
};