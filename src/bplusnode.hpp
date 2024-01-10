#pragma once

#include <memory>
#include <utility>

class BPlusNode {
    bool isLeaf;
    int key_count, offsets;
    BPlusNode* pointers;
    std::unique_ptr<int*> kv_pairs;
    BPlusNode(bool isLeaf, int key_count, BPlusNode* pointers, int offsets,
              const std::unique_ptr<int*>& kv_pairs)
        : isLeaf{isLeaf},
          key_count{key_count},
          pointers{pointers},
          offsets{offsets},
          kv_pairs{std::move(kv_pairs)} {
    }
};