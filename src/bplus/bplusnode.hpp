#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <utility>

static uint32_t PAGE_SIZE = 4096;

template <class K, class V>
class BPlusNode {
   public:
    const uint16_t HEADER_SIZE =
        sizeof(key_size) + sizeof(keys) + sizeof(is_leaf);
    uint16_t key_size;
    K** keys;
    bool is_leaf;
};

template <class K, class V>
class BPlusInternalNode : BPlusNode<K, V> {
   public:
    uint16_t children_size;
    BPlusNode<K, V>** children;
};

template <class K, class V>
class BPlusLeafNode : public BPlusNode<K, V> {
   public:
    const uint16_t HEADER_SIZE = BPlusNode<K, V>::HEADER_SIZE;
    const uint16_t NODE_CELL_SIZE = sizeof(K) + sizeof(V);
    const uint16_t MAX_CELLS = (PAGE_SIZE - HEADER_SIZE) / NODE_CELL_SIZE;
    V* values;
};