#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <utility>

template <class K, class V>
class BPlusNode {
   public:
    uint16_t key_size;
    K** keys;
    bool is_leaf;
};

template <class K, class V>
class BPlusInternalNode : BPlusNode<K, V> {
   public:
    uint16_t children_size;
    BPlusNode** children;
};

template <class K, class V>
class BPlusLeafNode : public BPlusNode<K, V> {
   public:
    std::array<T> values;
};