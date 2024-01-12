#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <utility>

template <class K, class V>
class BPlusNode {
   public:
    uint16_t key_size;
    std::unique_ptr<K*> keys;
    bool is_leaf;
};

template <class T>
class BPlusInternalNode : BPlusNode<T> {
   public:
    uint16_t children_size;
    std::unique_ptr<BPlusNode*> children;
};

template <class T>
class BPlusLeafNode : public BPlusNode {
   public:
    std::array<T> values;
};