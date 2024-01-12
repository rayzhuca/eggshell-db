#pragma once

#include <utility>

#include "bplus/bplusnode.hpp"

template <class K, class V>
class IBPlusTree {
    BPlusNode<K, V>* root;

   public:
    IBPlusTree() {
        root = new BPlusNode<K, V>();
    }

    ~IBPlusTree() {
        delete root;
    }

    void insert(K key, V value);
    void remove(K key);
};