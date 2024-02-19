#pragma once

#include <cstddef>
#include <cstdint>

struct Row {
    static const size_t COLUMN_USERNAME_SIZE = 32;
    static const size_t COLUMN_EMAIL_SIZE = 255;
    static const uint32_t ID_SIZE;
    static const uint32_t USERNAME_SIZE;
    static const uint32_t EMAIL_SIZE;

    static const uint32_t ID_OFFSET;
    static const uint32_t USERNAME_OFFSET;
    static const uint32_t EMAIL_OFFSET;
    static const uint32_t SIZE;

    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];

    void serialize(char* destination) const;

    void deserialize(const char* source);
};