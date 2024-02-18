#include "modeldb/storage/row.hpp"

#include <cstring>

const uint32_t Row::ID_SIZE = sizeof(uint32_t);
const uint32_t Row::USERNAME_SIZE = sizeof(char) * (COLUMN_USERNAME_SIZE + 1);
const uint32_t Row::EMAIL_SIZE = sizeof(char) * (COLUMN_EMAIL_SIZE + 1);
const uint32_t Row::ID_OFFSET = 0;
const uint32_t Row::USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t Row::EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t Row::SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void Row::serialize(char* destination) const {
    std::memcpy(destination + ID_OFFSET, &id, ID_SIZE);
    std::memcpy(destination + USERNAME_OFFSET, &username, USERNAME_SIZE);
    std::memcpy(destination + EMAIL_OFFSET, &email, EMAIL_SIZE);
}

void Row::deserialize(const char* source) {
    std::memcpy(&id, source + ID_OFFSET, ID_SIZE);
    std::memcpy(&username, source + USERNAME_OFFSET, USERNAME_SIZE);
    std::memcpy(&email, source + EMAIL_OFFSET, EMAIL_SIZE);
}