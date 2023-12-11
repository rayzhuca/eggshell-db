cmake_minimum_required(VERSION 3.9)
project("modeldb")

# setting version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# add library
file(GLOB MODELDB_SRC "src/*.cpp" "src/*.hpp")
file(GLOB MODELDB_INCLUDE "include/*.hpp")
add_library(modeldb SHARED ${MODELDB_SRC} ${MODELDB_INCLUDE}) 

# set include directories
target_include_directories(modeldb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/modeldb)
target_include_directories(modeldb PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# testing
enable_testing()
include(FetchContent) # for gtest
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(btree_test tests/btree_test.cpp)
target_link_libraries(btree_test GTest::gtest_main modeldb)

include(GoogleTest)
gtest_discover_tests(btree_test)