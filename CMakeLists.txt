cmake_minimum_required(VERSION 3.22)
project("eggshell")

# setting version
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# add library
file(GLOB_RECURSE EGGSHELL_SRC "src/*.cpp" "src/*.hpp")
file(GLOB_RECURSE EGGSHELL_INCLUDE "include/eggshell/*.hpp")
add_library(eggshell SHARED ${EGGSHELL_SRC} ${EGGSHELL_INCLUDE}) 

# set include directories
# target_include_directories(eggshell PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/eggshell)
# target_include_directories(eggshell PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(eggshell PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

add_executable(repl "src/repl.cpp")
target_link_libraries(repl PUBLIC eggshell)

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
target_link_libraries(btree_test GTest::gtest_main eggshell)

include(GoogleTest)
gtest_discover_tests(btree_test)