# ModelDB

Relational database model built in C++, written as a learning project.
We plan to add indexing, range queries, atomic transactions, concurrency, and custom parser, with everything built from scratch.

## Usage

Must have C++-17 installed along with CMake.

To build, run

```zsh
cmake --build build
```

## Tests

ModelDB uses [Google Test](https://github.com/google/googletest), which is simple to configure and run.
To test, run the following command after building:

```zsh
ctest --test-dir build
```

or alternatively,

```zsh
build && ctest
```

To run a specific test, do

```zsh
build/{TEST_FILE_NAME}
```

If intellisense is having trouble finding gtest, make sure to add the gtest onto the include path.
For VSCode, ``c_cpp_properties.json`` should have something like the following

```json
{
...
"configurations": [
        {
            ...
            "includePath": [
                ...
                "${workspaceFolder}/_deps/googletest-src/googletest/include"
            ],
        }
    ]
}
```
