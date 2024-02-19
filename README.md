# <img width="40" valign="top" alt="Screenshot 2024-02-18 at 4 57 57 PM" src="https://github.com/RayBipse/eggshell-db/assets/46636772/d95263e6-8ef0-439d-8fd8-2028306f2421"> eggshell db

![workflow badge](https://github.com/RayBipse/eggshell-db/actions/workflows/build.yml/badge.svg)

Relational database model built in C++, written as a learning project.

Eggshell could support concurrent read and write operations. It utilizes a readers-writer lock.

It could also support atomicity, as previous pages are logged before they are modified.

The project includes two basic SQL commands,

```SQL
INSERT INTO table_name VALUES (value1, value2, ...);
```

```SQL
SELECT column1, column2 FROM table_name;
```


## Architecture

The library is broken down into two sections, ``compiler`` and ``storage``, where the ``compiler`` composes inputs into
commands and ``storage`` is responsible for storing data.

![toydb architecture (1)](https://github.com/RayBipse/eggshell-db/assets/46636772/1d4dd79e-6d65-4afa-a110-ab685f92ed6a)


## Usage

Must have C++-17 installed along with CMake.

Before the first build, run

```zsh
cmake -S . -B ./build 
```

To build, run

```zsh
cmake --build build
```

To run the REPL, first create an empty file, e.g., ``touch example.db``, then run

```zsh
build/repl example.db
```


## Future features

Some features to be implemented in the future are
- ``JOIN`` clauses
- Immutable B+ tree for complete atomic transactions
- Distributed database
- Error system (for now, we are just using ``exit`` on failure)


## Tests

eggshell uses [Google Test](https://github.com/google/googletest), which is simple to configure and run.
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
"configurations": [
        {
            "includePath": [
                "${workspaceFolder}/_deps/googletest-src/googletest/include"
            ],
        }
    ]
}
```


## Resources and credits

As someone who had no previous knowledge of databases, I have used numerous resources for this project that I would love to share.

##### Good project examples

[Let's Build a Simple Database (C)](https://cstack.github.io/db_tutorial/)

[Build Your Own Database From Scratch (Go)](https://cstack.github.io/db_tutorial/)

[toyDB (Rust)](https://github.com/erikgrinaker/toydb/blob/master/README.md)

##### General database theory

[CMU Database Group](https://youtu.be/DJ5u5HrbcMk?si=tMYnMY950OVwwrhH)

##### Parser 

[Cornell Introduction to Compiler](https://www.cs.cornell.edu/courses/cs4120/2023sp/)
