# Stasis

> **Note:** This project is in an early stage of development. The API is subject to change.

A C++23 transactional in-memory state manager.

## Core Concept

Stasis provides a simple, robust way to manage temporary, mutable sets of key-value data. It allows complex changes to be grouped together, tried out, and then either confirmed (`COMMIT`) or completely undone (`ROLLBACK`) as a single atomic unit.

It's can be used as an engine for building "Apply/Cancel" logic, such as in application configuration dialogs, game level editors, or any scenario involving user-driven state changes.

## Features

- **Header-Only:** Just `#include <stasis/stasis.hpp>` to get started.
- **Modern C++23:** Built from the ground up with C++23 features.
- **Transactional:** Group operations with `BEGIN`, `COMMIT`, and `ROLLBACK`. Nested transactions are fully supported.
- **Safe & Exception-Free:** Uses `std::expected` for robust error handling.
- **Performant:** Leverages C++20 heterogeneous lookups to avoid unnecessary string allocations on key lookups.
- **Secure:** No filesystem, networking, or other system I/O. All data lives and dies with the process.

## Building the Project

Stasis uses a standard CMake build process.

```bash
# 1. Configure the project
cmake -S . -B build

# 2. Build all targets (library, examples, tests)
cmake --build build

# 3. Run the unit tests
ctest --test-dir build --verbose
````

## Usage

> **Note:** A practical usage example will be added here once the API stabilizes.

The core of the library is the `stasis::KeyValueStore` class found in the `<stasis/stasis.hpp>` header.

## License

Stasis is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
