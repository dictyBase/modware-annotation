# Go Coding Conventions

-- **Build, Test, and Lint Commands**
    - Run all tests: `gotestsum --format-hide-empty-pkg --format testdox --format-icons hivis`
    - Run specific test: `gotestsum --format-hide-empty-pkg --format testdox --format-icons hivis -- -run TestFindSimilar ./...`
    - Run tests with verbose output: `gotestum --format-hide-empty-pkg --format standard-verbose --format-icons hivis`
    - Format code: `gofumpt -w .`
    - Lint codebase: `golangcli-lint run`

- **Code Style Guidelines**
    - Imports: Standard library first, then external packages, then internal packages
    - Prefer functional programming utilities from collection package where appropriate
    - Use options pattern for configurable components (see SQLiteVectorStore)
    - Document all exported functions, types, and constants with proper Go doc comments
    - Test coverage should be comprehensive with both unit and integration tests

- **Project Structure**
    - Primary interface definitions in package root
    - Implementations in subdirectories by backing technology

- **Variable Name Length:**
    -  Favor variable names that are at least three characters long, except for loop indices (e.g., `i`, `j`), method receivers (e.g., `r` for `receiver`), and extremely common types (e.g., `r` for `io.Reader`, `w` for `io.Writer`).
    -  Prioritize clarity and readability.  Use the shortest name that effectively conveys the variable's purpose within its context.
    - Variable naming: camelCase, descriptive names, no abbreviations except for common ones

- **Naming Style:**
    - Use `camelCase` for variable and function names (e.g., `myVariableName`, `calculateTotal`).
    - Use `PascalCase` for exported (public) types, functions, and constants (e.g., `MyType`, `CalculateTotal`).
    - Avoid `snake_case` (e.g., `my_variable_name`) in most cases.

- **Clarity and Context:**
    - The further a variable is used from its declaration, the more descriptive its name should be.
    - Choose names that clearly indicate the variable's purpose and the type of data it holds.

- **Avoidance:**
    - Do not use spaces in variable names.
    - Variable names should start with a letter or underscore.
    - Do not use Go keywords as variable names.

- **Constants:**
    - Use `PascalCase` for constants. If a constant is unscoped, all letters in the constant should be capitalized. `const MAX_SIZE = 100`

- **Error Handling:**
    - When naming error variables, use `err` as the prefix:  `errMyCustomError`.
    - Always check errors and return meaningful wrapped errors

- **Receivers:**
    - Use short, one or two-letter receiver names that reflect the type (e.g., `r` for `io.Reader`, `f` for `*File`).

