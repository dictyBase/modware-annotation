# Go Coding Conventions for Aider

These conventions should guide the code generated and modified by aider.

- **Variable Name Length:**
    -  Favor variable names that are at least three characters long, except for loop indices (e.g., `i`, `j`), method receivers (e.g., `r` for `receiver`), and extremely common types (e.g., `r` for `io.Reader`, `w` for `io.Writer`).
    -  Prioritize clarity and readability.  Use the shortest name that effectively conveys the variable's purpose within its context.

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

- **Receivers:**
    - Use short, one or two-letter receiver names that reflect the type (e.g., `r` for `io.Reader`, `f` for `*File`).

