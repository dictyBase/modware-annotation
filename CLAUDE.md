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
      - Essential utility functions:
        ```go
        func Map[T, U any](ts []T, f func(T) U) []U {
            us := make([]U, len(ts))
            for i, t := range ts {
                us[i] = f(t)
            }
            return us
        }
        
        func Filter[T any](slice []T, predicate func(T) bool) []T {
            var result []T
            for _, item := range slice {
                if predicate(item) {
                    result = append(result, item)
                }
            }
            return result
        }
        
        func Find[T any](slice []T, predicate func(T) bool) (*T, bool) {
            for i := range slice {
                if predicate(slice[i]) {
                    return &slice[i], true
                }
            }
            return nil, false
        }
        
        func MapWithError[T, U any](ts []T, f func(T) (U, error)) ([]U, error) {
            us := make([]U, len(ts))
            for i, t := range ts {
                u, err := f(t)
                if err != nil {
                    return nil, err
                }
                us[i] = u
            }
            return us, nil
        }
        ```
      - Usage examples:
        ```go
        // Helper functions for transformations
        func stringToInt(s string) int {
            n, _ := strconv.Atoi(s)
            return n
        }
        
        func isEven(n int) bool {
            return n%2 == 0
        }
        
        func isBob(u string) bool {
            return u == "bob"
        }
        
        // Transform slice elements
        strings := []string{"1", "2", "3"}
        numbers := Map(strings, stringToInt)
        
        // Filter slice elements
        numbers := []int{1, 2, 3, 4, 5}
        evens := Filter(numbers, isEven)
        
        // Find first matching element
        users := []string{"alice", "bob", "charlie"}
        user, found := Find(users, isBob)
        
        // For complex operations with error handling
        func (c *Client) processArticleWithError(pmid string) (*Article, error) {
            article, err := c.GetArticle(pmid)
            if err != nil {
                return nil, fmt.Errorf("failed to process article %s: %w", pmid, err)
            }
            return article, nil
        }
        
        // Usage with MapWithError
        pmids := []string{"12345", "67890", "11111"}
        articles, err := MapWithError(pmids, c.processArticleWithError)
        ```
    - Use options pattern for configurable components
      ```go
      // Option type for functional options
      type Option func(*Config)
      
      // Config struct holds the configuration
      type Config struct {
          timeout     time.Duration
          retries     int
          debug       bool
          maxConnections int
      }
      
      // Option functions
      func WithTimeout(timeout time.Duration) Option {
          return func(c *Config) {
              c.timeout = timeout
          }
      }
      
      func WithRetries(retries int) Option {
          return func(c *Config) {
              c.retries = retries
          }
      }
      
      func WithDebug(debug bool) Option {
          return func(c *Config) {
              c.debug = debug
          }
      }
      
      func WithMaxConnections(max int) Option {
          return func(c *Config) {
              c.maxConnections = max
          }
      }
      
      // Constructor with default values and options
      func NewClient(opts ...Option) *Client {
          cfg := &Config{
              timeout:        30 * time.Second,
              retries:        3,
              debug:          false,
              maxConnections: 10,
          }
          
          for _, opt := range opts {
              opt(cfg)
          }
          
          return &Client{config: cfg}
      }
      
      // Usage examples
      client1 := NewClient() // Uses all defaults
      
      client2 := NewClient(
          WithTimeout(60*time.Second),
          WithRetries(5),
          WithDebug(true),
      )
      
      client3 := NewClient(WithMaxConnections(20))
      ```
    - Document all exported functions, types, and constants with proper Go doc comments
    - Test coverage should be comprehensive with both unit and integration tests
    - Use go-playground/validator for struct field and parameter validation
      ```go
      import (
          "github.com/go-playground/validator/v10"
      )
      
      // Struct with validation tags
      type CreateUserRequest struct {
          Name     string `validate:"required,min=2,max=50" json:"name"`
          Email    string `validate:"required,email" json:"email"`
          Age      int    `validate:"gte=18,lte=120" json:"age"`
          Password string `validate:"required,min=8" json:"password"`
          Role     string `validate:"required,oneof=admin user guest" json:"role"`
          Website  string `validate:"omitempty,url" json:"website"`
      }
      
      // Global validator instance (thread-safe singleton)
      var validate = validator.New()
      
      // Validate struct fields
      func CreateUser(req CreateUserRequest) error {
          if err := validate.Struct(req); err != nil {
              return fmt.Errorf("validation failed: %w", err)
          }
          
          // Process valid request
          return nil
      }
      
      // Validate individual parameters
      func UpdateUserEmail(userID string, newEmail string) error {
          if err := validate.Var(newEmail, "required,email"); err != nil {
              return fmt.Errorf("invalid email: %w", err)
          }
          
          if err := validate.Var(userID, "required,uuid"); err != nil {
              return fmt.Errorf("invalid user ID: %w", err)
          }
          
          // Process update
          return nil
      }
      
      // Complex validation with nested structs
      type Address struct {
          Street  string `validate:"required,min=5"`
          City    string `validate:"required"`
          Country string `validate:"required,iso3166_1_alpha2"`
          ZipCode string `validate:"required,postcode_iso3166_alpha2=US"`
      }
      
      type UserProfile struct {
          User    CreateUserRequest `validate:"required"`
          Address Address          `validate:"required"`
          Tags    []string         `validate:"dive,required,min=2"`
      }
      
      // Custom validation function
      func validateBusinessEmail(fl validator.FieldLevel) bool {
          email := fl.Field().String()
          // Business emails should not be from common free providers
          blockedDomains := []string{"gmail.com", "yahoo.com", "hotmail.com"}
          
          for _, domain := range blockedDomains {
              if strings.HasSuffix(email, "@"+domain) {
                  return false
              }
          }
          return true
      }
      
      // Register custom validator
      func init() {
          validate.RegisterValidation("business_email", validateBusinessEmail)
      }
      
      // Usage with custom validator
      type BusinessUser struct {
          Email string `validate:"required,email,business_email"`
      }
      
      // Helper function to format validation errors
      func FormatValidationError(err error) string {
          if validationErrors, ok := err.(validator.ValidationErrors); ok {
              var messages []string
              for _, e := range validationErrors {
                  switch e.Tag() {
                  case "required":
                      messages = append(messages, fmt.Sprintf("%s is required", e.Field()))
                  case "email":
                      messages = append(messages, fmt.Sprintf("%s must be a valid email", e.Field()))
                  case "min":
                      messages = append(messages, fmt.Sprintf("%s must be at least %s characters", e.Field(), e.Param()))
                  case "max":
                      messages = append(messages, fmt.Sprintf("%s must be at most %s characters", e.Field(), e.Param()))
                  default:
                      messages = append(messages, fmt.Sprintf("%s failed validation: %s", e.Field(), e.Tag()))
                  }
              }
              return strings.Join(messages, "; ")
          }
          return err.Error()
      }
      ```
    - Any function or method receiving more than three parameters should use a type struct
      ```go
      // Avoid: Too many parameters
      func CreateUser(name, email, phone, address string, age int, active bool) error {
          // implementation
      }
      
      // Preferred: Use a struct for parameters
      type CreateUserParams struct {
          Name    string
          Email   string
          Phone   string
          Address string
          Age     int
          Active  bool
      }
      
      func CreateUser(params CreateUserParams) error {
          // implementation
      }
      
      // Usage
      err := CreateUser(CreateUserParams{
          Name:    "John Doe",
          Email:   "john@example.com",
          Phone:   "555-0123",
          Address: "123 Main St",
          Age:     30,
          Active:  true,
      })
      ```

- **Project Structure**
    - Primary interface definitions in package root
    - Implementations in subdirectories by backing technology

- **Variable Name Length:**
    -  Favor variable names that are at least three characters long, except for
    loop indices (e.g., `i`, `j`), method receivers (e.g., `r` for `receiver`),
    and extremely common types (e.g., `r` for `io.Reader`, `w` for `io.Writer`).
    -  Prioritize clarity and readability.  Use the shortest name that
    effectively conveys the variable's purpose within its context.
    - Variable naming: camelCase, descriptive names, no abbreviations except for
    common ones

- **Naming Style:**
    - Use `camelCase` for variable and function names (e.g., `myVariableName`, `calculateTotal`).
    - Use `PascalCase` for exported (public) types, functions, and constants (e.g., `MyType`, `CalculateTotal`).
    - Avoid `snake_case` (e.g., `my_variable_name`) in most cases.

- **Clarity and Context:**
    - The further a variable is used from its declaration, the more descriptive
    its name should be
      ```go
      func processData() {
          // Short scope: single letter acceptable
          for i := 0; i < 10; i++ {
              fmt.Println(i)
          }
          
          // Medium scope: short but descriptive
          users := fetchUsers()
          for _, user := range users {
              processUser(user)
          }
      }
      
      func longRunningFunction() {
          // Long scope: highly descriptive names
          authenticatedUserRepository := NewUserRepository()
          configurationManager := NewConfigManager()
          emailNotificationService := NewEmailService()
          
          // These variables are used throughout the function
          for page := 1; page <= totalPages; page++ {
              paginatedUserResults := authenticatedUserRepository.GetUsersByPage(page)
              
              for _, individualUser := range paginatedUserResults {
                  userEmailAddress := individualUser.Email
                  notificationPreferences := configurationManager.GetPreferences(individualUser.ID)
                  
                  if notificationPreferences.EmailEnabled {
                      emailNotificationService.Send(userEmailAddress, "Welcome!")
                  }
              }
          }
      }
      
      // Function parameters and package-level variables: descriptive
      func CalculateMonthlySubscriptionRevenue(subscriptionDetails []Subscription, 
                                               discountCalculator DiscountService) decimal.Decimal {
          totalMonthlyRevenue := decimal.Zero
          
          for _, subscription := range subscriptionDetails {
              monthlyAmount := subscription.MonthlyPrice
              applicableDiscount := discountCalculator.Calculate(subscription)
              finalAmount := monthlyAmount.Sub(applicableDiscount)
              totalMonthlyRevenue = totalMonthlyRevenue.Add(finalAmount)
          }
          
          return totalMonthlyRevenue
      }
      ```
    - Choose names that clearly indicate the variable's purpose and the type of
    data it holds.

- **Avoidance:**
    - Do not use spaces in variable names.
    - Variable names should start with a letter or underscore.
    - Do not use Go keywords as variable names.

- **Constants:**
    - Use `PascalCase` for constants. If a constant is unscoped, all letters in
    the constant should be capitalized. `const MAX_SIZE = 100`

- **Error Handling:**
    - When naming error variables, use `err` as the prefix:  `errMyCustomError`.
    - Always check errors and return meaningful wrapped errors
      ```go
      import (
          "fmt"
          "io"
          "os"
      )
      
      // Avoid: Ignoring errors
      func badExample() {
          file, _ := os.Open("config.txt")
          data, _ := io.ReadAll(file)
          fmt.Println(string(data))
      }
      
      // Preferred: Always check and wrap errors with context
      func goodExample() error {
          file, err := os.Open("config.txt")
          if err != nil {
              return fmt.Errorf("failed to open config file: %w", err)
          }
          defer file.Close()
          
          data, err := io.ReadAll(file)
          if err != nil {
              return fmt.Errorf("failed to read config file: %w", err)
          }
          
          fmt.Println(string(data))
          return nil
      }
      
      // Multiple operations: preserve error chain
      func processUserData(userID string) error {
          user, err := fetchUser(userID)
          if err != nil {
              return fmt.Errorf("failed to fetch user %s: %w", userID, err)
          }
          
          profile, err := loadProfile(user.ProfileID)
          if err != nil {
              return fmt.Errorf("failed to load profile for user %s: %w", userID, err)
          }
          
          err = validateProfile(profile)
          if err != nil {
              return fmt.Errorf("invalid profile for user %s: %w", userID, err)
          }
          
          err = saveProcessedData(user, profile)
          if err != nil {
              return fmt.Errorf("failed to save processed data for user %s: %w", userID, err)
          }
          
          return nil
      }
      
      // Custom error types for better error handling
      type ValidationError struct {
          Field   string
          Value   interface{}
          Message string
      }
      
      func (e ValidationError) Error() string {
          return fmt.Sprintf("validation failed for field '%s' with value '%v': %s", 
                           e.Field, e.Value, e.Message)
      }
      
      func validateEmail(email string) error {
          if email == "" {
              return ValidationError{
                  Field:   "email",
                  Value:   email,
                  Message: "email cannot be empty",
              }
          }
          
          if !strings.Contains(email, "@") {
              return fmt.Errorf("invalid email format: %w", ValidationError{
                  Field:   "email",
                  Value:   email,
                  Message: "must contain @ symbol",
              })
          }
          
          return nil
      }
      
      // Validation errors using go-playground/validator
      func CreateUserWithValidation(req CreateUserRequest) error {
          // First validate the struct
          if err := validate.Struct(req); err != nil {
              if validationErrors, ok := err.(validator.ValidationErrors); ok {
                  return fmt.Errorf("validation failed: %w", &UserValidationError{
                      Errors: validationErrors,
                  })
              }
              return fmt.Errorf("validation failed: %w", err)
          }
          
          // Additional business logic validation
          if strings.Contains(req.Email, "test") {
              return fmt.Errorf("test emails not allowed: %w", &BusinessLogicError{
                  Field:   "email",
                  Value:   req.Email,
                  Message: "production environment does not accept test emails",
              })
          }
          
          // Process valid request
          return nil
      }
      
      // Custom error types for validation
      type UserValidationError struct {
          Errors validator.ValidationErrors
      }
      
      func (e *UserValidationError) Error() string {
          var messages []string
          for _, err := range e.Errors {
              switch err.Tag() {
              case "required":
                  messages = append(messages, fmt.Sprintf("%s is required", err.Field()))
              case "email":
                  messages = append(messages, fmt.Sprintf("%s must be a valid email address", err.Field()))
              case "min":
                  messages = append(messages, fmt.Sprintf("%s must be at least %s characters long", err.Field(), err.Param()))
              case "max":
                  messages = append(messages, fmt.Sprintf("%s must be at most %s characters long", err.Field(), err.Param()))
              case "gte":
                  messages = append(messages, fmt.Sprintf("%s must be greater than or equal to %s", err.Field(), err.Param()))
              case "oneof":
                  messages = append(messages, fmt.Sprintf("%s must be one of: %s", err.Field(), err.Param()))
              default:
                  messages = append(messages, fmt.Sprintf("%s failed validation rule: %s", err.Field(), err.Tag()))
              }
          }
          return strings.Join(messages, "; ")
      }
      
      type BusinessLogicError struct {
          Field   string
          Value   interface{}
          Message string
      }
      
      func (e *BusinessLogicError) Error() string {
          return fmt.Sprintf("business logic error for field '%s' with value '%v': %s", 
                           e.Field, e.Value, e.Message)
      }
      
      // Error checking with type assertions
      func HandleUserCreation(req CreateUserRequest) {
          err := CreateUserWithValidation(req)
          if err != nil {
              var validationErr *UserValidationError
              var businessErr *BusinessLogicError
              
              switch {
              case errors.As(err, &validationErr):
                  log.Printf("Validation errors: %s", validationErr.Error())
                  // Handle validation errors - return 400 Bad Request
              case errors.As(err, &businessErr):
                  log.Printf("Business logic error: %s", businessErr.Error())
                  // Handle business logic errors - return 422 Unprocessable Entity
              default:
                  log.Printf("Unknown error: %s", err.Error())
                  // Handle unknown errors - return 500 Internal Server Error
              }
          }
      }
      ```

- **Receivers:**
    - Use short, one or two-letter receiver names that reflect the type (e.g.,
    `r` for `io.Reader`, `f` for `*File`).

