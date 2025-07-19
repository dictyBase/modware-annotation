# Table of Contents

- [Go Coding Conventions](#go-coding-conventions)
  - [Build, Test, and Lint Commands](#build-test-and-lint-commands)
  - [Code Style Guidelines](#code-style-guidelines)
  - [Project Structure](#project-structure)
  - [Variable Name Length](#variable-name-length)
  - [Naming Style](#naming-style)
  - [Clarity and Context](#clarity-and-context)
  - [Avoidance](#avoidance)
  - [Constants](#constants)
  - [Error Handling](#error-handling)
  - [Receivers](#receivers)
- [Testing Best Practices](#testing-best-practices)
  - [Test Organization and Structure](#test-organization-and-structure)
  - [Parameter Structs for Test Functions](#parameter-structs-for-test-functions)
  - [GRPC Service Testing with bufconn](#grpc-service-testing-with-bufconn)
  - [Functional Programming in Tests](#functional-programming-in-tests)
  - [Error Handling and Assertions](#error-handling-and-assertions)
  - [Timestamp and Time Handling](#timestamp-and-time-handling)
  - [Test Data Management](#test-data-management)
  - [Comprehensive Test Coverage](#comprehensive-test-coverage)
  - [Test Helper Organization](#test-helper-organization)

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

# Testing Best Practices

- **Test Organization and Structure**
    - Use descriptive test function names that clearly indicate what is being tested
    - Organize tests into separate files by functionality: `*_test.go` for main tests, `*_test_helpers.go` for reusable helpers
    - Group related test helper functions by domain (CRUD operations, tag management, list operations)
    - Use parallel execution with `t.Parallel()` for independent tests to improve performance
      ```go
      func TestCreateFeatureAnnotation(t *testing.T) {
          t.Parallel()
          client, assert := setup(t)
          ctx := context.Background()
          params := &testParams{
              t:      t,
              ctx:    ctx,
              client: client,
              assert: assert,
          }
          testCreateValidFeature(params)
          testCreateMissingFields(params)
          testCreateDuplicateFeature(params)
      }
      ```

- **Parameter Structs for Test Functions**
    - Use structs to pass multiple parameters to test helper functions for better maintainability
    - Define parameter structs with clear, descriptive names
      ```go
      // tagPropertyCreateParams holds the parameters for the
      // createServiceTagPropertyCreate function.
      type tagPropertyCreateParams struct {
          tag       string
          value     string
          createdBy string
          timestamp *time.Time
      }
      
      // assertGrpcErrorParams holds the parameters for the assertGrpcError function.
      type assertGrpcErrorParams struct {
          assert               *require.Assertions
          err                  error
          expectedCode         codes.Code
          expectedMsgSubstring string
      }
      
      // Usage example
      func createServiceTagPropertyCreate(
          params *tagPropertyCreateParams,
      ) *feature.TagPropertyCreate {
          tagCreate := &feature.TagPropertyCreate{
              Tag:       params.tag,
              Value:     params.value,
              CreatedBy: params.createdBy,
          }
          
          if params.timestamp != nil {
              tagCreate.CreatedAt = timestamppb.New(*params.timestamp)
          }
          
          return tagCreate
      }
      ```

- **GRPC Service Testing with bufconn**
    - Use in-memory GRPC testing with `bufconn.Listen()` for fast, isolated tests
    - Create mock implementations for external dependencies
    - Ensure proper cleanup with `t.Cleanup()` to prevent resource leaks
      ```go
      func setup(
          t *testing.T,
      ) (feature.FeatureAnnotationServiceClient, *require.Assertions) {
          t.Helper()
          assert := require.New(t)
          tra, err := testarango.NewTestArangoFromEnv(true)
          assert.NoError(err, "expect no error from creating an arangodb instance")
          
          // Create repository with isolated test collections
          repo, err := arangodb.NewFeatureAnnoRepo(
              arangodb.GetConnectParamsFromDB(tra),
              &arangodb.FeatureCollectionParams{
                  Feature: "feature_test",
                  Pub:     "pub_test",
                  Edge:    "feature_pub_test",
                  Graph:   "feature_pub_graph_test",
              },
          )
          assert.NoError(err)
          
          // Create service with mock dependencies
          svc, err := NewFeatureAnnotationService(&FeatureParams{
              Repository: repo,
              Publisher:  &MockMessage{}, // Mock message publisher
          })
          assert.NoError(err)
          
          // GRPC server setup with bufconn
          server := grpc.NewServer()
          feature.RegisterFeatureAnnotationServiceServer(server, svc)
          lis := bufconn.Listen(1024 * 1024)
          go func() {
              if err = server.Serve(lis); err != nil {
                  t.Logf("Server exited with error: %v", err)
                  os.Exit(1)
              }
          }()
          
          // GRPC client setup
          dialer := func(context.Context, string) (net.Conn, error) {
              conn, errd := lis.Dial()
              assert.NoError(errd, "expect no error from creating listener")
              return conn, nil
          }
          
          conn, err := grpc.NewClient(
              "bufnet",
              grpc.WithTransportCredentials(insecure.NewCredentials()),
              grpc.WithContextDialer(dialer),
          )
          assert.NoError(err)
          
          // Cleanup resources
          t.Cleanup(func() {
              _ = repo.Dbh().Drop()
              conn.Close()
              lis.Close()
              server.Stop()
          })
          
          return feature.NewFeatureAnnotationServiceClient(conn), assert
      }
      
      // Mock implementation for message publisher
      type MockMessage struct{}
      
      func (msn *MockMessage) Publish(
          subject string,
          feat *feature.FeatureAnnotation,
      ) error {
          return nil
      }
      
      func (msn *MockMessage) Close() error {
          return nil
      }
      ```

- **Functional Programming in Tests**
    - Use collection utilities for data transformations and filtering in tests
    - Leverage `slices` package functions for sorting and searching
    - Prefer functional approaches for cleaner, more readable test code
      ```go
      // Use collection.Map for data transformations
      params.assert.ElementsMatch(
          collection.Map(
              req.Attributes.Properties,
              extractTagAndValue,
          ),
          collection.Map(
              resp.Attributes.Properties,
              extractTagAndValue,
          ),
          "should have matching properties",
      )
      
      // Use slices.SortFunc for consistent ordering
      slices.SortFunc(
          req.Attributes.Properties,
          sortTagPropertiesByTag,
      )
      slices.SortFunc(
          resp.Attributes.Properties,
          sortTagPropertiesByTag,
      )
      
      // Use slices.ContainsFunc for complex element search
      found := slices.ContainsFunc(result.Attributes.Properties,
          func(prop *feature.TagProperty) bool {
              return prop.Tag == expectedTag.Tag &&
                  prop.Value == expectedTag.Value &&
                  prop.CreatedBy == expectedTag.CreatedBy
          })
      
      // Helper functions for data extraction
      func sortTagPropertiesByTag(a, b *feature.TagProperty) int {
          return strings.Compare(
              strings.ToLower(a.Tag),
              strings.ToLower(b.Tag),
          )
      }
      
      func extractTagAndValue(prop *feature.TagProperty) *feature.TagProperty {
          return &feature.TagProperty{
              Tag:   prop.Tag,
              Value: prop.Value,
          }
      }
      ```

- **Error Handling and Assertions**
    - Create dedicated functions for GRPC error assertion to ensure consistency
    - Test all relevant error codes and messages
    - Use type assertions to verify specific error types
      ```go
      // Dedicated GRPC error assertion function
      func assertGrpcError(params assertGrpcErrorParams) {
          params.assert.Error(params.err, "expected a gRPC error")
          sts, ok := status.FromError(params.err)
          params.assert.True(ok, "error should be a gRPC status error")
          params.assert.Equal(
              params.expectedCode,
              sts.Code(),
              "expected gRPC code %s, but got %s",
              params.expectedCode,
              sts.Code(),
          )
          if params.expectedMsgSubstring != "" {
              params.assert.Contains(
                  strings.ToLower(sts.Message()), // Case-insensitive check
                  strings.ToLower(params.expectedMsgSubstring),
                  "expected gRPC error message to contain '%s', but got '%s'",
                  params.expectedMsgSubstring,
                  sts.Message(),
              )
          }
      }
      
      // Usage in tests
      func testAddTagsNonExistentFeature(params *testParams) {
          params.t.Helper()
          // ... test setup ...
          _, err := params.client.AddTags(params.ctx, addReq)
          
          params.assert.Error(err, "should return error for non-existent feature")
          assertGrpcError(assertGrpcErrorParams{
              assert:               params.assert,
              err:                  err,
              expectedCode:         codes.NotFound,
              expectedMsgSubstring: "not found",
          })
      }
      ```

- **Timestamp and Time Handling**
    - Use `assert.WithinDuration()` for timestamp tolerance testing
    - Truncate timestamps when comparing for consistent precision
    - Test both auto-generated and user-provided timestamps
      ```go
      // Verify auto-generated timestamps are recent
      params.assert.WithinDuration(
          time.Now(),
          result.Attributes.Properties[idx].CreatedAt.AsTime(),
          5*time.Second,
          "CreatedAt should be recent for tag %s",
          expectedTag.Tag,
      )
      
      // Verify provided timestamps are preserved with truncation
      params.assert.Equal(
          expectedTimestamps[idx].Truncate(time.Second),
          result.Attributes.Properties[idx].CreatedAt.AsTime().
              Truncate(time.Second),
          "CreatedAt should match provided timestamp for tag %s",
          expectedTag.Tag,
      )
      
      // Helper function for timestamp behavior testing
      func testTimestampBehavior(
          params *testParams,
          featureID string,
          withProvidedTimestamps bool,
          operation func(string, []*feature.TagPropertyCreate) (*feature.FeatureAnnotation, error),
      ) {
          params.t.Helper()
          
          var newTags []*feature.TagPropertyCreate
          var expectedTimestamps []time.Time
          
          if withProvidedTimestamps {
              newTags, expectedTimestamps = createTestTagsWithTimestamps()
          } else {
              newTags = createTestTagsWithoutTimestamps()
          }
          
          result, err := operation(created.Id, newTags)
          params.assert.NoError(err, "should successfully perform tag operation")
          
          // Verify timestamps
          verifyTagTimestamps(
              params,
              result,
              newTags,
              expectedTimestamps,
              !withProvidedTimestamps,
          )
      }
      ```

- **Test Data Management**
    - Create factory functions for consistent test data generation
    - Use unique IDs for test isolation and prevent conflicts
    - Organize test data creation in reusable helper functions
      ```go
      // Factory function for test data
      func newTestFeature() *feature.NewFeatureAnnotation {
          return &feature.NewFeatureAnnotation{
              Id:        "DDB_G0285425",
              CreatedBy: "testuser@dictybase.org",
              CreatedAt: timestamppb.Now(),
              Attributes: &feature.FeatureAnnotationAttributes{
                  Name:     "Test Feature",
                  Synonyms: []string{"test1", "test2"},
                  Properties: []*feature.TagProperty{
                      {
                          Tag:       "description",
                          Value:     "Test description",
                          CreatedBy: "testuser@dictybase.org",
                      },
                      {
                          Tag:       "note",
                          Value:     "Test note",
                          CreatedBy: "testuser@dictybase.org",
                      },
                  },
              },
          }
      }
      
      // Test data creation with parameter structs
      func createTestTagsWithTimestamps() ([]*feature.TagPropertyCreate, []time.Time) {
          specTs1 := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Microsecond)
          specTs2 := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Microsecond)
          expectedTimestamps := []time.Time{specTs1, specTs2}
          newTags := []*feature.TagPropertyCreate{
              createServiceTagPropertyCreate(&tagPropertyCreateParams{
                  tag:       "provided_timestamp1",
                  value:     "value1",
                  createdBy: "tester@example.org",
                  timestamp: &specTs1,
              }),
              createServiceTagPropertyCreate(&tagPropertyCreateParams{
                  tag:       "provided_timestamp2",
                  value:     "value2",
                  createdBy: "tester@example.org",
                  timestamp: &specTs2,
              }),
          }
          return newTags, expectedTimestamps
      }
      ```

- **Comprehensive Test Coverage**
    - Test success paths with multiple scenarios (single item, multiple items, edge cases)
    - Test validation errors for invalid input data
    - Test business logic errors for non-existent resources
    - Test edge cases like empty requests and boundary conditions
      ```go
      func TestSetTags(t *testing.T) {
          t.Parallel()
          client, assert := setup(t)
          ctx := context.Background()
          params := &testParams{
              t:      t,
              ctx:    ctx,
              client: client,
              assert: assert,
          }
          // Success scenarios
          testSetTagsSuccess(params)
          testSetTagsSingleTag(params)
          testSetTagsMultipleTags(params)
          testSetTagsReplaceExisting(params)
          
          // Edge cases
          testSetTagsEmptyRequest(params)
          
          // Timestamp handling
          testSetTagsDefaultTimestamps(params)
          testSetTagsProvidedTimestamps(params)
          
          // Error conditions
          testSetTagsNonExistentFeature(params)
          testSetTagsInvalidRequest(params)
      }
      ```

- **Test Helper Organization**
    - Mark helper functions with `t.Helper()` for better error reporting
    - Group helpers by functionality in separate files
    - Use descriptive names that clearly indicate the helper's purpose
    - Create verification functions that encapsulate complex assertion logic
      ```go
      // Verification helper with struct parameters
      func verifyServiceTagsSet(
          params *testParams,
          result *feature.FeatureAnnotation,
          expectedTags []*feature.TagPropertyCreate,
      ) {
          params.t.Helper()
          
          // Verify tag count matches exactly (should replace, not append)
          params.assert.Len(
              result.Attributes.Properties,
              len(expectedTags),
              "should have exactly the number of new tags",
          )
          
          // Verify each expected tag is present
          for _, expectedTag := range expectedTags {
              idx := slices.IndexFunc(
                  result.Attributes.Properties,
                  func(p *feature.TagProperty) bool {
                      return p.Tag == expectedTag.Tag && p.Value == expectedTag.Value
                  },
              )
              params.assert.Greater(idx, -1, "should find tag %s", expectedTag.Tag)
              params.assert.Equal(
                  expectedTag.CreatedBy,
                  result.Attributes.Properties[idx].CreatedBy,
                  "should match created by for tag %s",
                  expectedTag.Tag,
              )
          }
      }
      ```

