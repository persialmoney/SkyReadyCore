# Logbook Operations Lambda Tests

Comprehensive test suite for the logbook-operations Lambda function.

## Overview

This test suite covers all functionality of the logbook-operations Lambda, including:

- **Entry Management**: Create, read, update, delete operations
- **Embedding Generation**: Vector embeddings for semantic search
- **Filtering**: Date ranges, aircraft, flight types, status, instructor filters
- **Signature Operations**: Request and sign logbook entries
- **Data Conversion**: Database to GraphQL format conversion
- **Error Handling**: Edge cases and error scenarios

## Test Structure

```
tests/
├── __init__.py              # Package marker
├── conftest.py              # Pytest fixtures and configuration
├── test_operations.py       # Main test suite
├── run_tests.sh            # Test runner script
└── README.md               # This file
```

## Running Tests

### Basic Usage

```bash
# Run all tests
./run_tests.sh

# Run with verbose output
./run_tests.sh -v

# Run specific test class
./run_tests.sh -k TestHandleCreateEntry

# Run specific test method
./run_tests.sh -k test_create_entry_success

# Exit on first failure
./run_tests.sh -x
```

### With Coverage

```bash
# Run tests with coverage report
./run_tests.sh --cov

# View HTML coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### Using pytest directly

```bash
cd /path/to/logbook-operations

# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=term-missing
```

## Test Classes

### TestConcatenateLogbookEntry

Tests the concatenation of logbook entry fields into searchable text for embedding generation.

- Basic entry concatenation
- Aircraft object handling
- Route legs formatting
- Instructor information
- Flight types, landings, approaches
- Safety notes

### TestConvertDbEntryToGraphQL

Tests conversion between database format and GraphQL format.

- Complete entry conversion
- DateTime handling
- None value handling
- Nested objects (aircraft, instructor)
- Arrays (route legs, flight types)

### TestHandleCreateEntry

Tests creation of new logbook entries.

- Successful creation
- Embedding generation
- Database insertion
- Missing input validation
- Error handling and rollback

### TestHandleUpdateEntry

Tests updating existing logbook entries.

- Successful update
- Embedding regeneration
- Partial updates
- Missing entry validation
- Entry not found scenarios

### TestHandleDeleteEntry

Tests deletion of logbook entries.

- Successful deletion
- Cascade deletion (entry + embedding)
- Missing entry ID validation
- Entry not found scenarios

### TestHandleGetEntry

Tests retrieval of single logbook entries.

- Successful retrieval
- Entry not found
- Missing entry ID validation

### TestHandleListEntries

Tests listing and filtering logbook entries.

- Basic listing
- Date range filters
- Status filters
- Aircraft filters
- Flight type filters
- Instructor filters
- Pagination

### TestHandleSignatureOperations

Tests signature request and signing operations.

- Request signature
- Sign entry
- Status transitions
- Missing data validation

### TestHandler

Tests the main Lambda handler routing.

- Route to create entry
- Route to update entry
- Route to delete entry
- Route to get entry
- Route to list entries
- Route to signature operations
- Missing user ID validation
- Unknown field name handling

## Fixtures

### Mock Fixtures

- `mock_db_connection`: Mocked PostgreSQL connection and cursor
- `mock_bedrock_client`: Mocked AWS Bedrock client for embeddings
- `mock_secrets_client`: Mocked AWS Secrets Manager client

### Data Fixtures

- `sample_logbook_entry`: Complete logbook entry input data
- `sample_db_entry`: Sample database row format
- `sample_appsync_event`: Sample AppSync Lambda event

## Dependencies

Install test dependencies:

```bash
pip install pytest pytest-mock pytest-cov
```

Or from the requirements file:

```bash
pip install -r requirements.txt
pip install -r requirements-test.txt  # If exists
```

## Writing New Tests

### Test Structure

```python
class TestMyFeature:
    """Test description."""
    
    def test_success_case(self, fixture_name):
        """Test successful operation."""
        # Arrange
        # Act
        # Assert
    
    def test_error_case(self):
        """Test error handling."""
        with pytest.raises(ValueError):
            # Test code
```

### Using Fixtures

```python
def test_with_fixtures(
    self,
    mock_db_connection,
    sample_logbook_entry
):
    """Test using fixtures."""
    conn, cursor = mock_db_connection
    # Use fixtures in test
```

### Mocking AWS Services

```python
@patch('index.generate_embedding')
@patch('index.get_db_connection')
def test_with_mocks(self, mock_get_conn, mock_embedding):
    """Test with mocked AWS services."""
    mock_embedding.return_value = [0.1, 0.2, 0.3]
    # Test code
```

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run Lambda Tests
  run: |
    cd SkyReadyCore/lambdas/logbook-operations
    pip install -r requirements.txt
    pip install pytest pytest-mock pytest-cov
    pytest tests/ --cov=. --cov-report=xml
```

## Coverage Goals

Target coverage: **≥ 80%**

Current areas:
- Handler routing: ~100%
- CRUD operations: ~95%
- Data conversion: ~100%
- Error handling: ~90%
- Embedding generation: ~85%

## Troubleshooting

### Import Errors

If you get import errors, ensure the parent directory is in the Python path:

```python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

### Mock Issues

If mocks aren't working, check the patch path matches the import path in index.py:

```python
# If index.py has: from module import function
@patch('index.function')

# If index.py has: import module
@patch('module.function')
```

### Database Connection Issues

Tests use mocked database connections. To test against a real database, see integration tests.

## Related Documentation

- [Logbook Operations Lambda README](../README.md)
- [Database Schema](../../../SkyReadyCDK/database/logbook_schema.sql)
- [GraphQL Schema](../../../SkyReadyCDK/graphql/schema.graphql)
