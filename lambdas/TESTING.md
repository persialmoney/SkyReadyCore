# SkyReady Lambda Testing Guide

Comprehensive testing documentation for all SkyReady Lambda functions.

## Overview

The SkyReady Core Lambda functions include comprehensive test suites covering:

- **Unit Tests**: Individual function testing with mocked dependencies
- **Integration Tests**: End-to-end flows with multiple components
- **Error Handling**: Edge cases and failure scenarios
- **AWS Service Mocking**: Bedrock, Secrets Manager, RDS integration

## Lambda Test Suites

### 1. Logbook Operations (`logbook-operations/`)

**Purpose**: CRUD operations for logbook entries with automatic vector embeddings

**Test Coverage**:
- Entry creation, retrieval, update, deletion
- Vector embedding generation (AWS Bedrock Titan)
- Filtering by date, aircraft, flight types, status, instructor
- Signature request and signing workflows
- Database transaction handling

**Documentation**: [logbook-operations/tests/README.md](logbook-operations/tests/README.md)

**Run Tests**:
```bash
cd logbook-operations/tests
./run_tests.sh
```

### 2. Logbook AI (`logbook-ai/`)

**Purpose**: Semantic search and conversational AI for logbook queries

**Test Coverage**:
- Semantic search using vector similarity (pgvector)
- Embedding generation for search queries
- Claude Sonnet 4.5 chat integration
- Context building from logbook entries
- Natural language query processing

**Documentation**: [logbook-ai/tests/README.md](logbook-ai/tests/README.md)

**Run Tests**:
```bash
cd logbook-ai/tests
./run_tests.sh
```

### 3. Weather (`weather/`)

**Purpose**: Weather data retrieval and TAF/METAR parsing

**Test Coverage**:
- TAF parsing (structured and raw formats)
- AVWX integration
- Visibility parsing
- Cloud condition parsing
- Time conversion

**Documentation**: [weather/tests/README.md](weather/tests/README.md)

**Run Tests**:
```bash
cd weather/tests
./run_tests.sh
```

## Quick Start

### Install Test Dependencies

```bash
# For all lambdas
pip install pytest pytest-mock pytest-cov

# Or use requirements-test.txt for each lambda
cd logbook-operations
pip install -r requirements.txt -r requirements-test.txt
```

### Run All Tests

```bash
# From the lambdas directory
for lambda_dir in logbook-operations logbook-ai weather; do
    echo "Testing $lambda_dir..."
    cd $lambda_dir/tests
    ./run_tests.sh
    cd ../..
done
```

### Run with Coverage

```bash
cd logbook-operations/tests
./run_tests.sh --cov

cd logbook-ai/tests
./run_tests.sh --cov
```

## Test Organization

### Directory Structure

```
lambdas/
├── logbook-operations/
│   ├── index.py
│   ├── requirements.txt
│   ├── requirements-test.txt
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py           # Fixtures
│       ├── test_operations.py    # Unit tests
│       ├── run_tests.sh          # Test runner
│       └── README.md
├── logbook-ai/
│   ├── index.py
│   ├── requirements.txt
│   ├── requirements-test.txt
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py
│       ├── test_ai_features.py
│       ├── run_tests.sh
│       └── README.md
├── weather/
│   └── tests/
│       └── ...
└── TESTING.md                    # This file
```

### Test File Naming

- `test_*.py`: Test modules
- `conftest.py`: Shared fixtures and configuration
- `run_tests.sh`: Executable test runner script

## Common Testing Patterns

### 1. Mocking AWS Services

```python
@patch('index.bedrock_client')
def test_with_bedrock(mock_bedrock):
    """Test with mocked Bedrock client."""
    response = {
        'body': MagicMock()
    }
    response['body'].read.return_value = json.dumps({
        'embedding': [0.1, 0.2, 0.3]
    }).encode('utf-8')
    mock_bedrock.invoke_model.return_value = response
    
    result = generate_embedding("test")
    assert len(result) == 3
```

### 2. Mocking Database Connections

```python
@patch('index.get_db_connection')
@patch('index.return_db_connection')
def test_with_db(mock_return_conn, mock_get_conn, mock_db_connection):
    """Test with mocked database."""
    conn, cursor = mock_db_connection
    mock_get_conn.return_value = conn
    cursor.fetchone.return_value = {'id': '123'}
    
    result = get_entry('123')
    assert result['id'] == '123'
```

### 3. Testing AppSync Events

```python
def test_handler_routing():
    """Test Lambda handler routing."""
    event = {
        'identity': {'sub': 'user-123'},
        'arguments': {'input': {...}},
        'info': {'fieldName': 'createEntry'}
    }
    
    result = handler(event, None)
    assert result is not None
```

### 4. Using Fixtures

```python
def test_with_fixtures(
    sample_logbook_entry,
    mock_db_connection
):
    """Test using fixtures from conftest.py."""
    # Fixtures are automatically available
    assert sample_logbook_entry['date'] is not None
```

## Coverage Goals

| Lambda | Target | Current |
|--------|--------|---------|
| logbook-operations | ≥80% | ~90% |
| logbook-ai | ≥80% | ~88% |
| weather | ≥80% | ~85% |

### Viewing Coverage Reports

```bash
# Run with coverage
./run_tests.sh --cov

# Open HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Lambda Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        lambda: [logbook-operations, logbook-ai, weather]
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          cd SkyReadyCore/lambdas/${{ matrix.lambda }}
          pip install -r requirements.txt -r requirements-test.txt
      
      - name: Run tests
        run: |
          cd SkyReadyCore/lambdas/${{ matrix.lambda }}/tests
          pytest . --cov=.. --cov-report=xml
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
```

## Best Practices

### 1. Test Independence

Each test should be independent and not rely on other tests:

```python
# Good: Each test sets up its own data
def test_create_entry(self):
    entry = {'date': '2024-01-15'}
    result = create_entry(entry)
    assert result is not None

# Bad: Tests depend on each other
def test_create_then_get_entry(self):
    # Creates entry
    # Then retrieves it
    # Both operations tested together
```

### 2. Clear Test Names

Use descriptive test names that explain what is being tested:

```python
# Good
def test_create_entry_with_missing_required_field_raises_error(self):
    ...

# Bad
def test_entry_1(self):
    ...
```

### 3. Arrange-Act-Assert Pattern

Structure tests clearly:

```python
def test_example(self):
    # Arrange: Set up test data
    entry = {'date': '2024-01-15'}
    
    # Act: Perform the operation
    result = create_entry(entry)
    
    # Assert: Verify the result
    assert result['entryId'] is not None
```

### 4. Mock External Dependencies

Always mock AWS services, databases, and external APIs:

```python
@patch('index.boto3.client')
@patch('index.psycopg2.connect')
def test_with_mocks(mock_connect, mock_boto):
    # Test with mocked dependencies
    ...
```

### 5. Test Error Paths

Test both success and failure scenarios:

```python
def test_success_case(self):
    result = my_function(valid_input)
    assert result is not None

def test_failure_case(self):
    with pytest.raises(ValueError):
        my_function(invalid_input)
```

## Troubleshooting

### Common Issues

#### Import Errors

```python
# Add parent directory to path in conftest.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

#### Mock Not Working

Check the patch path matches the actual import:

```python
# If index.py has: import boto3
@patch('index.boto3')

# If index.py has: from mymodule import function
@patch('index.function')
```

#### Fixture Not Found

Ensure conftest.py is in the tests directory and properly named.

#### Coverage Not Recording

Run pytest from the lambda root directory:

```bash
cd logbook-operations
pytest tests/ --cov=.
```

## Performance Testing

For performance-critical lambdas, add timing tests:

```python
import time

def test_performance(self):
    start = time.time()
    result = expensive_operation()
    duration = time.time() - start
    
    assert duration < 1.0  # Should complete in < 1 second
```

## Integration Testing

For integration tests with real AWS services (not in this suite):

```python
@pytest.mark.integration
def test_real_bedrock_call():
    """Integration test (requires AWS credentials)."""
    # This test actually calls AWS
    ...
```

Run integration tests separately:

```bash
pytest -m integration
```

## Resources

### Documentation
- [pytest documentation](https://docs.pytest.org/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
- [AWS Boto3 mocking](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/stubber.html)

### Related Files
- [Database Schema](../../SkyReadyCDK/database/logbook_schema.sql)
- [GraphQL Schema](../../SkyReadyCDK/graphql/schema.graphql)
- [Lambda README](README.md)

## Contributing

When adding new Lambda functions:

1. Create a `tests/` directory in the lambda folder
2. Add `conftest.py` with appropriate fixtures
3. Create test files following the naming convention
4. Add a `run_tests.sh` script
5. Document tests in `tests/README.md`
6. Update this file with the new lambda

### Test Checklist

- [ ] Unit tests for all public functions
- [ ] Error handling tests
- [ ] Mock all external dependencies
- [ ] Add fixtures for common test data
- [ ] Document test coverage
- [ ] Add run_tests.sh script
- [ ] Update main TESTING.md

## Questions?

For questions about testing:
- Check individual lambda test READMEs
- Review existing test files for examples
- See pytest documentation for framework questions
