# Logbook AI Lambda Tests

Comprehensive test suite for the logbook-ai Lambda function.

## Overview

This test suite covers all AI-powered functionality of the logbook-ai Lambda, including:

- **Semantic Search**: Vector-based similarity search using pgvector
- **Embedding Generation**: AWS Bedrock Titan Embeddings
- **Chat Queries**: Conversational AI using Claude Sonnet 4.5
- **Context Building**: Intelligent context selection for LLM queries
- **Data Conversion**: Database to GraphQL format conversion
- **Error Handling**: Edge cases and error scenarios

## Test Structure

```
tests/
├── __init__.py              # Package marker
├── conftest.py              # Pytest fixtures and configuration
├── test_ai_features.py      # Main test suite
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
./run_tests.sh -k TestHandleSemanticSearch

# Run specific test method
./run_tests.sh -k test_semantic_search_success

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
cd /path/to/logbook-ai

# Run all tests
pytest tests/

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=term-missing
```

## Test Classes

### TestGenerateEmbedding

Tests vector embedding generation using AWS Bedrock Titan Embeddings.

- Successful embedding generation
- Bedrock API request format
- Empty response handling
- API error handling
- 1024-dimensional vectors

### TestInvokeClaudeChat

Tests Claude Sonnet 4.5 chat invocation.

- Successful Claude invocation
- Message formatting
- System prompt handling
- Response parsing (list and string formats)
- Unexpected response format handling
- API error handling

### TestHandleSemanticSearch

Tests semantic search using vector similarity.

- Successful search with results
- Query embedding generation
- pgvector cosine similarity
- Custom result limits
- No results scenarios
- Missing query validation

### TestHandleChatQuery

Tests conversational AI queries about logbook data.

- Successful chat query
- Context building from logbook entries
- Claude integration
- System prompt configuration
- No entries scenario
- Context limiting (recent entries only)
- Missing query validation

### TestConvertDbEntryToGraphQL

Tests conversion between database format and GraphQL format.

- Complete entry conversion
- None value handling
- DateTime formatting

### TestHandler

Tests the main Lambda handler routing.

- Route to semantic search
- Route to chat query
- Missing user ID validation
- Unknown field name handling
- Error handling and logging

### TestIntegration

End-to-end integration tests.

- Complete semantic search flow (embedding → query → results)
- Complete chat query flow (context → Claude → response)
- AWS service integration
- Database integration

## Fixtures

### Mock Fixtures

- `mock_db_connection`: Mocked PostgreSQL connection and cursor
- `mock_bedrock_client`: Mocked AWS Bedrock client
- `mock_secrets_client`: Mocked AWS Secrets Manager client

### Data Fixtures

- `sample_embedding`: 1024-dimensional embedding vector
- `sample_logbook_entries`: Multiple logbook entries with searchable text
- `sample_search_query`: Example semantic search query
- `sample_chat_query`: Example conversational query
- `sample_claude_response`: Example Claude API response
- `sample_appsync_search_event`: AppSync event for semantic search
- `sample_appsync_chat_event`: AppSync event for chat query

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

## AI Models Tested

### AWS Bedrock Titan Embeddings
- Model: `amazon.titan-embed-text-v1`
- Dimensions: 1024
- Use: Vector embeddings for semantic search

### Claude Sonnet 4.5
- Model: `anthropic.claude-sonnet-4-20250517-v1:0`
- Max Tokens: 4096
- Use: Conversational AI queries

## Writing New Tests

### Test Structure

```python
class TestMyFeature:
    """Test description."""
    
    @patch('index.bedrock_client')
    def test_success_case(self, mock_bedrock, fixture_name):
        """Test successful operation."""
        # Arrange
        mock_bedrock.invoke_model.return_value = {...}
        
        # Act
        result = my_function()
        
        # Assert
        assert result is not None
```

### Mocking Bedrock Responses

```python
@patch('index.bedrock_client')
def test_bedrock_call(self, mock_bedrock):
    """Test Bedrock API call."""
    response = {
        'body': MagicMock()
    }
    response['body'].read.return_value = json.dumps({
        'embedding': [0.1, 0.2, 0.3]
    }).encode('utf-8')
    mock_bedrock.invoke_model.return_value = response
    
    result = generate_embedding("test text")
    assert len(result) == 3
```

### Testing Semantic Search

```python
@patch('index.generate_embedding')
@patch('index.get_db_connection')
def test_semantic_search(self, mock_get_conn, mock_embedding):
    """Test semantic search."""
    mock_embedding.return_value = [0.1] * 1024
    # Mock database results with similarity scores
    # Test query execution
```

## Continuous Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run AI Lambda Tests
  run: |
    cd SkyReadyCore/lambdas/logbook-ai
    pip install -r requirements.txt
    pip install pytest pytest-mock pytest-cov
    pytest tests/ --cov=. --cov-report=xml
```

## Coverage Goals

Target coverage: **≥ 80%**

Current areas:
- Handler routing: ~100%
- Embedding generation: ~95%
- Semantic search: ~95%
- Chat queries: ~90%
- Claude integration: ~90%
- Data conversion: ~100%

## Performance Considerations

### Embedding Generation
- Titan Embeddings: ~100-200ms per call
- Batch operations not tested (would be integration tests)

### Semantic Search
- pgvector similarity: Fast (indexed)
- Result limit affects performance

### Chat Queries
- Claude Sonnet: ~2-5 seconds per query
- Context size affects latency
- Limited to 20 recent entries for performance

## Troubleshooting

### Import Errors

Ensure the parent directory is in the Python path:

```python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

### Mock Issues

Check the patch path matches the import path:

```python
# Correct
@patch('index.bedrock_client')

# For imports like: from module import client
@patch('index.client')
```

### JSON Encoding Issues

When mocking Bedrock responses, ensure proper JSON encoding:

```python
response['body'].read.return_value = json.dumps(data).encode('utf-8')
```

### Vector Dimension Mismatches

Titan embeddings are 1024-dimensional. Ensure test embeddings match:

```python
sample_embedding = [0.1] * 1024  # Correct dimension
```

## Security Notes

- Tests use mocked AWS credentials (no real API calls)
- No sensitive data in test fixtures
- Database connections are mocked
- Bedrock API calls are mocked (no actual cost)

## Related Documentation

- [Logbook AI Lambda README](../README.md)
- [Logbook Operations Tests](../../logbook-operations/tests/README.md)
- [Database Schema](../../../SkyReadyCDK/database/logbook_schema.sql)
- [GraphQL Schema](../../../SkyReadyCDK/graphql/schema.graphql)
- [AWS Bedrock Titan Embeddings](https://docs.aws.amazon.com/bedrock/latest/userguide/titan-embedding-models.html)
- [AWS Bedrock Claude](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-claude.html)
