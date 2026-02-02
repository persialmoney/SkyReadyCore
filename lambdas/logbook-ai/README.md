# Logbook AI Lambda

Lambda function for conversational AI features on logbook entries using semantic search and chat.

## Handler

`index.handler`

## Runtime

Python 3.11

## Function Name

`sky-ready-logbook-ai-{stage}`

## Environment Variables

- `DB_SECRET_ARN`: AWS Secrets Manager ARN for database credentials
- `DB_ENDPOINT`: RDS PostgreSQL endpoint hostname
- `DB_NAME`: Database name (default: "logbook")
- `BEDROCK_REGION`: AWS region for Bedrock (default: "us-east-1")
- `EMBEDDING_MODEL_ID`: Bedrock embedding model ID (default: "amazon.titan-embed-text-v1")
- `CHAT_MODEL_ID`: Bedrock Claude model ID (default: "anthropic.claude-sonnet-4-20250517-v1:0")

## Operations

### Queries

- `searchLogbookEntries`: Semantic search using vector similarity
  - Converts query text to embedding
  - Finds entries with similar embeddings using pgvector cosine similarity
  - Returns ranked results

- `chatLogbookQuery`: Conversational AI query about logbook
  - Uses Claude Sonnet 4.5 for natural language understanding
  - Provides context from user's recent logbook entries
  - Generates helpful responses about flight history, patterns, and statistics

## AI Models

- **Embeddings**: AWS Bedrock Amazon Titan Embeddings (1024 dimensions)
- **Chat**: AWS Bedrock Claude Sonnet 4.5

## Dependencies

- `boto3>=1.34.0`: AWS SDK
- `psycopg2-binary>=2.9.9`: PostgreSQL adapter
