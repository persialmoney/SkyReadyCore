# Logbook Operations Lambda

Lambda function for CRUD operations on logbook entries with automatic vector embedding generation.

## Handler

`index.handler`

## Runtime

Python 3.11

## Function Name

`sky-ready-logbook-operations-{stage}`

## Environment Variables

- `DB_SECRET_ARN`: AWS Secrets Manager ARN for database credentials
- `DB_ENDPOINT`: RDS PostgreSQL endpoint hostname
- `DB_NAME`: Database name (default: "logbook")
- `BEDROCK_REGION`: AWS region for Bedrock (default: "us-east-1")
- `EMBEDDING_MODEL_ID`: Bedrock embedding model ID (default: "amazon.titan-embed-text-v1")

## Operations

### Queries

- `getLogbookEntry`: Get a single logbook entry by ID
- `listLogbookEntries`: List logbook entries with optional filters (date range, aircraft, flight types, status, instructor)

### Mutations

- `createLogbookEntry`: Create a new logbook entry and generate vector embedding
- `updateLogbookEntry`: Update an existing entry and regenerate embedding if text changed
- `deleteLogbookEntry`: Delete an entry and its embedding
- `requestSignature`: Change entry status to "pending-signature"
- `signEntry`: Add signature and change status to "signed"

## Embedding Generation

When entries are created or updated, the Lambda automatically:
1. Concatenates all logbook entry fields into searchable text
2. Generates a 1024-dimensional vector embedding using AWS Bedrock Titan Embeddings
3. Stores the embedding in the `logbook_entry_embeddings` table

## Dependencies

- `boto3>=1.34.0`: AWS SDK
- `psycopg2-binary>=2.9.9`: PostgreSQL adapter

## Database Schema

See `../../SkyReadyCDK/database/logbook_schema.sql` for the complete database schema.
