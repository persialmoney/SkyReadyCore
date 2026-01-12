# SkyReady Core Package

This package contains the core business logic for SkyReady, including all Lambda functions that power the application.

## Architecture

SkyReady uses a serverless architecture:
- **AppSync**: GraphQL API (managed by CDK)
- **Lambda Functions**: Business logic (in this package)
- **DynamoDB**: Data storage (managed by CDK)
- **Cognito**: Authentication (managed by CDK)

The ECS service has been removed in favor of a fully serverless architecture.

## Project Structure

```
SkyReadyCore/
├── lambdas/                    # Lambda functions
│   ├── weather/               # Weather data fetching Lambda (AppSync resolver)
│   │   ├── index.py
│   │   └── requirements.txt
│   ├── weather-cache-ingest/  # Cache ingestion Lambda (EventBridge scheduled)
│   │   ├── index.py
│   │   ├── requirements.txt
│   │   └── SCHEMA.md          # ElastiCache ValKey schema documentation
│   ├── user-creation/          # Cognito post-confirmation Lambda
│   │   ├── index.py
│   │   └── requirements.txt
│   └── README.md
├── README.md                 # This file
├── WEATHER_CACHE.md          # Weather cache system documentation
├── MIGRATION_SUMMARY.md      # Migration documentation
└── RENAME_COMPLETE.md        # Rename documentation
```

## Lambda Functions

### Weather Lambda (`lambdas/weather/`)

AppSync resolver that fetches aviation weather data (METAR, TAF, NOTAMs) using cache-first strategy.

- **Handler**: `index.handler`
- **Runtime**: Python 3.11
- **Used as**: AppSync Lambda resolver
- **Function Name**: `sky-ready-weather-{stage}`
- **Strategy**: Cache-first with API fallback
  - Checks ElastiCache ValKey first (<1ms latency)
  - Falls back to AWC API if cache miss
  - Write-through caching for next request

**Dependencies:**
- `boto3>=1.34.0`
- `redis>=5.0.0` (ValKey-compatible Redis client)

**See**: [WEATHER_CACHE.md](WEATHER_CACHE.md) for detailed cache system documentation.

### Weather Cache Ingest Lambda (`lambdas/weather-cache-ingest/`)

Scheduled function that downloads bulk cache files from AWC and populates ElastiCache ValKey.

- **Handler**: `index.handler`
- **Runtime**: Python 3.11
- **Trigger**: EventBridge schedules
  - METAR/SIGMET/AIRMET/PIREP: Every 5 minutes
  - TAF: Every 10 minutes
  - Stations: Daily at midnight UTC
- **Function Name**: `sky-ready-weather-cache-ingest-{stage}`

**Process**:
1. Downloads gzipped cache files from AWC
2. Decompresses and parses (CSV/XML/JSON)
3. Stores in ElastiCache ValKey with TTLs
4. Updates index sets for efficient queries
5. Backs up raw files to S3

**Dependencies:**
- `boto3>=1.34.0`
- `redis>=5.0.0` (ValKey-compatible Redis client)

**See**: [WEATHER_CACHE.md](WEATHER_CACHE.md) for detailed cache system documentation.

### User Creation Lambda (`lambdas/user-creation/`)

Cognito Post-Confirmation trigger that automatically creates user profiles in DynamoDB when users confirm their email.

- **Handler**: `index.handler`
- **Runtime**: Python 3.11
- **Trigger**: Cognito Post-Confirmation event
- **Function Name**: `sky-ready-user-creation-{stage}`

**Dependencies:**
- `boto3>=1.34.0`

## Lambda Deployment

Lambda functions are automatically deployed via CI/CD pipeline when code is pushed to the repository.

### Automatic Deployment

The Lambda deployment pipeline watches this repository and automatically deploys Lambda functions when:
- Code is pushed to the `main` branch
- Files in `lambdas/` directory are changed

**Smart Deployment**: Only Lambda functions that have changed are deployed, making deployments fast and efficient (~30 seconds to 2 minutes).

### Manual Deployment (Development)

For local testing, you can manually deploy a Lambda function:

```bash
# Deploy weather Lambda
cd lambdas/weather
zip -r function.zip . -x "*.pyc" "__pycache__/*"
aws lambda update-function-code \
  --function-name sky-ready-weather-dev \
  --zip-file fileb://function.zip \
  --region us-east-1
```

### Pipeline Details

- **Pipeline Name**: `sky-ready-lambda-pipeline-{stage}`
- **Build Project**: `sky-ready-lambda-deploy-{stage}`
- **Trigger**: Push to `main` branch
- **Deployment Time**: ~30 seconds to 2 minutes (only changed functions)

See `SkyReadyCDK/LAMBDA_PIPELINE.md` for detailed pipeline documentation.

## Development Workflow

### Making Lambda Changes

1. **Edit Lambda code** in `lambdas/{function-name}/`
2. **Test locally** (if possible)
3. **Commit and push** to repository
4. **Pipeline automatically deploys** only the changed Lambda function(s)

### Local Testing

To test a Lambda function locally:

```bash
# Install dependencies
cd lambdas/weather
pip install -r requirements.txt -t .

# Test the handler
python -c "from index import handler; print(handler({}, {}))"
```

## Environment Variables

Lambda functions receive environment variables from the CDK stack:

**Common Variables**:
- `STAGE`: Deployment stage (dev, gamma, prod)
- `USERS_TABLE`: DynamoDB table name for users
- `SAVED_AIRPORTS_TABLE`: DynamoDB table name for saved airports
- `ALERTS_TABLE`: DynamoDB table name for alerts

**Weather Cache Variables** (for weather and cache-ingest Lambdas):
- `ELASTICACHE_ENDPOINT`: ElastiCache ValKey cluster endpoint
- `ELASTICACHE_PORT`: ElastiCache ValKey cluster port (default: 6379)
- `CACHE_FILES_BUCKET`: S3 bucket name for cache file backups (cache-ingest only)

Access via `os.environ.get('VARIABLE_NAME')` in your Lambda code.

## Adding a New Lambda Function

1. Create a new directory: `lambdas/{function-name}/`
2. Add `index.py` with handler function
3. Add `requirements.txt` with dependencies
4. Update the Lambda pipeline in `SkyReadyCDK` to include the new function
5. Push to repository - pipeline will automatically deploy it

## Dependencies

Each Lambda function has its own `requirements.txt` file:

- **Common**: `boto3>=1.34.0` for AWS SDK
- **Weather Functions**: `redis>=5.0.0` for ElastiCache ValKey access
  - Note: ValKey is Redis-compatible, so we use the standard `redis` Python library

Add additional dependencies to each function's `requirements.txt` as needed.

## Security

- Lambda functions use least-privilege IAM roles
- Environment variables for configuration
- No hardcoded secrets
- VPC configuration (if needed) managed by CDK

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test locally
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
