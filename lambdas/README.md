# Lambda Functions

This directory contains all Lambda functions for the SkyReady application.

## Structure

Each Lambda function is in its own directory with:
- `index.py`: Lambda handler code
- `requirements.txt`: Python dependencies

## Functions

### weather

Fetches aviation weather data (METAR, TAF, NOTAMs) from external APIs.

**Handler**: `index.handler`  
**Runtime**: Python 3.11  
**Function Name**: `sky-ready-weather-{stage}`

**Usage**: Called by AppSync as a Lambda resolver for weather queries. Uses cache-first strategy with ElastiCache ValKey.

### weather-cache-ingest

Scheduled function that downloads bulk cache files from AWC and populates ElastiCache ValKey.

**Handler**: `index.handler`  
**Runtime**: Python 3.11  
**Function Name**: `sky-ready-weather-cache-ingest-{stage}`  
**Trigger**: EventBridge schedules (METAR/SIGMET/AIRMET/PIREP: 5 min, TAF: 10 min, Stations: daily)

**Usage**: Background job to keep weather cache populated with latest data from AWC cache API.

**See**: [../WEATHER_CACHE.md](../WEATHER_CACHE.md) for detailed documentation.

### user-creation

Cognito Post-Confirmation trigger that automatically creates user profiles in DynamoDB when users confirm their email.

**Handler**: `index.handler`  
**Runtime**: Python 3.11  
**Function Name**: `sky-ready-user-creation-{stage}`  
**Trigger**: Cognito Post-Confirmation event

**Usage**: Automatically creates user records when new users sign up.

## Deployment

Lambda functions are automatically deployed via CI/CD pipeline when code is pushed to the repository.

### Automatic Deployment

1. Push code to `main` branch
2. Pipeline detects changes in `lambdas/` directory
3. Only changed Lambda functions are deployed
4. Deployment completes in ~30 seconds to 2 minutes

### Local Testing

To test a Lambda function locally:

```bash
# Install dependencies
cd lambdas/weather
pip install -r requirements.txt -t .

# Test the handler
python -c "from index import handler; print(handler({}, {}))"
```

### Manual Deployment (Development)

```bash
# Package Lambda
cd lambdas/weather
zip -r function.zip . -x "*.pyc" "__pycache__/*" "*.zip"

# Deploy
aws lambda update-function-code \
  --function-name sky-ready-weather-dev \
  --zip-file fileb://function.zip \
  --region us-east-1

# Cleanup
rm function.zip
```

## Adding a New Lambda Function

1. Create a new directory: `lambdas/{function-name}/`
2. Add `index.py` with handler function
3. Add `requirements.txt` with dependencies
4. Update the Lambda pipeline in `SkyReadyCDK` to include the new function
5. Push to repository - pipeline will automatically deploy it

## Dependencies

All Lambda functions use:
- `boto3>=1.34.0` for AWS SDK

Weather-related functions also use:
- `redis>=5.0.0` for ElastiCache ValKey access (ValKey is Redis-compatible)

Add additional dependencies to each function's `requirements.txt` as needed.

## Environment Variables

Lambda functions receive environment variables from the CDK stack:
- `STAGE`: Deployment stage (dev, gamma, prod)
- `USERS_TABLE`: DynamoDB table name
- `SAVED_AIRPORTS_TABLE`: DynamoDB table name
- `ALERTS_TABLE`: DynamoDB table name

Weather cache functions also receive:
- `ELASTICACHE_ENDPOINT`: ElastiCache ValKey cluster endpoint
- `ELASTICACHE_PORT`: ElastiCache ValKey cluster port (default: 6379)
- `CACHE_FILES_BUCKET`: S3 bucket for cache file backups (cache-ingest only)

Access via `os.environ.get('VARIABLE_NAME')` in your Lambda code.
