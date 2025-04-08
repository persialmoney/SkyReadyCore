# Financial Transactions GraphQL Service

This service provides a GraphQL API for accessing financial transaction data through Plaid's API.

## Prerequisites

- Python 3.11+
- Docker
- Plaid API credentials

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in your Plaid credentials:
   ```bash
   cp .env.example .env
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running Locally

```bash
uvicorn app.main:app --reload
```

The service will be available at:
- GraphQL endpoint: http://localhost:8000/graphql
- Health check: http://localhost:8000/health

## Docker Build and Run

```bash
docker build -t financial-transactions-service .
docker run -p 8000:8000 --env-file .env financial-transactions-service
```

## GraphQL Usage

Example query:
```graphql
query {
  transactions(
    accessToken: "your_access_token"
    startDate: "2024-01-01"
    endDate: "2024-01-31"
  ) {
    id
    amount
    date
    name
    category
    merchantName
  }
}
```

## Deployment to ECS

1. Push the Docker image to Amazon ECR
2. Create an ECS task definition with the container image
3. Configure environment variables in the task definition
4. Create an ECS service using the task definition
5. Set up an Application Load Balancer if needed

Make sure to configure appropriate security groups and IAM roles for your ECS deployment. 