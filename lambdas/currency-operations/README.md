# Currency Operations Lambda

This Lambda function calculates pilot currency requirements based on logbook entries and medical certificate data.

## Purpose

Handles the `getCurrency` GraphQL query in the SkyReady AppSync API. Calculates and returns currency status for:
- Day currency (passenger carrying)
- Night currency (passenger carrying at night)
- IFR currency (instrument flight rules)
- Flight review (biennial)
- Medical certificate

## Environment Variables

- `DB_SECRET_ARN`: ARN of the Secrets Manager secret containing PostgreSQL credentials
- `DB_ENDPOINT`: PostgreSQL RDS instance endpoint
- `DB_NAME`: PostgreSQL database name (default: `logbook`)
- `STAGE`: Deployment stage (dev, staging, prod)

## Dependencies

- `boto3`: AWS SDK for Secrets Manager and DynamoDB access
- `psycopg2-binary`: PostgreSQL database driver

## Currency Calculations

### Day Currency (14 CFR 61.57(a))
- Requires 3 takeoffs and landings within preceding 90 days
- Status: CURRENT (>15 days), EXPIRING (1-15 days), EXPIRED

### Night Currency (14 CFR 61.57(b))
- Requires 3 night takeoffs and full-stop landings within preceding 90 days
- Night defined as: 1 hour after sunset to 1 hour before sunrise
- Status: CURRENT (>15 days), EXPIRING (1-15 days), EXPIRED

### IFR Currency (14 CFR 61.57(c))
- Requires within preceding 6 months:
  - 6 instrument approaches
  - Holding procedures
  - Intercepting and tracking courses
- Status: CURRENT (>15 days), EXPIRING (1-15 days), EXPIRED

### Flight Review (14 CFR 61.56)
- Required every 24 calendar months
- Must be logged with instructor signature
- Status: CURRENT (>30 days), EXPIRING (1-30 days), EXPIRED

### Medical Certificate (14 CFR 61.23)
- Expiration varies by class and pilot age
- Class 1/2/3: 60 months (under 40) or 24 months (40+) for private operations
- Status: CURRENT (>30 days), EXPIRING (1-30 days), EXPIRED, NOT_APPLICABLE

## Data Sources

- **PostgreSQL**: Logbook entries (flights, landings, approaches, etc.)
- **DynamoDB**: User medical certificate information

## Error Handling

- Comprehensive None-checking to prevent crashes with missing data
- Graceful degradation when user has no logbook entries
- Returns NOT_APPLICABLE status for missing medical certificate data
- Detailed logging for debugging

## Deployment

Deployed via CDK infrastructure and CI/CD pipeline:
- Function name: `sky-ready-currency-operations-{stage}`
- Timeout: 30 seconds
- Memory: 256 MB
- VPC: Same as logbook database for connectivity

## Testing

Test the Lambda locally or via GraphQL query:

```graphql
query GetCurrency {
  getCurrency {
    dayCurrency {
      name
      status
      daysRemaining
      validUntil
      details
      explanation
      requirements
    }
    nightCurrency { ... }
    ifrCurrency { ... }
    flightReview { ... }
    medicalCertificate { ... }
  }
}
```

## Related Lambdas

- **logbook-operations**: Handles CRUD operations for logbook entries
- Shares database utilities via `shared/db_utils.py` module
