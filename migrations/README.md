# Database Migrations

Alembic-based database migrations for the SkyReady logbook system.

## Automatic Migrations (via Pipeline)

Migrations run automatically when you trigger the migration pipeline:
- Go to AWS CodePipeline console
- Find `sky-ready-migration-pipeline-dev`
- Click "Release change" to trigger

## Manual Migrations (CloudShell)

```bash
cd migrations
export STAGE=dev
export AWS_REGION=us-east-1
export DB_SECRET_ARN=$(aws ssm get-parameter --name "/sky-ready/$STAGE/logbook-db-secret-arn" --query 'Parameter.Value' --output text)
export DB_ENDPOINT=$(aws ssm get-parameter --name "/sky-ready/$STAGE/logbook-db-endpoint" --query 'Parameter.Value' --output text)
export DB_NAME=logbook

pip3 install -r requirements.txt --user
alembic upgrade head
```

## Creating New Migrations

```bash
alembic revision -m "add column xyz to logbook_entries"
# Edit the generated file in versions/
# Commit and push - then trigger the migration pipeline
```

## Check Migration Status

```bash
alembic current   # Shows current migration version
alembic history   # Shows all migrations
```
