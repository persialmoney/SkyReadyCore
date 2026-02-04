import os
import sys
import json
import boto3
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

def get_database_url():
    """Get database URL from AWS"""
    stage = os.environ.get('STAGE', 'dev')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    # Get credentials
    secrets_client = boto3.client('secretsmanager', region_name=region)
    secret_arn = os.environ.get('DB_SECRET_ARN')
    
    if not secret_arn:
        raise ValueError("DB_SECRET_ARN environment variable not set")
    
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    
    db_endpoint = os.environ.get('DB_ENDPOINT')
    db_name = os.environ.get('DB_NAME', 'logbook')
    
    if not db_endpoint:
        raise ValueError("DB_ENDPOINT environment variable not set")
    
    username = secret['username']
    password = secret['password']
    
    return f"postgresql://{username}:{password}@{db_endpoint}:5432/{db_name}"

def run_migrations_offline():
    """Run migrations in 'offline' mode."""
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Run migrations in 'online' mode."""
    configuration = config.get_section(config.config_ini_section)
    configuration['sqlalchemy.url'] = get_database_url()
    
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
