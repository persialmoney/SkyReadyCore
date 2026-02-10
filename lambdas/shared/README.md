# SkyReady Shared Lambda Layer

This directory contains shared code and dependencies used by multiple Lambda functions.

## Structure

```
shared/
  python/           # Lambda Layer contents (available at /opt/python in Lambda runtime)
    db_utils.py     # Database connection utilities
    __init__.py     # Package marker
    <dependencies>  # Installed Python packages (psycopg2-binary, boto3, etc.)
  requirements.txt  # Dependencies to install in python/ directory
  build-layer.sh    # Script to build layer with Linux dependencies
```

## Building the Layer

The Lambda Layer is automatically built by CDK during deployment. CDK packages the `shared/` directory, which contains the `python/` subdirectory with all code and dependencies.

To manually build locally for testing:
```bash
./build-layer.sh
```

This installs Linux-compatible dependencies into `python/` directory.

## Usage in Lambda Functions

Lambda functions that use this layer import shared utilities directly:
```python
from db_utils import get_db_connection
```

The layer is available at `/opt/python/` in the Lambda runtime environment.

## Lambdas Using This Layer

- sync-push
- sync-pull
- outbox-processor
