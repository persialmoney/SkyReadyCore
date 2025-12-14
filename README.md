# Base ECS Core Service

This is a base ECS Core service that can be customized for different projects. It provides a foundation for building containerized applications on AWS ECS.

## Project Configuration

The project can be customized for different applications by modifying the configuration in `app/base-config.py`. The main configuration variables are:

- `PROJECT_NAME`: The name of your project (default: "SkyReady")
- `PROJECT_DESCRIPTION`: Description of your project
- `SERVICE_NAME`: The name of your ECS service
- `SERVICE_DESCRIPTION`: Description of your ECS service

## Prerequisites

- Python 3.9 or later
- Docker
- AWS CLI configured with appropriate credentials
- AWS CDK CLI installed (`npm install -g aws-cdk`)

## Setup

1. Create and activate a Python virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```

## Development

1. Build the Docker image:
```bash
docker build -t base-ecs-core .
```

2. Run the container locally:
```bash
docker run -p 3000:3000 base-ecs-core
```

The service will be available at `http://localhost:3000`

## Project Structure

- `app/`: Application code directory
  - `base-config.py`: Project configuration
  - `main.py`: Main application entry point
  - `handlers/`: Request handlers
  - `models/`: Data models
  - `utils/`: Utility functions
- `Dockerfile`: Container definition
- `requirements.txt`: Python dependencies

## Customization

To use this service for a different project:

1. Copy the entire `BaseECSCore` directory to your new project
2. Update the configuration in `app/base-config.py`
3. Modify the application code in `app/`
4. Update the Dockerfile if needed
5. Deploy using the CDK infrastructure

## Environment Variables

The service uses the following environment variables:

- `SERVICE_NAME`: Name of the service
- `STAGE`: Deployment stage (dev, gamma, prod)
- `REGION`: AWS region
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Security

- Container security best practices
- Environment variables for sensitive configuration
- IAM roles with minimum required permissions
- Network security through VPC and security groups

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 