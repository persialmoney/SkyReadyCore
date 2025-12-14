"""
Configuration settings for the ECS Core service.
This file contains all project-specific variables that can be customized for different projects.
"""

# Project Configuration
PROJECT_NAME = "SkyReady"
PROJECT_DESCRIPTION = "SkyReady ECS Core service for containerized applications"

# Service Configuration
SERVICE_NAME = f"{PROJECT_NAME}Core"
SERVICE_DESCRIPTION = f"Core service for {PROJECT_NAME}"

# Stage Configuration
STAGES = {
    "DEV": "dev",
    "GAMMA": "gamma",
    "PROD": "prod"
}

# Default Configuration
DEFAULT_CONFIG = {
    "memory": 512,
    "cpu": 256,
    "region": "us-east-1",
    "stage": STAGES["DEV"]
}

# Environment Variables
ENV_VARS = {
    "STAGE": "STAGE",
    "REGION": "REGION",
    "LOG_LEVEL": "LOG_LEVEL"
}

# Resource Name Generators
def get_service_name(stage: str) -> str:
    return f"{SERVICE_NAME}-{stage}"

def get_service_description(stage: str) -> str:
    return f"{SERVICE_DESCRIPTION} - {stage}"

def get_container_name(stage: str) -> str:
    return f"{SERVICE_NAME}-{stage}"

def get_task_family(stage: str) -> str:
    return f"{SERVICE_NAME}-{stage}"

# Logging Configuration
LOG_LEVELS = {
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "WARNING": "WARNING",
    "ERROR": "ERROR",
    "CRITICAL": "CRITICAL"
}

DEFAULT_LOG_LEVEL = LOG_LEVELS["INFO"] 