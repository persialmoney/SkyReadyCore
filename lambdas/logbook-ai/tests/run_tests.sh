#!/bin/bash

# Run tests for logbook-ai lambda
# Usage: ./run_tests.sh [options]
#
# Options:
#   -v, --verbose    Run tests with verbose output
#   -k EXPRESSION    Run only tests matching EXPRESSION
#   -x, --exitfirst  Exit on first test failure
#   --cov           Run with coverage report

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LAMBDA_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

echo -e "${YELLOW}Running tests for logbook-ai lambda${NC}"
echo "Lambda directory: $LAMBDA_DIR"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}pytest is not installed. Installing test dependencies...${NC}"
    pip install pytest pytest-mock pytest-cov
fi

# Change to lambda directory
cd "$LAMBDA_DIR"

# Run pytest with arguments
if [ "$1" == "--cov" ]; then
    echo -e "${YELLOW}Running tests with coverage...${NC}"
    pytest tests/ \
        --cov=. \
        --cov-report=term-missing \
        --cov-report=html \
        -v
    echo ""
    echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
else
    pytest tests/ "$@"
fi

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo ""
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
