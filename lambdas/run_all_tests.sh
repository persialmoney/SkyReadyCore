#!/bin/bash

# Run all Lambda tests in the SkyReadyCore package
# Usage: ./run_all_tests.sh [options]
#
# Options:
#   -v, --verbose    Run tests with verbose output
#   --cov           Run with coverage report
#   -x, --exitfirst  Exit on first test failure

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  SkyReady Lambda Test Suite${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}pytest is not installed. Installing test dependencies...${NC}"
    pip install pytest pytest-mock pytest-cov
    echo ""
fi

# Lambda directories to test
LAMBDAS=(
    "logbook-operations"
    "logbook-ai"
    "weather"
)

# Track results
TOTAL_LAMBDAS=0
PASSED_LAMBDAS=0
FAILED_LAMBDAS=0
FAILED_LAMBDA_NAMES=()

# Run tests for each lambda
for lambda_name in "${LAMBDAS[@]}"; do
    lambda_path="$SCRIPT_DIR/$lambda_name"
    
    # Skip if directory doesn't exist or no tests
    if [ ! -d "$lambda_path/tests" ]; then
        echo -e "${YELLOW}⊘ Skipping $lambda_name (no tests directory)${NC}"
        echo ""
        continue
    fi
    
    TOTAL_LAMBDAS=$((TOTAL_LAMBDAS + 1))
    
    echo -e "${YELLOW}▶ Testing: $lambda_name${NC}"
    echo "  Location: $lambda_path"
    echo ""
    
    cd "$lambda_path"
    
    # Run pytest
    if [ "$1" == "--cov" ]; then
        if pytest tests/ --cov=. --cov-report=term-missing -v; then
            echo -e "${GREEN}✓ $lambda_name: PASSED${NC}"
            PASSED_LAMBDAS=$((PASSED_LAMBDAS + 1))
        else
            echo -e "${RED}✗ $lambda_name: FAILED${NC}"
            FAILED_LAMBDAS=$((FAILED_LAMBDAS + 1))
            FAILED_LAMBDA_NAMES+=("$lambda_name")
            
            if [ "$2" == "-x" ] || [ "$2" == "--exitfirst" ]; then
                echo -e "${RED}Exiting due to --exitfirst flag${NC}"
                exit 1
            fi
        fi
    else
        if pytest tests/ "$@"; then
            echo -e "${GREEN}✓ $lambda_name: PASSED${NC}"
            PASSED_LAMBDAS=$((PASSED_LAMBDAS + 1))
        else
            echo -e "${RED}✗ $lambda_name: FAILED${NC}"
            FAILED_LAMBDAS=$((FAILED_LAMBDAS + 1))
            FAILED_LAMBDA_NAMES+=("$lambda_name")
            
            if [ "$1" == "-x" ] || [ "$1" == "--exitfirst" ]; then
                echo -e "${RED}Exiting due to --exitfirst flag${NC}"
                exit 1
            fi
        fi
    fi
    
    echo ""
    echo "─────────────────────────────────────────────────────────────"
    echo ""
done

# Print summary
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo "Total Lambda Functions: $TOTAL_LAMBDAS"
echo -e "${GREEN}Passed: $PASSED_LAMBDAS${NC}"
if [ $FAILED_LAMBDAS -gt 0 ]; then
    echo -e "${RED}Failed: $FAILED_LAMBDAS${NC}"
    echo ""
    echo "Failed lambdas:"
    for failed in "${FAILED_LAMBDA_NAMES[@]}"; do
        echo -e "  ${RED}✗ $failed${NC}"
    done
else
    echo -e "${RED}Failed: 0${NC}"
fi
echo ""

# Exit with appropriate code
if [ $FAILED_LAMBDAS -gt 0 ]; then
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed! 🎉${NC}"
    exit 0
fi
