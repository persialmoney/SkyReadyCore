#!/bin/bash
# Test runner script for TAF parsing tests

set -e

echo "🧪 Running TAF Parsing Tests"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "⚠️  pytest not found. Installing dependencies..."
    pip install -r requirements.txt
fi

# Run tests with verbose output
echo "Running unit tests..."
pytest tests/test_taf_parsing.py -v

echo ""
echo "Running real-world scenario tests..."
pytest tests/test_taf_real_world.py -v

echo ""
echo "✅ All tests completed!"

