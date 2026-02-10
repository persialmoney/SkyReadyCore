#!/bin/bash
# Build Lambda Layer with dependencies for Linux runtime

echo "Building Lambda Layer..."

# Clean up any existing build
rm -rf python python.zip

# Create python directory (Lambda Layer structure)
mkdir -p python

# Copy shared utilities
cp db_utils.py python/
cp __init__.py python/

# Install dependencies for Lambda Linux environment
pip install -r requirements.txt \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --target python/ \
  --upgrade

echo "✅ Lambda Layer built successfully in python/ directory"
echo "Layer contents:"
ls -lh python/
