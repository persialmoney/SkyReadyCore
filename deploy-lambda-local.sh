#!/bin/bash
# Deploy Lambda directly to AWS without git commit
# Perfect for quick debugging iterations with logging changes
# Usage: ./deploy-lambda-local.sh <lambda-name> [stage] [aws-profile]
# Example: ./deploy-lambda-local.sh sync-pull dev AdministratorAccess-053141520112

set -e

# Check arguments
if [ -z "$1" ]; then
    echo "❌ Error: Lambda name is required"
    echo ""
    echo "Usage: ./deploy-lambda-local.sh <lambda-name> [stage] [aws-profile]"
    echo ""
    echo "Available Lambda functions:"
    echo "  - sync-pull"
    echo "  - sync-push"
    echo "  - weather"
    echo "  - weather-cache-ingest"
    echo "  - user-creation"
    echo "  - user-update"
    echo "  - outbox-processor"
    echo "  - logbook-ai"
    echo ""
    echo "Example: ./deploy-lambda-local.sh sync-pull dev"
    echo "Example: ./deploy-lambda-local.sh sync-pull dev AdministratorAccess-053141520112"
    exit 1
fi

LAMBDA_NAME=$1
STAGE=${2:-dev}
AWS_PROFILE=${3:-AdministratorAccess-053141520112}
FUNCTION_NAME="sky-ready-$LAMBDA_NAME-$STAGE"

echo "🚀 Deploying $LAMBDA_NAME Lambda to AWS"
echo "   Function: $FUNCTION_NAME"
echo "   Stage: $STAGE"
echo "   Profile: $AWS_PROFILE"
echo ""

# Check if lambda directory exists
if [ ! -d "lambdas/$LAMBDA_NAME" ]; then
    echo "❌ Error: lambdas/$LAMBDA_NAME directory not found"
    echo ""
    echo "Available Lambda functions:"
    ls -1 lambdas/
    exit 1
fi

cd "lambdas/$LAMBDA_NAME"

# Create temporary package directory
echo "🧹 Cleaning up previous builds..."
rm -rf .package
mkdir .package

# Install dependencies if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "📦 Installing Lambda dependencies..."
    pip3 install -r requirements.txt \
        --platform manylinux2014_x86_64 \
        --only-binary=:all: \
        --target .package \
        --upgrade 2>&1 | grep -v "Requirement already satisfied" || true
    echo "   ✓ Dependencies installed"
fi

# Install shared dependencies if they exist
if [ -f "../shared/requirements.txt" ]; then
    echo "📦 Installing shared dependencies..."
    pip3 install -r ../shared/requirements.txt \
        --platform manylinux2014_x86_64 \
        --only-binary=:all: \
        --target .package \
        --upgrade 2>&1 | grep -v "Requirement already satisfied" || true
    
    echo "📁 Copying shared utilities..."
    cp -r ../shared .package/
    echo "   ✓ Shared utilities copied"
fi

# Copy Lambda code (all Python files)
echo "📁 Copying Lambda code..."
cp *.py .package/ 2>/dev/null || true
echo "   ✓ Lambda code copied"

# Package
echo "📦 Creating deployment package..."
cd .package
zip -r ../lambda-deploy.zip . -x "*.pyc" "__pycache__/*" "*.git*" "tests/*" "*-test.txt" "README.md" > /dev/null 2>&1
cd ..
PACKAGE_SIZE=$(ls -lh lambda-deploy.zip | awk '{print $5}')
echo "   ✓ Package created ($PACKAGE_SIZE)"

# Deploy
echo "☁️  Deploying to AWS..."
echo "   Function: $FUNCTION_NAME"

if aws lambda update-function-code \
    --profile "$AWS_PROFILE" \
    --function-name "$FUNCTION_NAME" \
    --zip-file fileb://lambda-deploy.zip > /dev/null 2>&1; then
    echo "   ✓ Code uploaded"
    
    # Get function info
    LAST_MODIFIED=$(aws lambda get-function \
        --profile "$AWS_PROFILE" \
        --function-name "$FUNCTION_NAME" \
        --query 'Configuration.LastModified' \
        --output text)
    CODE_SIZE=$(aws lambda get-function \
        --profile "$AWS_PROFILE" \
        --function-name "$FUNCTION_NAME" \
        --query 'Configuration.CodeSize' \
        --output text)
    
    echo ""
    echo "✅ Lambda deployed successfully!"
    echo "   Last Modified: $LAST_MODIFIED"
    echo "   Code Size: $CODE_SIZE bytes"
else
    echo ""
    echo "❌ Deployment failed"
    echo ""
    echo "Possible issues:"
    echo "  - AWS credentials not configured (run: aws configure)"
    echo "  - Lambda function '$FUNCTION_NAME' does not exist"
    echo "  - Insufficient IAM permissions (need lambda:UpdateFunctionCode)"
    exit 1
fi

# Cleanup
echo ""
echo "🧹 Cleaning up..."
rm -rf .package lambda-deploy.zip
echo "   ✓ Temporary files removed"

echo ""
echo "🎉 Done! Your Lambda function is live."
echo ""
echo "To test the deployed function:"
echo "  aws lambda invoke --function-name $FUNCTION_NAME --payload '{}' /tmp/response.json"
echo "  cat /tmp/response.json"
