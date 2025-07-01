#!/bin/bash

echo "🔧 Fixing Lambda Deployment for Plontis"
echo "========================================"

# Step 1: Verify Lambda environment variables
echo "📋 Step 1: Checking Lambda environment variables..."
aws lambda get-function-configuration \
    --function-name plontis-central-api-prod-api \
    --query 'Environment.Variables' \
    --output table

echo ""

# Step 2: Force update environment variables (in case they didn't stick)
echo "🔧 Step 2: Force updating environment variables..."

cat > /tmp/lambda-env.json << 'EOF'
{
  "Variables": {
    "DB_HOST": "plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com",
    "DB_USER": "plontis",
    "DB_PASSWORD": "Andyandy19",
    "DB_NAME": "plontis_central"
  }
}
EOF

aws lambda update-function-configuration \
    --function-name plontis-central-api-prod-api \
    --environment file:///tmp/lambda-env.json

echo "✅ Environment variables updated"

# Step 3: Clean redeploy
echo ""
echo "🚀 Step 3: Clean redeployment..."

# First, try a function-only deploy
echo "📦 Deploying function code..."
serverless deploy function --function api --force

# Wait for deployment
echo "⏳ Waiting for deployment to complete..."
sleep 15

# Step 4: Test the API
echo ""
echo "🧪 Step 4: Testing API..."

echo "🌐 API Test 1:"
API_RESPONSE=$(curl -s https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/v1/market-intelligence)
echo "$API_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$API_RESPONSE"

# Check if still failing
if echo "$API_RESPONSE" | grep -q "sample_data"; then
    echo ""
    echo "⚠️  Still returning sample data. Trying full redeploy..."
    
    # Step 5: Full redeploy if function deploy didn't work
    echo "🚀 Step 5: Full service redeploy..."
    serverless deploy --force
    
    echo "⏳ Waiting for full deployment..."
    sleep 20
    
    echo ""
    echo "🧪 API Test 2 (after full redeploy):"
    API_RESPONSE2=$(curl -s https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/v1/market-intelligence)
    echo "$API_RESPONSE2" | python3 -m json.tool 2>/dev/null || echo "$API_RESPONSE2"
    
    # If still failing, check logs
    if echo "$API_RESPONSE2" | grep -q "sample_data"; then
        echo ""
        echo "🔍 Still having issues. Let's check Lambda logs..."
        echo "📋 Recent Lambda logs:"
        aws logs filter-log-events \
            --log-group-name "/aws/lambda/plontis-central-api-prod-api" \
            --start-time $(date -d '5 minutes ago' +%s)000 \
            --query 'events[].message' \
            --output text
    else
        echo ""
        echo "🎉 SUCCESS! API is now returning live data!"
    fi
else
    echo ""
    echo "🎉 SUCCESS! API is now returning live data!"
fi

# Step 6: Verify final status
echo ""
echo "📊 FINAL VERIFICATION:"
echo "====================="

FINAL_RESPONSE=$(curl -s https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/v1/market-intelligence)
STATUS=$(echo "$FINAL_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('status', 'unknown'))" 2>/dev/null || echo "unknown")

echo "🎯 Final API Status: $STATUS"

if [ "$STATUS" = "live" ]; then
    echo "✅ SUCCESS! Your Plontis API is now connected to live database!"
    echo ""
    echo "📊 Live Data Summary:"
    echo "$FINAL_RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f'   Total Detections (24h): {data.get(\"total_detections_24h\", 0)}')
    print(f'   Average Content Value: \${data.get(\"average_content_value\", 0):.2f}')
    print(f'   Top Companies: {len(data.get(\"top_companies\", []))}')
    for i, company in enumerate(data.get('top_companies', [])[:3]):
        print(f'     {i+1}. {company[\"company\"]}: {company[\"detections\"]} detections, \${company[\"total_value\"]:.2f}')
except:
    pass
"
else
    echo "❌ API is still returning status: $STATUS"
    echo "🔍 Check the logs above for more details"
fi

# Clean up
rm -f /tmp/lambda-env.json

echo ""
echo "✅ Deployment process complete!"