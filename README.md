# Plontis Central API

AWS Lambda-based central API for collecting and analyzing AI bot detection data from WordPress sites running the Plontis plugin.

## 🏗️ Architecture

- **AWS Lambda**: Serverless API handlers
- **AWS RDS MySQL**: Database for storing detections and registrations
- **AWS API Gateway**: RESTful API endpoints
- **Serverless Framework**: Infrastructure as code

## 📊 Database Schema

### Tables

- **`detections`**: AI bot detection events from WordPress sites
- **`api_registrations`**: Registered WordPress sites using Plontis plugin

## 🚀 API Endpoints

### Market Intelligence
```
GET /v1/market-intelligence
```
Returns aggregated AI bot detection statistics from all registered sites.

### Site Registration  
```
POST /v1/register
```
Registers a new WordPress site with the central system.

### Site Insights
```
GET /v1/site-insights?site_hash={hash}
```
Returns analytics for a specific registered site.

## 🛠️ Development

### Prerequisites
- AWS CLI configured
- Serverless Framework installed
- Python 3.9+
- MySQL client (optional)

### Environment Variables
```bash
export DB_HOST="plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com"
export DB_USER="plontis"
export DB_PASSWORD="your-password"
export DB_NAME="plontis_central"
```

### Deploy
```bash
# Deploy entire stack
serverless deploy

# Deploy single function
serverless deploy function --function api
```

### Local Testing
```bash
# Test database connection
python3 debug_handler.py

# Test API locally
python3 handler.py
```

## 📈 Database Connection

**Host**: `plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com`  
**Port**: `3306`  
**Database**: `plontis_central`

### Connect via MySQL Client
```bash
mysql -h plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com \
      -P 3306 -u plontis -p plontis_central
```

## 🔍 Monitoring

- **API Gateway**: `https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/`
- **Lambda Logs**: `/aws/lambda/plontis-central-api-prod-api`
- **RDS Monitoring**: AWS Console → RDS → plontis-central

## 📝 Usage Examples

### Get Market Intelligence
```bash
curl https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/v1/market-intelligence
```

### Register a Site
```bash
curl -X POST https://0ak4j2uw02.execute-api.us-east-1.amazonaws.com/prod/v1/register \
  -H 'Content-Type: application/json' \
  -d '{"api_key": "your-key", "site_hash": "your-hash"}'
```

## 🏢 Business Model

This central API enables:
- **Market Intelligence**: Aggregate AI bot activity across all WordPress sites
- **Site Analytics**: Individual site performance and AI bot interactions
- **Licensing Data**: Evidence for content licensing negotiations with AI companies

## 🔧 Maintenance

### View Database Contents
```bash
python3 view_database.py
```

### Backup Database
```bash
# RDS automated backups are enabled
# Manual backup via AWS Console → RDS → Snapshots
```

---

**Built with ❤️ for content creators seeking fair compensation from AI companies.**
