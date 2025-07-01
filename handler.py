import json
import os
import pymysql
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def api_handler(event, context):
    """Main Lambda handler for Plontis Central API"""
    
    try:
        # Log the incoming event for debugging
        logger.info(f"Event received: {json.dumps(event, default=str)}")
        
        # Parse the request
        method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')
        headers = event.get('headers', {})
        body = event.get('body', '{}')
        
        logger.info(f"Processing: {method} {path}")
        
        # Route requests
        if method == 'GET' and path == '/v1/market-intelligence':
            logger.info("Calling get_market_intelligence")
            return get_market_intelligence()
        elif method == 'POST' and path == '/v1/detections':
            return handle_detection_report(body, headers)
        elif method == 'POST' and path == '/v1/register':
            return handle_registration(body, headers)
        elif method == 'GET' and path.startswith('/v1/insights/'):
            site_hash = path.split('/')[-1]
            return get_site_insights(site_hash, headers)
        elif method == 'GET' and path == '/':
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'message': 'Plontis Central API', 
                    'status': 'running', 
                    'path_received': path,
                    'timestamp': datetime.now().isoformat()
                })
            }
        else:
            logger.info(f"No route matched for {method} {path}")
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Endpoint not found', 'path_received': path, 'method': method})
            }
            
    except Exception as e:
        logger.error(f"API Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }

def get_db_connection():
    """Database connection"""
    try:
        host = os.environ.get('DB_HOST')
        user = os.environ.get('DB_USER')
        password = os.environ.get('DB_PASSWORD')
        database = os.environ.get('DB_NAME')
        
        logger.info(f"Connecting to database: {host} as {user}")
        
        if not all([host, user, password, database]):
            raise Exception("Missing database environment variables")
        
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            autocommit=True,
            connect_timeout=10
        )
        
        logger.info("Database connection successful")
        return connection
        
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return None

def get_market_intelligence():
    """Public market intelligence endpoint"""
    
    try:
        logger.info("Starting market intelligence function")
        
        # Try to get live data from database
        connection = get_db_connection()
        
        if connection:
            try:
                with connection.cursor() as cursor:
                    # Get recent stats
                    sql = """
                        SELECT company, COUNT(*) as detections, 
                               AVG(estimated_value) as avg_value, SUM(estimated_value) as total_value
                        FROM detections 
                        WHERE detected_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                        GROUP BY company
                        ORDER BY total_value DESC
                        LIMIT 5
                    """
                    
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    
                    logger.info(f"Database query returned {len(results)} rows")
                    
                    if results:
                        # Process real data
                        top_companies = []
                        total_detections = 0
                        total_value = 0
                        
                        for row in results:
                            company_data = {
                                'company': row[0],
                                'detections': int(row[1]),
                                'total_value': float(row[3])
                            }
                            top_companies.append(company_data)
                            total_detections += int(row[1])
                            total_value += float(row[3])
                        
                        avg_value = total_value / total_detections if total_detections > 0 else 0
                        
                        stats = {
                            'total_detections_24h': total_detections,
                            'top_companies': top_companies,
                            'average_content_value': round(avg_value, 2),
                            'last_updated': datetime.now().isoformat(),
                            'status': 'live_data'
                        }
                    else:
                        # No data in database, return sample data
                        stats = get_sample_data('no_database_data')
                        
                connection.close()
                
            except Exception as db_error:
                logger.error(f"Database query error: {str(db_error)}")
                stats = get_sample_data(f'database_query_error')
                if connection:
                    connection.close()
        else:
            # No database connection, return sample data
            stats = get_sample_data('no_database_connection')
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(stats)
        }
        
    except Exception as e:
        logger.error(f"Market intelligence error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Market intelligence error: {str(e)}'})
        }

def get_sample_data(reason='fallback'):
    """Return sample data"""
    return {
        'total_detections_24h': 15,
        'top_companies': [
            {'company': 'OpenAI', 'detections': 6, 'total_value': 142.50},
            {'company': 'Anthropic', 'detections': 5, 'total_value': 118.75},
            {'company': 'Google', 'detections': 4, 'total_value': 89.25}
        ],
        'average_content_value': 23.37,
        'last_updated': datetime.now().isoformat(),
        'status': f'sample_data_{reason}'
    }

def handle_detection_report(body, headers):
    """Handle detection data from WordPress sites"""
    
    try:
        # Validate API key
        api_key = headers.get('x-api-key', '')
        if not validate_api_key(api_key):
            return {
                'statusCode': 401,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Invalid API key'})
            }
        
        # Parse detection data
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Invalid JSON'})
            }
        
        # Validate required fields
        required_fields = ['site_hash', 'company', 'content_type', 'estimated_value', 'detected_at']
        for field in required_fields:
            if field not in data:
                return {
                    'statusCode': 400,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': f'Missing required field: {field}'})
                }
        
        # Store detection
        detection_id = store_detection(data)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'success': True,
                'detection_id': detection_id
            })
        }
        
    except Exception as e:
        logger.error(f"Detection report error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Failed to process detection report'})
        }

def store_detection(data):
    """Store detection in database"""
    
    connection = get_db_connection()
    if not connection:
        logger.error("Cannot store detection - no database connection")
        return None
    
    try:
        with connection.cursor() as cursor:
            sql = """INSERT INTO detections (
                site_hash, site_category, site_region,
                company, bot_type, content_type, content_quality,
                estimated_value, risk_level, commercial_risk,
                detected_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"""
            
            cursor.execute(sql, (
                data['site_hash'],
                data.get('site_category', 'blog'),
                data.get('site_region', 'US'),
                data['company'],
                data.get('bot_type', 'Unknown'),
                data['content_type'],
                data.get('content_quality', 50),
                data['estimated_value'],
                data.get('risk_level', 'medium'),
                data.get('commercial_risk', False),
                data['detected_at']
            ))
            
            detection_id = cursor.lastrowid
            logger.info(f"Stored detection with ID: {detection_id}")
            
        return detection_id
        
    except Exception as e:
        logger.error(f"Error storing detection: {str(e)}")
        return None
    finally:
        connection.close()

def handle_registration(body, headers):
    """Handle new site registration"""
    
    try:
        data = json.loads(body)
        api_key = data['api_key']
        site_hash = data['site_hash']
        
        # Store API key registration
        connection = get_db_connection()
        if not connection:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Database unavailable'})
            }
        
        with connection.cursor() as cursor:
            sql = """INSERT INTO api_registrations (
                api_key, site_hash, site_url_hash, 
                wordpress_version, plugin_version, registered_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            last_seen = NOW(), plugin_version = %s"""
            
            cursor.execute(sql, (
                api_key,
                site_hash,
                data.get('site_url_hash', ''),
                data.get('wordpress_version', ''),
                data.get('plugin_version', ''),
                data.get('registered_at', datetime.now()),
                data.get('plugin_version', '')
            ))
        
        connection.close()
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'success': True, 'registered': True})
        }
        
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Registration failed'})
        }

def get_site_insights(site_hash, headers):
    """Get insights for a specific site"""
    
    # Validate API key
    api_key = headers.get('x-api-key', '')
    if not validate_api_key(api_key):
        return {
            'statusCode': 401,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Invalid API key'})
        }
    
    connection = get_db_connection()
    if not connection:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Database unavailable'})
        }
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT company, COUNT(*) as detections, AVG(estimated_value) as avg_value, SUM(estimated_value) as total_value
                FROM detections 
                WHERE site_hash = %s 
                AND detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY company
                ORDER BY total_value DESC
            """, (site_hash,))
            
            results = cursor.fetchall()
            
        insights = {
            'site_hash': site_hash,
            'period': '30_days',
            'company_breakdown': [
                {
                    'company': row[0], 
                    'detections': row[1], 
                    'avg_value': float(row[2]), 
                    'total_value': float(row[3])
                }
                for row in results
            ]
        }
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(insights)
        }
        
    except Exception as e:
        logger.error(f"Site insights error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Failed to get site insights'})
        }
        
    finally:
        connection.close()

def validate_api_key(api_key):
    """Basic API key validation"""
    if not api_key or len(api_key) < 10:
        return False
    return True