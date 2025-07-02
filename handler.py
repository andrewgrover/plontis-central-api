import json
import os
import pymysql
from datetime import datetime
import logging
import sys

# Fix logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def fix_site_hash_in_lambda():
    """Fix site_hash values directly from Lambda"""
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        with connection.cursor() as cursor:
            # Check current state
            cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NULL OR site_hash = ''")
            null_count = cursor.fetchone()[0]
            print(f"Lambda sees {null_count} records with NULL/empty site_hash")
            
            if null_count > 0:
                # Fix them directly from Lambda
                correct_site_hash = "f51bc27669e6f0c9cd71fe4dae0c03f44d461738e7e45b615188398def349678"
                
                print(f"Fixing {null_count} records with site_hash: {correct_site_hash}")
                
                cursor.execute("""
                    UPDATE detections 
                    SET site_hash = %s 
                    WHERE site_hash IS NULL OR site_hash = ''
                """, (correct_site_hash,))
                
                updated_rows = cursor.rowcount
                print(f"Lambda updated {updated_rows} records")
                
                # Verify the fix
                cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NOT NULL AND site_hash != ''")
                valid_count = cursor.fetchone()[0]
                print(f"After fix: {valid_count} records have valid site_hash")
                
                return True
            else:
                print("Lambda sees no NULL site_hash values")
                return True
                
    except Exception as e:
        print(f"Lambda fix error: {str(e)}")
        return False
    finally:
        connection.close()

def api_handler(event, context):
    """Main Lambda handler for Plontis Central API"""
    
    try:
        # Log the incoming event for debugging
        print(f"Event received: {json.dumps(event, default=str)}")
        logger.info(f"Event received: {json.dumps(event, default=str)}")
        
        # Parse the request
        method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')
        headers = event.get('headers', {})
        body = event.get('body', '{}')
        
        print(f"Processing: {method} {path}")
        logger.info(f"Processing: {method} {path}")
        
        # Route requests
        if method == 'GET' and path == '/v1/market-intelligence':
            print("Calling get_market_intelligence")
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
            print(f"No route matched for {method} {path}")
            logger.info(f"No route matched for {method} {path}")
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Endpoint not found', 'path_received': path, 'method': method})
            }
            
    except Exception as e:
        error_msg = f"API Error: {str(e)}"
        print(error_msg)
        logger.error(error_msg, exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }

def get_db_connection():
    """Database connection with forced fresh connection"""
    try:
        host = os.environ.get('DB_HOST')
        user = os.environ.get('DB_USER')
        password = os.environ.get('DB_PASSWORD')
        database = os.environ.get('DB_NAME')
        
        print(f"Attempting database connection to: {host} as {user} to database {database}")
        logger.info(f"Connecting to database: {host} as {user}")
        
        if not all([host, user, password, database]):
            missing = [k for k, v in {'DB_HOST': host, 'DB_USER': user, 'DB_PASSWORD': password, 'DB_NAME': database}.items() if not v]
            error_msg = f"Missing database environment variables: {missing}"
            print(error_msg)
            raise Exception(error_msg)
        
        # Force a completely fresh connection every time
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            autocommit=True,
            connect_timeout=10,
            read_timeout=10,
            write_timeout=10,
            # Force fresh connection - no connection reuse
            use_unicode=True,
            sql_mode='TRADITIONAL'
        )
        
        print("Database connection successful")
        logger.info("Database connection successful")
        
        # Force a test query to ensure we have the latest data
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            print(f"DEBUG: Records with non-NULL site_hash: {non_null_count}")
            
            cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NULL")
            null_count = cursor.fetchone()[0]
            print(f"DEBUG: Records with NULL site_hash: {null_count}")
        
        return connection
        
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        print(error_msg)
        logger.error(error_msg)
        return None

def get_market_intelligence():
    """Public market intelligence endpoint with auto-fix"""
    
    try:
        print("=== Starting market intelligence function ===")
        logger.info("Starting market intelligence function")
        
        # Get a fresh database connection
        connection = get_db_connection()
        
        if connection:
            try:
                with connection.cursor() as cursor:
                    # Check table exists
                    print("Checking if detections table exists...")
                    cursor.execute("SHOW TABLES LIKE 'detections'")
                    table_exists = cursor.fetchone()
                    
                    if not table_exists:
                        print("ERROR: Detections table does not exist!")
                        stats = get_fallback_stats('no_detections_table')
                    else:
                        print("Detections table exists, checking data...")
                        
                        # Get total count
                        cursor.execute("SELECT COUNT(*) FROM detections")
                        total_count = cursor.fetchone()[0]
                        print(f"Total records in detections table: {total_count}")
                        
                        # Check site_hash status
                        cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NOT NULL AND site_hash != ''")
                        valid_hash_count = cursor.fetchone()[0]
                        print(f"Records with valid site_hash: {valid_hash_count}")
                        
                        cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NULL OR site_hash = ''")
                        invalid_hash_count = cursor.fetchone()[0]
                        print(f"Records with invalid site_hash: {invalid_hash_count}")
                        
                        # If no valid site_hash values, try to fix them
                        if valid_hash_count == 0 and invalid_hash_count > 0:
                            print("ERROR: No records have valid site_hash values!")
                            print("Attempting to fix site_hash values from Lambda...")
                            
                            # Close current connection and fix
                            connection.close()
                            
                            # Try to fix the data
                            fix_success = fix_site_hash_in_lambda()
                            
                            if fix_success:
                                print("Fix applied, retrying query...")
                                # Get a new connection and try again
                                connection = get_db_connection()
                                if connection:
                                    with connection.cursor() as new_cursor:
                                        # Check again after fix
                                        new_cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NOT NULL AND site_hash != ''")
                                        fixed_count = new_cursor.fetchone()[0]
                                        print(f"After Lambda fix: {fixed_count} records have valid site_hash")
                                        
                                        if fixed_count > 0:
                                            # Now get real statistics
                                            print("Getting statistics with fixed data...")
                                            new_cursor.execute("""
                                                SELECT 
                                                    COUNT(DISTINCT site_hash) as total_sites,
                                                    COUNT(*) as total_detections,
                                                    AVG(estimated_value) as avg_value_per_detection
                                                FROM detections 
                                                WHERE site_hash IS NOT NULL 
                                                AND site_hash != ''
                                                AND detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
                                            """)
                                            
                                            result = new_cursor.fetchone()
                                            print(f"Statistics query result after fix: {result}")
                                            
                                            # Get most active company
                                            new_cursor.execute("""
                                                SELECT company, COUNT(*) as count
                                                FROM detections 
                                                WHERE site_hash IS NOT NULL 
                                                AND site_hash != ''
                                                AND detected_at > DATE_SUB(NOW(), INTERVAL 7 DAY) 
                                                AND company IS NOT NULL
                                                GROUP BY company 
                                                ORDER BY count DESC 
                                                LIMIT 1
                                            """)
                                            company_result = new_cursor.fetchone()
                                            most_active = company_result[0] if company_result else 'N/A'
                                            print(f"Most active company: {most_active}")
                                            
                                            stats = {
                                                'total_sites': int(result[0] or 0),
                                                'total_detections': int(result[1] or 0),
                                                'average_value_per_detection': round(float(result[2] or 0), 2),
                                                'most_active_company': most_active,
                                                'status': 'live_data_fixed',
                                                'last_updated': datetime.now().isoformat()
                                            }
                                        else:
                                            stats = get_fallback_stats('fix_failed')
                                else:
                                    stats = get_fallback_stats('reconnection_failed')
                            else:
                                stats = get_fallback_stats('fix_failed')
                        else:
                            # We have valid data, proceed normally
                            print("Getting detection statistics...")
                            cursor.execute("""
                                SELECT 
                                    COUNT(DISTINCT site_hash) as total_sites,
                                    COUNT(*) as total_detections,
                                    AVG(estimated_value) as avg_value_per_detection
                                FROM detections 
                                WHERE site_hash IS NOT NULL 
                                AND site_hash != ''
                                AND detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
                            """)
                            
                            result = cursor.fetchone()
                            print(f"Statistics query result: {result}")
                            
                            # Get most active company
                            cursor.execute("""
                                SELECT company, COUNT(*) as count
                                FROM detections 
                                WHERE site_hash IS NOT NULL 
                                AND site_hash != ''
                                AND detected_at > DATE_SUB(NOW(), INTERVAL 7 DAY) 
                                AND company IS NOT NULL
                                GROUP BY company 
                                ORDER BY count DESC 
                                LIMIT 1
                            """)
                            company_result = cursor.fetchone()
                            most_active = company_result[0] if company_result else 'N/A'
                            print(f"Most active company: {most_active}")
                            
                            stats = {
                                'total_sites': int(result[0] or 0),
                                'total_detections': int(result[1] or 0),
                                'average_value_per_detection': round(float(result[2] or 0), 2),
                                'most_active_company': most_active,
                                'status': 'live_data',
                                'last_updated': datetime.now().isoformat()
                            }
                
                if connection:
                    connection.close()
                
            except Exception as db_error:
                error_msg = f"Database operation error: {str(db_error)}"
                print(error_msg)
                logger.error(error_msg, exc_info=True)
                stats = get_fallback_stats('database_operation_error')
                if connection:
                    connection.close()
        else:
            print("No database connection available")
            stats = get_fallback_stats('no_database_connection')
        
        print(f"Final stats being returned: {stats}")
        logger.info(f"Final stats being returned: {stats}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(stats)
        }
        
    except Exception as e:
        error_msg = f"Market intelligence error: {str(e)}"
        print(error_msg)
        logger.error(error_msg, exc_info=True)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': error_msg})
        }

def get_fallback_stats(reason='fallback'):
    """Return fallback stats when no real data is available"""
    stats = {
        'total_sites': 1,
        'total_detections': 0,
        'average_value_per_detection': 0.00,
        'most_active_company': 'N/A',
        'status': f'fallback_{reason}',
        'last_updated': datetime.now().isoformat()
    }
    print(f"Returning fallback stats: {stats}")
    return stats

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