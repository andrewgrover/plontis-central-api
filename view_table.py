#!/usr/bin/env python3
"""
Plontis Central API - In-Depth Database Table Viewer
====================================================

Comprehensive database inspection tool for viewing all tables,
their structure, contents, and analytics.

Usage:
    python3 view_tables.py
    python3 view_tables.py --table detections
    python3 view_tables.py --analytics
    python3 view_tables.py --export csv
"""

import pymysql
import os
import sys
import json
import csv
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import argparse

class PlontisDBViewer:
    def __init__(self):
        self.connection = None
        self.connect_to_database()
    
    def connect_to_database(self):
        """Establish connection to the database"""
        try:
            self.connection = pymysql.connect(
                host=os.environ.get('DB_HOST', 'plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com'),
                user=os.environ.get('DB_USER', 'plontis'),
                password=os.environ.get('DB_PASSWORD', 'Andyandy19'),
                database=os.environ.get('DB_NAME', 'plontis_central'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("✅ Connected to Plontis Central Database")
            print(f"📍 Host: {os.environ.get('DB_HOST', 'localhost')}")
            print(f"🗄️  Database: {os.environ.get('DB_NAME', 'plontis_central')}")
            print("=" * 60)
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def get_database_info(self):
        """Get basic database information"""
        with self.connection.cursor() as cursor:
            # MySQL version
            cursor.execute("SELECT VERSION() as version")
            version = cursor.fetchone()['version']
            
            # Database size
            cursor.execute("""
                SELECT 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'size_mb'
                FROM information_schema.tables 
                WHERE table_schema = %s
            """, (os.environ.get('DB_NAME', 'plontis_central'),))
            size = cursor.fetchone()['size_mb'] or 0
            
            # Table count
            cursor.execute("SHOW TABLES")
            table_count = len(cursor.fetchall())
            
            return {
                'version': version,
                'size_mb': size,
                'table_count': table_count
            }
    
    def get_all_tables(self):
        """Get list of all tables with basic info"""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    table_name,
                    table_rows,
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'size_mb',
                    table_comment
                FROM information_schema.tables 
                WHERE table_schema = %s 
                ORDER BY table_rows DESC
            """, (os.environ.get('DB_NAME', 'plontis_central'),))
            return cursor.fetchall()
    
    def get_table_structure(self, table_name):
        """Get detailed table structure"""
        with self.connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            # Get indexes
            cursor.execute(f"SHOW INDEX FROM {table_name}")
            indexes = cursor.fetchall()
            
            # Get foreign keys
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    REFERENCED_TABLE_NAME,
                    REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = %s 
                AND TABLE_NAME = %s 
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (os.environ.get('DB_NAME', 'plontis_central'), table_name))
            foreign_keys = cursor.fetchall()
            
            return {
                'columns': columns,
                'indexes': indexes,
                'foreign_keys': foreign_keys
            }
    
    def get_table_data(self, table_name, limit=50, order_by=None):
        """Get table data with optional ordering"""
        with self.connection.cursor() as cursor:
            # Get total count
            cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
            total_count = cursor.fetchone()['total']
            
            # Build query
            query = f"SELECT * FROM {table_name}"
            if order_by:
                query += f" ORDER BY {order_by} DESC"
            elif table_name == 'detections':
                query += " ORDER BY detected_at DESC"
            elif table_name == 'api_registrations':
                query += " ORDER BY registered_at DESC"
            else:
                query += " ORDER BY id DESC"
            
            query += f" LIMIT {limit}"
            
            cursor.execute(query)
            data = cursor.fetchall()
            
            return {
                'total_count': total_count,
                'data': data,
                'showing': len(data)
            }
    
    def analyze_detections_table(self):
        """Deep analysis of detections table"""
        with self.connection.cursor() as cursor:
            analysis = {}
            
            # Time-based analysis
            cursor.execute("""
                SELECT 
                    DATE(detected_at) as date,
                    COUNT(*) as count,
                    AVG(estimated_value) as avg_value,
                    SUM(estimated_value) as total_value
                FROM detections 
                WHERE detected_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY DATE(detected_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            analysis['daily_stats'] = cursor.fetchall()
            
            # Company analysis
            cursor.execute("""
                SELECT 
                    company,
                    COUNT(*) as detections,
                    AVG(estimated_value) as avg_value,
                    SUM(estimated_value) as total_value,
                    MIN(detected_at) as first_seen,
                    MAX(detected_at) as last_seen
                FROM detections 
                GROUP BY company 
                ORDER BY total_value DESC
            """)
            analysis['company_stats'] = cursor.fetchall()
            
            # Content type analysis
            cursor.execute("""
                SELECT 
                    content_type,
                    COUNT(*) as count,
                    AVG(estimated_value) as avg_value,
                    SUM(estimated_value) as total_value
                FROM detections 
                GROUP BY content_type 
                ORDER BY count DESC
            """)
            analysis['content_type_stats'] = cursor.fetchall()
            
            # Risk level analysis
            cursor.execute("""
                SELECT 
                    risk_level,
                    COUNT(*) as count,
                    AVG(estimated_value) as avg_value,
                    AVG(confidence) as avg_confidence
                FROM detections 
                GROUP BY risk_level 
                ORDER BY count DESC
            """)
            analysis['risk_level_stats'] = cursor.fetchall()
            
            # IP address analysis (top IPs)
            cursor.execute("""
                SELECT 
                    ip_address,
                    COUNT(*) as count,
                    GROUP_CONCAT(DISTINCT company) as companies,
                    SUM(estimated_value) as total_value
                FROM detections 
                GROUP BY ip_address 
                HAVING count > 1
                ORDER BY count DESC 
                LIMIT 20
            """)
            analysis['ip_stats'] = cursor.fetchall()
            
            # Value distribution
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN estimated_value = 0 THEN '$0'
                        WHEN estimated_value < 1 THEN '$0.01-$0.99'
                        WHEN estimated_value < 5 THEN '$1.00-$4.99'
                        WHEN estimated_value < 10 THEN '$5.00-$9.99'
                        WHEN estimated_value < 25 THEN '$10.00-$24.99'
                        WHEN estimated_value < 50 THEN '$25.00-$49.99'
                        WHEN estimated_value < 100 THEN '$50.00-$99.99'
                        ELSE '$100+'
                    END as value_range,
                    COUNT(*) as count
                FROM detections 
                GROUP BY value_range 
                ORDER BY AVG(estimated_value)
            """)
            analysis['value_distribution'] = cursor.fetchall()
            
            # Recent activity (last 24h)
            cursor.execute("""
                SELECT 
                    HOUR(detected_at) as hour,
                    COUNT(*) as count,
                    AVG(estimated_value) as avg_value
                FROM detections 
                WHERE detected_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                GROUP BY HOUR(detected_at)
                ORDER BY hour
            """)
            analysis['hourly_activity'] = cursor.fetchall()
            
            return analysis
    
    def analyze_registrations_table(self):
        """Deep analysis of api_registrations table"""
        with self.connection.cursor() as cursor:
            analysis = {}
            
            # Registration timeline
            cursor.execute("""
                SELECT 
                    DATE(registered_at) as date,
                    COUNT(*) as new_registrations
                FROM api_registrations 
                GROUP BY DATE(registered_at)
                ORDER BY date DESC
                LIMIT 30
            """)
            analysis['registration_timeline'] = cursor.fetchall()
            
            # WordPress version distribution
            cursor.execute("""
                SELECT 
                    wordpress_version,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM api_registrations), 2) as percentage
                FROM api_registrations 
                WHERE wordpress_version IS NOT NULL AND wordpress_version != ''
                GROUP BY wordpress_version 
                ORDER BY count DESC
            """)
            analysis['wordpress_versions'] = cursor.fetchall()
            
            # Plugin version distribution
            cursor.execute("""
                SELECT 
                    plugin_version,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM api_registrations), 2) as percentage
                FROM api_registrations 
                WHERE plugin_version IS NOT NULL AND plugin_version != ''
                GROUP BY plugin_version 
                ORDER BY count DESC
            """)
            analysis['plugin_versions'] = cursor.fetchall()
            
            # Activity analysis (sites with detections)
            cursor.execute("""
                SELECT 
                    'Active Sites' as metric,
                    COUNT(DISTINCT d.ip_address) as count
                FROM api_registrations r
                LEFT JOIN detections d ON r.site_hash = SHA2(d.ip_address, 256)
                WHERE d.id IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    'Inactive Sites' as metric,
                    COUNT(*) as count
                FROM api_registrations r
                LEFT JOIN detections d ON r.site_hash = SHA2(d.ip_address, 256)
                WHERE d.id IS NULL
            """)
            analysis['site_activity'] = cursor.fetchall()
            
            return analysis
    
    def print_table_overview(self):
        """Print overview of all tables"""
        print("📊 DATABASE OVERVIEW")
        print("=" * 60)
        
        db_info = self.get_database_info()
        print(f"🗄️  MySQL Version: {db_info['version']}")
        print(f"📏 Database Size: {db_info['size_mb']} MB")
        print(f"📋 Total Tables: {db_info['table_count']}")
        print()
        
        tables = self.get_all_tables()
        print("📋 TABLES SUMMARY")
        print("-" * 60)
        print(f"{'Table Name':<20} {'Rows':<10} {'Size (MB)':<10} {'Comment':<20}")
        print("-" * 60)
        
        for table in tables:
            name = table['table_name']
            rows = f"{table['table_rows']:,}" if table['table_rows'] else "0"
            size = f"{table['size_mb']:.2f}" if table['size_mb'] else "0.00"
            comment = table['table_comment'] or "No comment"
            print(f"{name:<20} {rows:<10} {size:<10} {comment:<20}")
        print()
    
    def print_table_details(self, table_name):
        """Print detailed information about a specific table"""
        print(f"🔍 DETAILED VIEW: {table_name.upper()}")
        print("=" * 60)
        
        # Table structure
        structure = self.get_table_structure(table_name)
        print("📋 TABLE STRUCTURE")
        print("-" * 60)
        print(f"{'Column':<20} {'Type':<20} {'Null':<6} {'Key':<6} {'Default':<15} {'Extra':<15}")
        print("-" * 60)
        
        for col in structure['columns']:
            name = col['Field']
            type_info = col['Type']
            null = col['Null']
            key = col['Key']
            default = str(col['Default']) if col['Default'] is not None else 'NULL'
            extra = col['Extra']
            print(f"{name:<20} {type_info:<20} {null:<6} {key:<6} {default:<15} {extra:<15}")
        
        # Indexes
        if structure['indexes']:
            print("\n🔑 INDEXES")
            print("-" * 40)
            index_info = {}
            for idx in structure['indexes']:
                key_name = idx['Key_name']
                if key_name not in index_info:
                    index_info[key_name] = {
                        'unique': not idx['Non_unique'],
                        'columns': []
                    }
                index_info[key_name]['columns'].append(idx['Column_name'])
            
            for idx_name, info in index_info.items():
                unique = "UNIQUE" if info['unique'] else "INDEX"
                columns = ", ".join(info['columns'])
                print(f"{unique:<8} {idx_name:<15} ({columns})")
        
        # Foreign keys
        if structure['foreign_keys']:
            print("\n🔗 FOREIGN KEYS")
            print("-" * 40)
            for fk in structure['foreign_keys']:
                print(f"{fk['COLUMN_NAME']} -> {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}")
        
        print()
        
        # Data sample
        data_info = self.get_table_data(table_name, limit=10)
        print(f"📄 DATA SAMPLE (showing {data_info['showing']} of {data_info['total_count']:,} total rows)")
        print("-" * 60)
        
        if data_info['data']:
            # Print headers
            headers = list(data_info['data'][0].keys())
            header_row = " | ".join(f"{h:<15}" for h in headers[:6])  # Limit to first 6 columns
            print(header_row)
            print("-" * len(header_row))
            
            # Print data rows
            for row in data_info['data'][:5]:  # Show first 5 rows
                values = []
                for i, (key, value) in enumerate(row.items()):
                    if i >= 6:  # Limit to first 6 columns
                        break
                    if value is None:
                        values.append("NULL")
                    elif isinstance(value, str) and len(value) > 15:
                        values.append(value[:12] + "...")
                    else:
                        values.append(str(value))
                
                row_str = " | ".join(f"{v:<15}" for v in values)
                print(row_str)
        
        print()
    
    def print_analytics(self):
        """Print detailed analytics for all tables"""
        print("📈 DETAILED ANALYTICS")
        print("=" * 60)
        
        # Detections analytics
        print("🤖 DETECTIONS TABLE ANALYTICS")
        print("-" * 40)
        
        det_analysis = self.analyze_detections_table()
        
        # Company stats
        print("\n🏢 Top Companies by Value:")
        for company in det_analysis['company_stats'][:10]:
            name = company['company']
            detections = company['detections']
            total_value = company['total_value']
            avg_value = company['avg_value']
            print(f"  {name:<15} {detections:>6} detections  ${total_value:>8.2f} total  ${avg_value:>6.2f} avg")
        
        # Daily activity (last 7 days)
        print("\n📅 Daily Activity (Last 7 Days):")
        for day in det_analysis['daily_stats'][:7]:
            date = day['date']
            count = day['count']
            total_value = day['total_value']
            avg_value = day['avg_value']
            print(f"  {date}  {count:>4} detections  ${total_value:>8.2f} total  ${avg_value:>6.2f} avg")
        
        # Content types
        print("\n📄 Content Type Distribution:")
        for content in det_analysis['content_type_stats']:
            type_name = content['content_type']
            count = content['count']
            total_value = content['total_value']
            print(f"  {type_name:<15} {count:>6} detections  ${total_value:>8.2f} total")
        
        # Risk levels
        print("\n⚠️  Risk Level Distribution:")
        for risk in det_analysis['risk_level_stats']:
            level = risk['risk_level']
            count = risk['count']
            avg_confidence = risk['avg_confidence']
            print(f"  {level:<10} {count:>6} detections  {avg_confidence:>6.1f}% avg confidence")
        
        # Value distribution
        print("\n💰 Value Distribution:")
        for value_range in det_analysis['value_distribution']:
            range_name = value_range['value_range']
            count = value_range['count']
            print(f"  {range_name:<15} {count:>6} detections")
        
        # Top IPs (if any suspicious activity)
        if det_analysis['ip_stats']:
            print("\n🌐 Top IP Addresses:")
            for ip in det_analysis['ip_stats'][:10]:
                ip_addr = ip['ip_address'] or 'Unknown'
                count = ip['count']
                companies = (ip['companies'] or 'Unknown')[:50]  # Truncate if too long
                total_value = ip['total_value'] or 0.0
                print(f"  {ip_addr:<15} {count:>4} detections  ${total_value:>8.2f}  [{companies}]")
        
        # Registrations analytics
        print("\n📝 API REGISTRATIONS ANALYTICS")
        print("-" * 40)
        
        reg_analysis = self.analyze_registrations_table()
        
        # WordPress versions
        print("\n🔧 WordPress Version Distribution:")
        for wp in reg_analysis['wordpress_versions']:
            version = wp['wordpress_version']
            count = wp['count']
            percentage = wp['percentage']
            print(f"  {version:<10} {count:>4} sites ({percentage:>5.1f}%)")
        
        # Plugin versions
        print("\n🔌 Plugin Version Distribution:")
        for plugin in reg_analysis['plugin_versions']:
            version = plugin['plugin_version']
            count = plugin['count']
            percentage = plugin['percentage']
            print(f"  {version:<10} {count:>4} sites ({percentage:>5.1f}%)")
        
        # Site activity
        print("\n📊 Site Activity Status:")
        for activity in reg_analysis['site_activity']:
            metric = activity['metric']
            count = activity['count']
            print(f"  {metric:<15} {count:>4} sites")
        
        print()
    
    def export_data(self, format_type='csv', table_name=None):
        """Export data in various formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if table_name:
            tables_to_export = [table_name]
        else:
            tables = self.get_all_tables()
            tables_to_export = [t['table_name'] for t in tables]
        
        for table in tables_to_export:
            filename = f"plontis_{table}_{timestamp}.{format_type}"
            
            if format_type == 'csv':
                self.export_csv(table, filename)
            elif format_type == 'json':
                self.export_json(table, filename)
            
            print(f"✅ Exported {table} to {filename}")
    
    def export_csv(self, table_name, filename):
        """Export table to CSV"""
        data_info = self.get_table_data(table_name, limit=10000)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if data_info['data']:
                fieldnames = data_info['data'][0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in data_info['data']:
                    # Handle datetime objects
                    processed_row = {}
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            processed_row[key] = value.isoformat()
                        else:
                            processed_row[key] = value
                    writer.writerow(processed_row)
    
    def export_json(self, table_name, filename):
        """Export table to JSON"""
        data_info = self.get_table_data(table_name, limit=10000)
        
        # Handle datetime objects
        processed_data = []
        for row in data_info['data']:
            processed_row = {}
            for key, value in row.items():
                if isinstance(value, datetime):
                    processed_row[key] = value.isoformat()
                else:
                    processed_row[key] = value
            processed_data.append(processed_row)
        
        export_data = {
            'table_name': table_name,
            'total_rows': data_info['total_count'],
            'exported_rows': len(processed_data),
            'export_timestamp': datetime.now().isoformat(),
            'data': processed_data
        }
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(export_data, jsonfile, indent=2, default=str)
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Plontis Central API Database Viewer')
    parser.add_argument('--table', help='Show details for specific table')
    parser.add_argument('--analytics', action='store_true', help='Show detailed analytics')
    parser.add_argument('--export', choices=['csv', 'json'], help='Export data in specified format')
    parser.add_argument('--limit', type=int, default=50, help='Limit number of rows shown (default: 50)')
    
    args = parser.parse_args()
    
    # Initialize database viewer
    viewer = PlontisDBViewer()
    
    try:
        if args.table:
            # Show specific table details
            viewer.print_table_details(args.table)
        elif args.analytics:
            # Show detailed analytics
            viewer.print_analytics()
        elif args.export:
            # Export data
            viewer.export_data(args.export, args.table)
        else:
            # Default: show overview and all table details
            viewer.print_table_overview()
            
            tables = viewer.get_all_tables()
            for table in tables:
                print()
                viewer.print_table_details(table['table_name'])
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        viewer.close()

if __name__ == "__main__":
    main()