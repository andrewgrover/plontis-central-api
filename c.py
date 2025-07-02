#!/usr/bin/env python3
"""
Verify the database fix worked and troubleshoot the site count
"""

import pymysql

def verify_database_fix():
    print("=== Verifying Database Fix ===")
    
    # Database credentials
    host = "plontis-central.cu7ee8mue0y6.us-east-1.rds.amazonaws.com"
    user = "plontis"
    password = "Andyandy19"
    database = "plontis_central"
    
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4'
        )
        
        with connection.cursor() as cursor:
            print("🔍 Checking current database state...")
            
            # Check for NULL site_hash
            cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NULL")
            null_count = cursor.fetchone()[0]
            print(f"📊 Records with NULL site_hash: {null_count}")
            
            # Check for non-NULL site_hash  
            cursor.execute("SELECT COUNT(*) FROM detections WHERE site_hash IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            print(f"📊 Records with valid site_hash: {non_null_count}")
            
            # Show all site_hash values
            cursor.execute("SELECT site_hash, COUNT(*) FROM detections GROUP BY site_hash")
            all_hashes = cursor.fetchall()
            print(f"\n🔍 All site_hash values:")
            for hash_val, count in all_hashes:
                if hash_val is None:
                    print(f"  NULL: {count} records")
                else:
                    print(f"  {hash_val}: {count} records")
            
            # Test the exact query that's failing
            print(f"\n🎯 Testing the exact market intelligence query...")
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT site_hash) as total_sites,
                    COUNT(*) as total_detections,
                    AVG(estimated_value) as avg_value_per_detection
                FROM detections 
                WHERE detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            """)
            
            result = cursor.fetchone()
            print(f"  Total Sites (DISTINCT site_hash): {result[0]}")
            print(f"  Total Detections: {result[1]}")
            print(f"  Avg Value: {result[2]:.2f}" if result[2] else "  Avg Value: 0.00")
            
            # Let's also check without the date filter
            print(f"\n🔍 Testing without date filter (all time)...")
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT site_hash) as total_sites,
                    COUNT(*) as total_detections,
                    AVG(estimated_value) as avg_value_per_detection
                FROM detections 
            """)
            
            result_all = cursor.fetchone()
            print(f"  Total Sites (all time): {result_all[0]}")
            print(f"  Total Detections (all time): {result_all[1]}")
            print(f"  Avg Value (all time): {result_all[2]:.2f}" if result_all[2] else "  Avg Value: 0.00")
            
            # Check the detected_at dates
            print(f"\n📅 Checking detection dates...")
            cursor.execute("""
                SELECT 
                    MIN(detected_at) as earliest,
                    MAX(detected_at) as latest,
                    COUNT(*) as total
                FROM detections
            """)
            
            date_result = cursor.fetchone()
            print(f"  Earliest detection: {date_result[0]}")
            print(f"  Latest detection: {date_result[1]}")
            print(f"  Total records: {date_result[2]}")
            
            # Check how many are within 30 days
            cursor.execute("""
                SELECT COUNT(*) 
                FROM detections 
                WHERE detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            """)
            recent_count = cursor.fetchone()[0]
            print(f"  Records within 30 days: {recent_count}")
            
            # If we have NULL values, let's fix them NOW
            if null_count > 0:
                print(f"\n🔧 Found {null_count} NULL site_hash values. Fixing them...")
                correct_site_hash = "f51bc27669e6f0c9cd71fe4dae0c03f44d461738e7e45b615188398def349678"
                
                cursor.execute("""
                    UPDATE detections 
                    SET site_hash = %s 
                    WHERE site_hash IS NULL OR site_hash = ''
                """, (correct_site_hash,))
                
                updated = cursor.rowcount
                print(f"✅ Updated {updated} records with site_hash")
                
                # Test again after fix
                cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT site_hash) as total_sites,
                        COUNT(*) as total_detections
                    FROM detections 
                    WHERE detected_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
                """)
                
                final_result = cursor.fetchone()
                print(f"\n🎉 After fix:")
                print(f"  Total Sites: {final_result[0]}")
                print(f"  Total Detections: {final_result[1]}")
                
        connection.close()
        
    except Exception as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    verify_database_fix()