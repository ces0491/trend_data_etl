#!/usr/bin/env python3
"""
Quick SQLite Database Test Script
Tests basic database operations after setup
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def test_basic_operations():
    """Test basic database operations"""
    print("🔍 Testing basic database operations...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("⚠️  python-dotenv not available")
    
    db_url = os.getenv('DATABASE_URL', 'sqlite:///./temp/streaming_analytics.db')
    db_path = db_url.replace('sqlite:///', '')
    
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Test 1: Query platforms
    cursor.execute("SELECT code, name FROM platforms ORDER BY code")
    platforms = cursor.fetchall()
    print(f"✅ Found {len(platforms)} platforms")
    
    # Test 2: Insert test data
    test_record_id = f"test_{int(datetime.now().timestamp())}"
    cursor.execute("""
        INSERT INTO streaming_records 
        (id, date, platform_id, artist_name, track_title, metric_type, metric_value, data_quality_score)
        VALUES (?, ?, 1, ?, ?, ?, ?, ?)
    """, (test_record_id, datetime.now(), "Test Artist", "Test Song", "streams", 1000.0, 95.0))
    
    print("✅ Test record inserted successfully")
    
    # Test 3: Query test data
    cursor.execute("SELECT COUNT(*) FROM streaming_records WHERE id = ?", (test_record_id,))
    count = cursor.fetchone()[0]
    
    if count == 1:
        print("✅ Test record query successful")
    else:
        print(f"❌ Expected 1 record, found {count}")
        return False
    
    # Test 4: Use view
    cursor.execute("SELECT * FROM daily_platform_summary LIMIT 1")
    view_result = cursor.fetchone()
    print("✅ Database view query successful")
    
    # Clean up test data
    cursor.execute("DELETE FROM streaming_records WHERE id = ?", (test_record_id,))
    conn.commit()
    print("✅ Test data cleaned up")
    
    conn.close()
    print("✅ All database operations successful")
    return True

if __name__ == "__main__":
    success = test_basic_operations()
    print("\n" + "="*40)
    if success:
        print("🎉 DATABASE TEST PASSED!")
        print("Your SQLite database is ready for use.")
    else:
        print("❌ DATABASE TEST FAILED!")
        print("Please check your database setup.")
    
    sys.exit(0 if success else 1)
