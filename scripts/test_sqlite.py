#!/usr/bin/env python3
"""
Fixed SQLite Database Test Script
Tests basic database operations with correct schema
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime
import uuid

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def test_basic_operations():
    """Test basic database operations with correct schema"""
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
    
    # Test 2: Create test artist and track (following proper schema)
    test_record_id = str(uuid.uuid4())
    
    # Insert test artist
    cursor.execute("""
        INSERT INTO artists (name, name_normalized)
        VALUES (?, ?)
    """, ("Test Artist", "test artist"))
    
    artist_id = cursor.lastrowid
    print("✅ Test artist created")
    
    # Insert test track
    cursor.execute("""
        INSERT INTO tracks (title, title_normalized, artist_id)
        VALUES (?, ?, ?)
    """, ("Test Song", "test song", artist_id))
    
    track_id = cursor.lastrowid
    print("✅ Test track created")
    
    # Insert test streaming record (using correct schema)
    cursor.execute("""
        INSERT INTO streaming_records 
        (id, date, platform_id, track_id, metric_type, metric_value, data_quality_score)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (test_record_id, datetime.now(), 1, track_id, "streams", 1000.0, 95.0))
    
    print("✅ Test streaming record inserted successfully")
    
    # Test 3: Query test data with joins
    cursor.execute("""
        SELECT sr.id, a.name, t.title, sr.metric_value
        FROM streaming_records sr
        JOIN tracks t ON sr.track_id = t.id
        JOIN artists a ON t.artist_id = a.id
        WHERE sr.id = ?
    """, (test_record_id,))
    
    result = cursor.fetchone()
    
    if result:
        record_id, artist_name, track_title, metric_value = result
        print(f"✅ Test record query successful: {artist_name} - {track_title} ({metric_value} streams)")
    else:
        print("❌ Test record not found")
        return False
    
    # Test 4: Use view
    cursor.execute("SELECT * FROM daily_platform_summary LIMIT 1")
    view_result = cursor.fetchone()
    print("✅ Database view query successful")
    
    # Clean up test data
    cursor.execute("DELETE FROM streaming_records WHERE id = ?", (test_record_id,))
    cursor.execute("DELETE FROM tracks WHERE id = ?", (track_id,))
    cursor.execute("DELETE FROM artists WHERE id = ?", (artist_id,))
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