# scripts/setup_sqlite.py
"""
SQLite Database Setup Script
Creates SQLite database for local testing without Docker
"""

import os
import sqlite3
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def create_sqlite_database():
    """Create SQLite database with all required tables"""
    
    # Create temp directory if it doesn't exist
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    
    db_path = temp_dir / "trend_data_test.db"
    
    # Remove existing database
    if db_path.exists():
        db_path.unlink()
    
    print(f"Creating SQLite database: {db_path}")
    
    # Create new database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Create tables with SQLite-compatible schema
    cursor.executescript("""
        -- Platforms reference table
        CREATE TABLE platforms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Artists table
        CREATE TABLE artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL,
            external_ids TEXT, -- JSON as TEXT in SQLite
            metadata TEXT,     -- JSON as TEXT in SQLite
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Tracks table
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            isrc TEXT UNIQUE,
            title TEXT NOT NULL,
            title_normalized TEXT NOT NULL,
            album_name TEXT,
            duration_ms INTEGER,
            genre TEXT,
            artist_id INTEGER REFERENCES artists(id),
            external_ids TEXT, -- JSON as TEXT
            metadata TEXT,     -- JSON as TEXT
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Main streaming records table (hypertable in PostgreSQL, regular table in SQLite)
        CREATE TABLE streaming_records (
            id TEXT PRIMARY KEY, -- UUID as TEXT in SQLite
            date TIMESTAMP NOT NULL,
            platform_id INTEGER REFERENCES platforms(id),
            track_id INTEGER REFERENCES tracks(id),
            metric_type TEXT NOT NULL,
            metric_value REAL NOT NULL,
            geography TEXT,
            device_type TEXT,
            subscription_type TEXT,
            context_type TEXT,
            user_demographic TEXT, -- JSON as TEXT
            data_quality_score REAL,
            raw_data_source TEXT,
            file_hash TEXT,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Processing logs table
        CREATE TABLE data_processing_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            platform_id INTEGER REFERENCES platforms(id),
            processing_status TEXT NOT NULL,
            records_processed INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            quality_score REAL,
            error_message TEXT,
            processing_config TEXT, -- JSON as TEXT
            performance_metrics TEXT, -- JSON as TEXT
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Quality scores table
        CREATE TABLE quality_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform_id INTEGER REFERENCES platforms(id),
            file_hash TEXT NOT NULL,
            overall_score REAL NOT NULL,
            completeness_score REAL,
            consistency_score REAL,
            validity_score REAL,
            quality_details TEXT, -- JSON as TEXT
            validation_results TEXT, -- JSON as TEXT
            measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Insert platform reference data
        INSERT INTO platforms (code, name) VALUES
        ('apl-apple', 'Apple Music/iTunes'),
        ('awa-awa', 'AWA'),
        ('boo-boomplay', 'Boomplay'),
        ('dzr-deezer', 'Deezer'),
        ('fbk-facebook', 'Facebook/Meta'),
        ('plt-peloton', 'Peloton'),
        ('scu-soundcloud', 'SoundCloud'),
        ('spo-spotify', 'Spotify'),
        ('vvo-vevo', 'Vevo');
        
        -- Create indexes for performance
        CREATE INDEX idx_streaming_records_date ON streaming_records(date);
        CREATE INDEX idx_streaming_records_platform ON streaming_records(platform_id);
        CREATE INDEX idx_streaming_records_track ON streaming_records(track_id);
        CREATE INDEX idx_streaming_records_hash ON streaming_records(file_hash);
        CREATE INDEX idx_streaming_records_metric_date ON streaming_records(metric_type, date);
        
        CREATE INDEX idx_artists_normalized ON artists(name_normalized);
        CREATE INDEX idx_tracks_normalized ON tracks(title_normalized);
        CREATE INDEX idx_tracks_isrc ON tracks(isrc);
        CREATE INDEX idx_tracks_artist ON tracks(artist_id);
        
        CREATE INDEX idx_processing_logs_hash ON data_processing_logs(file_hash);
        CREATE INDEX idx_processing_logs_status ON data_processing_logs(processing_status);
        
        CREATE INDEX idx_quality_scores_hash ON quality_scores(file_hash);
        CREATE INDEX idx_quality_scores_platform ON quality_scores(platform_id);
    """)
    
    conn.commit()
    conn.close()
    
    # Update .env file with SQLite URL
    sqlite_url = f"sqlite:///{db_path.absolute()}"
    update_env_file("DATABASE_URL", sqlite_url)
    
    print(f"✅ SQLite database created successfully")
    print(f"   Location: {db_path}")
    print(f"   Connection: {sqlite_url}")
    
    return sqlite_url

def update_env_file(key, value):
    """Update .env file with new value"""
    env_file = Path(".env")
    
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    else:
        lines = []
    
    # Update or add the key
    updated = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            updated = True
            break
    
    if not updated:
        lines.append(f"{key}={value}")
    
    # Write back to file
    env_file.write_text('\n'.join(lines) + '\n')
    print(f"✅ Updated .env: {key}={value}")

def test_database_connection():
    """Test that the database was created correctly"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        from database.models import DatabaseManager
        
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not found in environment")
            return False
        
        db_manager = DatabaseManager(db_url)
        
        with db_manager.get_session() as session:
            from database.models import Platform
            platforms = session.query(Platform).all()
            
            print(f"✅ Database connection successful")
            print(f"✅ Found {len(platforms)} platforms:")
            
            for platform in platforms:
                print(f"   - {platform.code}: {platform.name}")
            
            return True
            
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False

def create_sample_test_files():
    """Create sample test files if none exist"""
    sample_dir = Path("data/sample")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if files already exist
    existing_files = list(sample_dir.glob("*"))
    if existing_files:
        print(f"✅ Found {len(existing_files)} existing sample files")
        return
    
    print("Creating sample test files for validation...")
    
    # Apple sample (quote-wrapped TSV format)
    apple_data = '''\"artist_name\ttitle\treport_date\tquantity\tcustomer_currency\tvendor_identifier\"
\"Taylor Swift\tShake It Off\t2024-01-15\t1250\tUSD\tPADPIDA2021030304M_191061307952_USCGH1743953\"
\"Ed Sheeran\tShape of You\t2024-01-15\t890\tUSD\tPADPIDA2021030304M_191061307952_GBUM71505078\"
\"Billie Eilish\tbad guy\t2024-01-15\t2100\tUSD\tPADPIDA2021030304M_191061307952_USIR12000001\"
\"The Weeknd\tBlinding Lights\t2024-01-15\t1800\tUSD\tPADPIDA2021030304M_191061307952_USUG12001234\"'''
    
    (sample_dir / "apl-apple_test.txt").write_text(apple_data, encoding='utf-8')
    
    # Facebook sample (quoted CSV format)
    facebook_data = '''\"isrc\",\"date\",\"product_type\",\"plays\"
\"USRC17607839\",\"2024-01-15\",\"FB_REELS\",\"1200\"
\"GBUM71505078\",\"2024-01-15\",\"IG_MUSIC_STICKER\",\"890\"
\"USIR12000001\",\"2024-01-15\",\"FB_FROM_IG_CROSSPOST\",\"1500\"
\"USUG12001234\",\"2024-01-15\",\"FB_MUSIC_STICKER\",\"750\"'''
    
    (sample_dir / "fbk-facebook_test.csv").write_text(facebook_data, encoding='utf-8')
    
    # Spotify sample (standard TSV)
    spotify_data = '''artist_name\ttrack_name\tstreams\tdate\tcountry
Taylor Swift\tAnti-Hero\t45000\t2024-01-15\tUS
Bad Bunny\tTití Me Preguntó\t38000\t2024-01-15\tUS
Harry Styles\tAs It Was\t29000\t2024-01-15\tUS
The Weeknd\tBlinding Lights\t32000\t2024-01-15\tUS'''
    
    (sample_dir / "spo-spotify_test.tsv").write_text(spotify_data, encoding='utf-8')
    
    # Boomplay sample (European date format DD/MM/YYYY)
    boomplay_data = '''song_id\tartist_name\ttitle\tdate\tcountry\tstreams
12345\tBurna Boy\tLast Last\t15/01/2024\tNG\t5000
67890\tWizkid\tEssence\t15/01/2024\tZA\t4500
11111\tDavido\tFEM\t15/01/2024\tKE\t3200
22222\tTems\tCrazy Tings\t15/01/2024\tGH\t2800'''
    
    (sample_dir / "boo-boomplay_test.tsv").write_text(boomplay_data, encoding='utf-8')
    
    # AWA sample (Japanese market, compact date format YYYYMMDD)
    awa_data = '''track_id\tartist_name\ttitle\tdate\tprefecture\tplays\tuser_type
JP001\tYoasobi\tIdol\t20240115\t13.0\t3500\tPaid
JP002\tOfficial HIGE DANdism\tSubtitle\t20240115\t27.0\t2800\tFree
JP003\tKing Gnu\tHakujitsu\t20240115\t23.0\t4200\tRFT
JP004\tLisa\tGurenge\t20240115\t14.0\t2900\tPaid'''
    
    (sample_dir / "awa-awa_test.tsv").write_text(awa_data, encoding='utf-8')
    
    # SoundCloud sample (multiple related files, precise timestamps with timezone)
    soundcloud_data = '''track_id\tuser_id\tartist_name\ttrack_title\ttimestamp\tplays\tplaylist_type
SC001\tuser123\tIndependent Artist 1\tMidnight Vibes\t2024-01-15 17:18:10.040+00\t850\tUser Playlist
SC002\tuser456\tIndie Band X\tNeon Dreams\t2024-01-15 18:22:35.120+00\t1200\tRadio Station
SC003\tuser789\tBedroom Producer\tLo-Fi Study Beat\t2024-01-15 19:45:12.580+00\t2100\tAuto Playlist
SC004\tuser321\tExperimental Artist\tSynthwave Journey\t2024-01-15 20:15:45.230+00\t675\tUser Playlist'''
    
    (sample_dir / "scu-soundcloud_test.tsv").write_text(soundcloud_data, encoding='utf-8')
    
    print(f"✅ Created 6 sample test files in {sample_dir}")
    print("   Files cover the main format challenges:")
    print("   - Apple: Quote-wrapped TSV format")
    print("   - Facebook: Quoted CSV format")  
    print("   - Boomplay: European DD/MM/YYYY date format")
    print("   - AWA: Compact YYYYMMDD date format")
    print("   - SoundCloud: Timezone-aware timestamps")
    print("   - Spotify: Standard TSV format")

def main():
    """Main setup function"""
    print("STREAMING ANALYTICS PLATFORM - SQLITE SETUP")
    print("=" * 50)
    
    try:
        # Step 1: Create SQLite database
        sqlite_url = create_sqlite_database()
        
        # Step 2: Test database connection
        if not test_database_connection():
            print("❌ Database setup failed")
            return
        
        # Step 3: Create sample files
        create_sample_test_files()
        
        print()
        print("🚀 SQLite setup complete!")
        print("=" * 30)
        print("Next steps:")
        print("  1. .\setup.ps1 demo           # Run quick demo")
        print("  2. .\setup.ps1 test-samples   # Test with sample data")
        print("  3. .\setup.ps1 serve          # Start API server")
        print()
        print("Database ready for local development and testing!")
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()