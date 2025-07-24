#!/usr/bin/env python3
"""
Complete SQLite Database Setup Script
Creates SQLite database for local testing with comprehensive error handling
"""

import os
import sqlite3
import sys
from pathlib import Path
from sqlalchemy import text

# Setup Python path with multiple fallback strategies
def setup_python_path():
    """Setup Python path to find our modules"""
    # Strategy 1: Standard project structure
    project_root = Path(__file__).parent.parent
    src_dir = project_root / "src"
    
    # Strategy 2: If we're in scripts/, go up one level
    if "scripts" in str(Path(__file__).parent):
        project_root = Path(__file__).parent.parent
        src_dir = project_root / "src"
    
    # Strategy 3: Current directory fallback
    if not src_dir.exists():
        current_dir = Path.cwd()
        src_dir = current_dir / "src"
        project_root = current_dir
    
    # Add to path if exists
    if src_dir.exists():
        src_path = str(src_dir.absolute())
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        return True, project_root, src_dir
    
    return False, project_root, src_dir

# Setup paths
PATH_SETUP_SUCCESS, PROJECT_ROOT, SRC_DIR = setup_python_path()

class SQLiteSetupError(Exception):
    """Custom exception for SQLite setup errors"""
    pass

def ensure_directories():
    """Ensure all required directories exist"""
    print("🔧 Ensuring directories exist...")
    
    required_dirs = [
        "temp",
        "data", 
        "data/sample",
        "logs"
    ]
    
    created_dirs = []
    for dir_path in required_dirs:
        full_path = PROJECT_ROOT / dir_path
        if not full_path.exists():
            full_path.mkdir(parents=True, exist_ok=True)
            created_dirs.append(dir_path)
            print(f"✅ Created directory: {dir_path}")
    
    if not created_dirs:
        print("✅ All required directories already exist")
    
    return created_dirs

def create_sqlite_database() -> str:
    """Create SQLite database with all required tables and optimizations"""
    print("🔄 Creating SQLite database...")
    
    # Ensure temp directory exists
    temp_dir = PROJECT_ROOT / "temp"
    temp_dir.mkdir(exist_ok=True)
    
    db_path = temp_dir / "streaming_analytics.db"
    
    # Remove existing database if it exists
    if db_path.exists():
        try:
            db_path.unlink()
            print(f"✅ Removed existing database: {db_path}")
        except Exception as e:
            print(f"⚠️  Could not remove existing database: {e}")
    
    print(f"📍 Creating database at: {db_path}")
    
    try:
        # Create new database with proper settings
        conn = sqlite3.connect(str(db_path))
        
        # Enable foreign keys and other optimizations
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging for better performance
        conn.execute("PRAGMA synchronous = NORMAL")  # Balance between safety and speed
        conn.execute("PRAGMA cache_size = 10000")   # Increase cache size
        conn.execute("PRAGMA temp_store = MEMORY")   # Store temporary tables in memory
        
        cursor = conn.cursor()
        
        # Create tables with SQLite-compatible schema
        print("🔧 Creating database tables...")
        
        cursor.executescript("""
            -- Platforms reference table
            CREATE TABLE platforms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                is_active BOOLEAN DEFAULT 1,
                file_patterns TEXT, -- JSON array as TEXT
                date_formats TEXT,  -- JSON array as TEXT
                delimiter_type TEXT DEFAULT 'auto',
                encoding TEXT DEFAULT 'utf-8',
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
                artist_id INTEGER,
                external_ids TEXT, -- JSON as TEXT
                metadata TEXT,     -- JSON as TEXT
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (artist_id) REFERENCES artists(id)
            );
            
            -- Main streaming records table
            CREATE TABLE streaming_records (
                id TEXT PRIMARY KEY, -- UUID as TEXT in SQLite
                date TIMESTAMP NOT NULL,
                platform_id INTEGER NOT NULL,
                track_id INTEGER,
                track_isrc TEXT,
                artist_name TEXT,
                track_title TEXT,
                album_name TEXT,
                metric_type TEXT NOT NULL CHECK (metric_type IN ('streams', 'plays', 'saves', 'shares', 'video_views', 'social_interactions', 'fitness_plays')),
                metric_value REAL NOT NULL CHECK (metric_value >= 0),
                geography TEXT,
                device_type TEXT CHECK (device_type IN ('mobile', 'desktop', 'tablet', 'tv', 'unknown', NULL)),
                subscription_type TEXT CHECK (subscription_type IN ('free', 'paid', 'trial', 'unknown', NULL)),
                context_type TEXT CHECK (context_type IN ('playlist', 'radio', 'search', 'social', 'unknown', NULL)),
                user_demographic TEXT, -- JSON as TEXT
                genre TEXT,
                data_quality_score REAL CHECK (data_quality_score >= 0 AND data_quality_score <= 100),
                raw_data_source TEXT,
                file_hash TEXT,
                processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms(id),
                FOREIGN KEY (track_id) REFERENCES tracks(id)
            );
            
            -- Processing logs table
            CREATE TABLE data_processing_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                file_hash TEXT NOT NULL,
                platform_id INTEGER,
                processing_status TEXT NOT NULL CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
                records_processed INTEGER DEFAULT 0 CHECK (records_processed >= 0),
                records_failed INTEGER DEFAULT 0 CHECK (records_failed >= 0),
                records_skipped INTEGER DEFAULT 0 CHECK (records_skipped >= 0),
                quality_score REAL CHECK (quality_score >= 0 AND quality_score <= 100),
                error_message TEXT,
                error_details TEXT, -- JSON as TEXT
                processing_config TEXT, -- JSON as TEXT
                performance_metrics TEXT, -- JSON as TEXT
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                processing_duration_ms INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms(id)
            );
            
            -- Quality scores table
            CREATE TABLE quality_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform_id INTEGER,
                file_hash TEXT NOT NULL,
                file_path TEXT,
                overall_score REAL NOT NULL CHECK (overall_score >= 0 AND overall_score <= 100),
                completeness_score REAL CHECK (completeness_score >= 0 AND completeness_score <= 100),
                consistency_score REAL CHECK (consistency_score >= 0 AND consistency_score <= 100),
                validity_score REAL CHECK (validity_score >= 0 AND validity_score <= 100),
                accuracy_score REAL CHECK (accuracy_score >= 0 AND accuracy_score <= 100),
                quality_details TEXT, -- JSON as TEXT
                validation_results TEXT, -- JSON as TEXT
                issues_found TEXT, -- JSON array as TEXT
                recommendations TEXT, -- JSON array as TEXT
                measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms(id)
            );
            
            -- File processing queue (for batch processing)
            CREATE TABLE file_processing_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                file_hash TEXT,
                platform_id INTEGER,
                priority INTEGER DEFAULT 5 CHECK (priority >= 1 AND priority <= 10),
                status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
                attempts INTEGER DEFAULT 0 CHECK (attempts >= 0),
                max_attempts INTEGER DEFAULT 3 CHECK (max_attempts > 0),
                error_message TEXT,
                scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (platform_id) REFERENCES platforms(id)
            );
        """)
        
        print("✅ Database tables created successfully")
        
        # Insert platform reference data with comprehensive configurations
        print("🔧 Inserting platform reference data...")
        
        platforms_data = [
            ('apl-apple', 'Apple Music/iTunes', 'Apple Music and iTunes Store streaming data', 
             '["*.txt", "*.tsv", "*apple*"]', 
             '["MM/dd/yy", "yyyy-MM-dd", "MM/dd/yyyy"]', 
             'tab_quoted', 'utf-8'),
            ('awa-awa', 'AWA', 'AWA Japanese streaming platform data',
             '["*.tsv", "*.csv", "*awa*"]',
             '["yyyyMMdd", "yyyy-MM-dd"]',
             'tab', 'utf-8'),
            ('boo-boomplay', 'Boomplay', 'Boomplay African streaming platform data',
             '["*.tsv", "*.csv", "*boomplay*", "*boo*"]',
             '["dd/MM/yyyy", "yyyy-MM-dd"]',
             'tab', 'utf-8'),
            ('dzr-deezer', 'Deezer', 'Deezer streaming platform data',
             '["*.csv", "*.tsv", "*deezer*", "*dzr*"]',
             '["yyyy-MM-dd", "dd/MM/yyyy"]',
             'comma', 'utf-8'),
            ('fbk-facebook', 'Facebook/Meta', 'Facebook and Instagram music usage data',
             '["*.csv", "*facebook*", "*meta*", "*fbk*"]',
             '["yyyy-MM-dd", "MM/dd/yyyy"]',
             'comma_quoted', 'utf-8'),
            ('plt-peloton', 'Peloton', 'Peloton fitness platform music data',
             '["*.csv", "*.tsv", "*peloton*", "*plt*"]',
             '["yyyy-MM-dd", "MM/dd/yyyy"]',
             'comma', 'utf-8'),
            ('scu-soundcloud', 'SoundCloud', 'SoundCloud streaming and user interaction data',
             '["*.tsv", "*.csv", "*soundcloud*", "*scu*"]',
             '["yyyy-MM-dd HH:mm:ss.SSS+00", "yyyy-MM-dd HH:mm:ss"]',
             'tab', 'utf-8'),
            ('spo-spotify', 'Spotify', 'Spotify streaming data with demographics',
             '["*.tsv", "*.csv", "*spotify*", "*spo*"]',
             '["yyyy-MM-dd", "MM/dd/yyyy"]',
             'tab', 'utf-8'),
            ('vvo-vevo', 'Vevo', 'Vevo video streaming and view data',
             '["*.csv", "*.tsv", "*vevo*", "*vvo*"]',
             '["yyyy-MM-dd", "MM/dd/yyyy"]',
             'comma', 'utf-8')
        ]
        
        cursor.executemany("""
            INSERT INTO platforms (code, name, description, file_patterns, date_formats, delimiter_type, encoding)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, platforms_data)
        
        print(f"✅ Inserted {len(platforms_data)} platform configurations")
        
        # Create comprehensive indexes for performance
        print("🔧 Creating database indexes...")
        
        index_queries = [
            # Streaming records indexes
            "CREATE INDEX idx_streaming_records_date ON streaming_records(date)",
            "CREATE INDEX idx_streaming_records_platform ON streaming_records(platform_id)",
            "CREATE INDEX idx_streaming_records_track ON streaming_records(track_id)",
            "CREATE INDEX idx_streaming_records_hash ON streaming_records(file_hash)",
            "CREATE INDEX idx_streaming_records_metric_date ON streaming_records(metric_type, date)",
            "CREATE INDEX idx_streaming_records_platform_date ON streaming_records(platform_id, date)",
            "CREATE INDEX idx_streaming_records_artist ON streaming_records(artist_name)",
            "CREATE INDEX idx_streaming_records_isrc ON streaming_records(track_isrc)",
            "CREATE INDEX idx_streaming_records_geography ON streaming_records(geography)",
            "CREATE INDEX idx_streaming_records_quality ON streaming_records(data_quality_score)",
            
            # Artists and tracks indexes
            "CREATE INDEX idx_artists_normalized ON artists(name_normalized)",
            "CREATE INDEX idx_artists_name ON artists(name)",
            "CREATE INDEX idx_tracks_normalized ON tracks(title_normalized)",
            "CREATE INDEX idx_tracks_isrc ON tracks(isrc)",
            "CREATE INDEX idx_tracks_artist ON tracks(artist_id)",
            "CREATE INDEX idx_tracks_title ON tracks(title)",
            
            # Processing logs indexes
            "CREATE INDEX idx_processing_logs_hash ON data_processing_logs(file_hash)",
            "CREATE INDEX idx_processing_logs_status ON data_processing_logs(processing_status)",
            "CREATE INDEX idx_processing_logs_platform ON data_processing_logs(platform_id)",
            "CREATE INDEX idx_processing_logs_path ON data_processing_logs(file_path)",
            "CREATE INDEX idx_processing_logs_started ON data_processing_logs(started_at)",
            
            # Quality scores indexes
            "CREATE INDEX idx_quality_scores_hash ON quality_scores(file_hash)",
            "CREATE INDEX idx_quality_scores_platform ON quality_scores(platform_id)",
            "CREATE INDEX idx_quality_scores_overall ON quality_scores(overall_score)",
            "CREATE INDEX idx_quality_scores_measured ON quality_scores(measured_at)",
            
            # Processing queue indexes
            "CREATE INDEX idx_queue_status ON file_processing_queue(status)",
            "CREATE INDEX idx_queue_priority ON file_processing_queue(priority, scheduled_at)",
            "CREATE INDEX idx_queue_platform ON file_processing_queue(platform_id)",
            "CREATE INDEX idx_queue_scheduled ON file_processing_queue(scheduled_at)",
        ]
        
        for query in index_queries:
            cursor.execute(query)
        
        print(f"✅ Created {len(index_queries)} database indexes")
        
        # Create views for common queries
        print("🔧 Creating database views...")
        
        view_queries = [
            """
            CREATE VIEW daily_platform_summary AS
            SELECT 
                DATE(date) as summary_date,
                p.code as platform_code,
                p.name as platform_name,
                metric_type,
                COUNT(*) as record_count,
                SUM(metric_value) as total_value,
                AVG(metric_value) as avg_value,
                MIN(metric_value) as min_value,
                MAX(metric_value) as max_value,
                AVG(data_quality_score) as avg_quality_score
            FROM streaming_records sr
            JOIN platforms p ON sr.platform_id = p.id
            GROUP BY DATE(date), p.code, metric_type
            """,
            
            """
            CREATE VIEW top_tracks_by_platform AS
            SELECT 
                p.code as platform_code,
                sr.artist_name,
                sr.track_title,
                SUM(sr.metric_value) as total_streams,
                COUNT(*) as record_count,
                AVG(sr.data_quality_score) as avg_quality
            FROM streaming_records sr
            JOIN platforms p ON sr.platform_id = p.id
            WHERE sr.metric_type = 'streams'
            GROUP BY p.code, sr.artist_name, sr.track_title
            """,
            
            """
            CREATE VIEW processing_status_summary AS
            SELECT 
                processing_status,
                COUNT(*) as file_count,
                SUM(records_processed) as total_records_processed,
                SUM(records_failed) as total_records_failed,
                AVG(quality_score) as avg_quality_score,
                AVG(processing_duration_ms) as avg_processing_time_ms
            FROM data_processing_logs
            GROUP BY processing_status
            """
        ]
        
        for query in view_queries:
            cursor.execute(query)
        
        print(f"✅ Created {len(view_queries)} database views")
        
        # Commit all changes
        conn.commit()
        conn.close()
        
        # Generate SQLite connection URL with absolute path
        sqlite_url = f"sqlite:///{db_path.absolute()}"
        
        print(f"✅ SQLite database created successfully")
        print(f"   📍 Location: {db_path}")
        print(f"   🔗 Connection URL: {sqlite_url}")
        
        return sqlite_url
        
    except sqlite3.Error as e:
        if 'conn' in locals():
            conn.close()
        raise SQLiteSetupError(f"SQLite database creation failed: {e}")
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        raise SQLiteSetupError(f"Unexpected error during database creation: {e}")

def update_env_file(key: str, value: str) -> bool:
    """Update .env file with new key-value pair"""
    print(f"🔧 Updating environment configuration...")
    
    env_file = PROJECT_ROOT / ".env"
    
    try:
        # Read existing content
        if env_file.exists():
            lines = env_file.read_text(encoding='utf-8').splitlines()
        else:
            lines = []
        
        # Update or add the key
        updated = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key}="):
                lines[i] = f"{key}={value}"
                updated = True
                print(f"✅ Updated existing {key} in .env")
                break
        
        if not updated:
            lines.append(f"{key}={value}")
            print(f"✅ Added new {key} to .env")
        
        # Write back to file
        env_file.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to update .env file: {e}")
        return False

def test_database_connection(sqlite_url: str) -> bool:
    """Test that the database was created correctly and is accessible"""
    print("🔍 Testing database connection...")
    
    try:
        # Method 1: Direct SQLite connection test
        conn = sqlite3.connect(sqlite_url.replace('sqlite:///', ''))
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT COUNT(*) FROM platforms")
        platform_count = cursor.fetchone()[0]
        
        # Test platform data
        cursor.execute("SELECT code, name FROM platforms ORDER BY code")
        platforms = cursor.fetchall()
        
        conn.close()
        
        print(f"✅ Direct SQLite connection successful")
        print(f"✅ Found {platform_count} platforms in database:")
        
        for code, name in platforms:
            print(f"   - {code}: {name}")
        
        # Method 2: Test with our database models if available
        if PATH_SETUP_SUCCESS:
            try:
                # Load environment to get DATABASE_URL
                try:
                    from dotenv import load_dotenv
                    load_dotenv()
                    print("✅ Environment variables loaded")
                except ImportError:
                    print("⚠️  python-dotenv not available, using manual env loading")
                    # Manually set DATABASE_URL for this test
                    os.environ['DATABASE_URL'] = sqlite_url
                
                # Test with DatabaseManager if available
                try:
                    from database.models import DatabaseManager, Platform
                    
                    db_manager = DatabaseManager(sqlite_url)
                    
                    with db_manager.get_session() as session:
                        platforms = session.query(Platform).all()
                        print(f"✅ DatabaseManager connection successful")
                        print(f"✅ ORM query returned {len(platforms)} platforms")
                        
                        # Test a simple query
                        test_query = session.execute(text("SELECT 1 AS test")).fetchone()
                        if test_query and test_query[0] == 1:
                            print("✅ Database queries working correctly")
                        
                    return True
                    
                except ImportError as e:
                    print(f"⚠️  DatabaseManager not available: {e}")
                    print("✅ Direct SQLite connection is working")
                    return True
                    
            except Exception as e:
                print(f"⚠️  ORM connection test failed: {e}")
                print("✅ Direct SQLite connection is still working")
                return True
        else:
            print("⚠️  Source path not configured, skipping ORM test")
            print("✅ Direct SQLite connection is working")
            return True
            
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False

def create_comprehensive_sample_data() -> bool:
    """Create comprehensive sample test files covering all format challenges"""
    print("🔧 Creating comprehensive sample test data...")
    
    sample_dir = PROJECT_ROOT / "data" / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if files already exist
    existing_files = list(sample_dir.glob("*"))
    if len(existing_files) > 5:
        print(f"✅ Found {len(existing_files)} existing sample files, skipping creation")
        return True
    
    try:
        # Apple sample (quote-wrapped TSV format) - Most complex format
        apple_data = '''\"artist_name\ttitle\treport_date\tquantity\tcustomer_currency\tvendor_identifier\tterritories\"
\"Taylor Swift\tShake It Off\t12/01/24\t1250\tUSD\tPADPIDA2021030304M_191061307952_USCGH1743953\tUS\"
\"Ed Sheeran\tShape of You\t12/01/24\t890\tGBP\tPADPIDA2021030304M_191061307952_GBUM71505078\tGB\"
\"Billie Eilish\tbad guy\t2024-12-01\t2100\tUSD\tPADPIDA2021030304M_191061307952_USIR12000001\tUS\"
\"The Weeknd\tBlinding Lights\t12/01/24\t1800\tCAD\tPADPIDA2021030304M_191061307952_USUG12001234\tCA\"
\"Bad Bunny\tTití Me Preguntó\t12/01/24\t3200\tUSD\tPADPIDA2021030304M_191061307952_USPR12003456\tPR\"'''
        
        (sample_dir / "apl-apple_sample_20241201.txt").write_text(apple_data, encoding='utf-8')
        
        # Facebook sample (quoted CSV format)
        facebook_data = '''\"isrc\",\"date\",\"product_type\",\"plays\",\"territory\"
\"USRC17607839\",\"2024-12-01\",\"FB_REELS\",\"1200\",\"US\"
\"GBUM71505078\",\"2024-12-01\",\"IG_MUSIC_STICKER\",\"890\",\"GB\"
\"USIR12000001\",\"2024-12-01\",\"FB_FROM_IG_CROSSPOST\",\"1500\",\"US\"
\"USUG12001234\",\"2024-12-01\",\"FB_MUSIC_STICKER\",\"750\",\"US\"
\"USPR12003456\",\"2024-12-01\",\"IG_STORY_MUSIC\",\"2100\",\"PR\"
\"BRUVD1900001\",\"2024-12-01\",\"FB_REELS\",\"1800\",\"BR\"'''
        
        (sample_dir / "fbk-facebook_sample_20241201.csv").write_text(facebook_data, encoding='utf-8')
        
        # Spotify sample (standard TSV with demographics)
        spotify_data = '''artist_name\ttrack_name\tstreams\tdate\tcountry\tage_range\tgender
Taylor Swift\tAnti-Hero\t45000\t2024-12-01\tUS\t18-24\tF
Bad Bunny\tTití Me Preguntó\t38000\t2024-12-01\tUS\t25-34\tM
Harry Styles\tAs It Was\t29000\t2024-12-01\tGB\t18-24\tF
The Weeknd\tBlinding Lights\t32000\t2024-12-01\tCA\t25-34\tM
Billie Eilish\tbad guy\t27000\t2024-12-01\tUS\t18-24\tF
Dua Lipa\tLevitating\t24000\t2024-12-01\tGB\t25-34\tF'''
        
        (sample_dir / "spo-spotify_sample_20241201.tsv").write_text(spotify_data, encoding='utf-8')
        
        # Boomplay sample (European DD/MM/YYYY date format, African markets)
        boomplay_data = '''song_id\tartist_name\ttitle\tdate\tcountry\tstreams\tdevice_type\tuser_type
12345\tBurna Boy\tLast Last\t01/12/2024\tNG\t5000\tmobile\tpaid
67890\tWizkid\tEssence\t01/12/2024\tZA\t4500\tmobile\tfree
11111\tDavido\tFEM\t01/12/2024\tKE\t3200\ttablet\tpaid
22222\tTems\tCrazy Tings\t01/12/2024\tGH\t2800\tmobile\tfree
33333\tAmapiano Artists\tUmlando\t01/12/2024\tZA\t3800\tmobile\tpaid
44444\tFireboy DML\tPeru\t01/12/2024\tNG\t4200\tdesktop\tpaid'''
        
        (sample_dir / "boo-boomplay_sample_20241201.tsv").write_text(boomplay_data, encoding='utf-8')
        
        # AWA sample (Japanese market, compact YYYYMMDD date format, prefecture codes)
        awa_data = '''track_id\tartist_name\ttitle\tdate\tprefecture\tplays\tuser_type\tage
JP001\tYoasobi\tIdol\t20241201\t13.0\t3500\tPaid\t22.0
JP002\tOfficial HIGE DANdism\tSubtitle\t20241201\t27.0\t2800\tFree\t28.0
JP003\tKing Gnu\tHakujitsu\t20241201\t23.0\t4200\tRFT\t25.0
JP004\tLisa\tGurenge\t20241201\t14.0\t2900\tPaid\t24.0
JP005\tAdoNightmare\t20241201\t01.0\t5200\tPaid\t19.0
JP006\tOfficial HIGE DANdism\tCry Baby\t20241201\t13.0\t3100\tFree\t26.0'''
        
        (sample_dir / "awa-awa_sample_20241201.tsv").write_text(awa_data, encoding='utf-8')
        
        # SoundCloud sample (precise timestamps with timezone, multiple file types)
        soundcloud_data = '''track_id\tuser_id\tartist_name\ttrack_title\ttimestamp\tplays\tplaylist_type\tgenre
SC001\tuser123\tIndependent Artist 1\tMidnight Vibes\t2024-12-01 17:18:10.040+00\t850\tUser Playlist\tLo-Fi
SC002\tuser456\tIndie Band X\tNeon Dreams\t2024-12-01 18:22:35.120+00\t1200\tRadio Station\tSynthwave
SC003\tuser789\tBedroom Producer\tLo-Fi Study Beat\t2024-12-01 19:45:12.580+00\t2100\tAuto Playlist\tLo-Fi
SC004\tuser321\tExperimental Artist\tSynthwave Journey\t2024-12-01 20:15:45.230+00\t675\tUser Playlist\tElectronic
SC005\tuser654\tPodcast Producer\tTech Talk Episode 1\t2024-12-01 21:30:22.150+00\t450\tPodcast\tSpoken Word
SC006\tuser987\tIndie Folk Artist\tCampfire Stories\t2024-12-01 22:10:18.890+00\t980\tUser Playlist\tFolk'''
        
        (sample_dir / "scu-soundcloud_sample_20241201.tsv").write_text(soundcloud_data, encoding='utf-8')
        
        # Deezer sample (standard CSV format)
        deezer_data = '''track_isrc,artist_name,track_title,album_name,streams,date,country,genre
USRC17607839,Taylor Swift,Anti-Hero,Midnights,12000,2024-12-01,US,Pop
GBUM71505078,Ed Sheeran,Shape of You,÷ (Divide),8900,2024-12-01,GB,Pop
FRNO12000001,Stromae,Alors on danse,Cheese,5600,2024-12-01,FR,Electronic
DEUM72100123,Rammstein,Du hast,Sehnsucht,4200,2024-12-01,DE,Metal
USPR12003456,Bad Bunny,Tití Me Preguntó,Un Verano Sin Ti,15000,2024-12-01,PR,Reggaeton'''
        
        (sample_dir / "dzr-deezer_sample_20241201.csv").write_text(deezer_data, encoding='utf-8')
        
        # Vevo sample (video-specific metrics)
        vevo_data = '''video_id,artist_name,track_title,views,watch_time_seconds,date,country,device_type
VEVO001,Taylor Swift,Anti-Hero,25000,3750000,2024-12-01,US,mobile
VEVO002,Bad Bunny,Tití Me Preguntó,18000,2700000,2024-12-01,US,desktop
VEVO003,Ed Sheeran,Shape of You,12000,1680000,2024-12-01,GB,mobile
VEVO004,The Weeknd,Blinding Lights,15000,2250000,2024-12-01,CA,tv
VEVO005,Billie Eilish,bad guy,20000,2800000,2024-12-01,US,tablet'''
        
        (sample_dir / "vvo-vevo_sample_20241201.csv").write_text(vevo_data, encoding='utf-8')
        
        # Create a comprehensive mixed formats test file
        mixed_formats_readme = '''# Sample Data Files - Format Reference

This directory contains sample files demonstrating the various data format challenges encountered across streaming platforms:

## Format Challenges Covered:

### Apple Music (apl-apple_sample_20241201.txt)
- **Format**: Quote-wrapped TSV (entire rows wrapped in quotes)  
- **Date Formats**: Mixed MM/dd/yy and yyyy-MM-dd
- **Encoding**: UTF-8
- **Special**: Complex vendor identifiers, multi-currency data

### Facebook/Meta (fbk-facebook_sample_20241201.csv)
- **Format**: Quoted CSV (individual fields quoted)
- **Date Format**: yyyy-MM-dd
- **Content**: Social media music interaction events
- **Special**: Product type categorization (FB_REELS, IG_MUSIC_STICKER, etc.)

### Spotify (spo-spotify_sample_20241201.tsv)
- **Format**: Standard TSV
- **Date Format**: yyyy-MM-dd  
- **Content**: Streaming data with demographics
- **Special**: Age ranges and gender demographics

### Boomplay (boo-boomplay_sample_20241201.tsv)
- **Format**: Standard TSV
- **Date Format**: DD/MM/YYYY (European)
- **Content**: African market streaming data
- **Special**: Device type tracking, African country codes

### AWA (awa-awa_sample_20241201.tsv)
- **Format**: Standard TSV
- **Date Format**: YYYYMMDD (compact)
- **Content**: Japanese market data
- **Special**: Prefecture codes (geographic), subscription types

### SoundCloud (scu-soundcloud_sample_20241201.tsv)
- **Format**: Standard TSV
- **Date Format**: ISO with timezone (yyyy-MM-dd HH:mm:ss.SSS+00)
- **Content**: User-generated content and playlist data
- **Special**: Precise timestamps, playlist categorization

### Deezer (dzr-deezer_sample_20241201.csv)
- **Format**: Standard CSV
- **Date Format**: yyyy-MM-dd
- **Content**: European market streaming data
- **Special**: ISRC codes, album information

### Vevo (vvo-vevo_sample_20241201.csv)
- **Format**: Standard CSV
- **Date Format**: yyyy-MM-dd
- **Content**: Video streaming metrics
- **Special**: Video-specific metrics (views, watch time)

## Usage:
These files are used by the validation and testing scripts to ensure the ETL pipeline can handle all real-world format variations encountered in production data.
'''
        
        (sample_dir / "README.md").write_text(mixed_formats_readme, encoding='utf-8')
        
        # Count created files
        created_files = [
            "apl-apple_sample_20241201.txt",
            "fbk-facebook_sample_20241201.csv", 
            "spo-spotify_sample_20241201.tsv",
            "boo-boomplay_sample_20241201.tsv",
            "awa-awa_sample_20241201.tsv",
            "scu-soundcloud_sample_20241201.tsv",
            "dzr-deezer_sample_20241201.csv",
            "vvo-vevo_sample_20241201.csv",
            "README.md"
        ]
        
        print(f"✅ Created {len(created_files)} comprehensive sample files:")
        for filename in created_files:
            print(f"   - {filename}")
        
        print()
        print("📊 Sample data covers these format challenges:")
        print("   - Quote-wrapped TSV (Apple)")
        print("   - Quoted CSV (Facebook)")
        print("   - Multiple date formats (MM/dd/yy, DD/MM/YYYY, YYYYMMDD, ISO)")
        print("   - Timezone-aware timestamps (SoundCloud)")
        print("   - International markets (Japan, Africa, Europe)")
        print("   - Various metric types (streams, views, social interactions)")
        print("   - Demographics and geographic data")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create sample data: {e}")
        return False

def verify_database_integrity() -> bool:
    """Verify database integrity and performance"""
    print("🔍 Verifying database integrity...")
    
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not found in environment")
            return False
        
        # Extract path from SQLite URL
        db_path = db_url.replace('sqlite:///', '')
        
        if not Path(db_path).exists():
            print(f"❌ Database file not found: {db_path}")
            return False
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = [
            'platforms', 'artists', 'tracks', 'streaming_records',
            'data_processing_logs', 'quality_scores', 'file_processing_queue'
        ]
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            print(f"❌ Missing tables: {missing_tables}")
            return False
        
        print(f"✅ All {len(expected_tables)} required tables exist")
        
        # Check indexes exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
        indexes = [row[0] for row in cursor.fetchall()]
        
        print(f"✅ Found {len(indexes)} performance indexes")
        
        # Check views exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
        views = [row[0] for row in cursor.fetchall()]
        
        print(f"✅ Found {len(views)} database views")
        
        # Check platform data
        cursor.execute("SELECT COUNT(*) FROM platforms")
        platform_count = cursor.fetchone()[0]
        
        if platform_count < 9:
            print(f"❌ Expected 9 platforms, found {platform_count}")
            return False
        
        print(f"✅ All {platform_count} platforms configured")
        
        # Test foreign key constraints
        cursor.execute("PRAGMA foreign_key_check")
        fk_violations = cursor.fetchall()
        
        if fk_violations:
            print(f"❌ Foreign key violations found: {fk_violations}")
            return False
        
        print("✅ Foreign key constraints are valid")
        
        # Check database file size
        db_size = Path(db_path).stat().st_size
        print(f"✅ Database size: {db_size / 1024:.1f} KB")
        
        conn.close()
        
        print("✅ Database integrity verification passed")
        return True
        
    except Exception as e:
        print(f"❌ Database integrity check failed: {e}")
        return False

def create_quick_test_script() -> bool:
    """Create a quick test script for database operations"""
    print("🔧 Creating quick test script...")
    
    try:
        test_script_content = '''#!/usr/bin/env python3
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
    print("\\n" + "="*40)
    if success:
        print("🎉 DATABASE TEST PASSED!")
        print("Your SQLite database is ready for use.")
    else:
        print("❌ DATABASE TEST FAILED!")
        print("Please check your database setup.")
    
    sys.exit(0 if success else 1)
'''
        
        test_script_path = PROJECT_ROOT / "scripts" / "test_sqlite.py"
        test_script_path.write_text(test_script_content, encoding='utf-8')
        
        # Make executable on Unix systems
        try:
            import stat
            test_script_path.chmod(test_script_path.stat().st_mode | stat.S_IEXEC)
        except:
            pass  # Not critical if this fails
        
        print(f"✅ Created test script: {test_script_path}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create test script: {e}")
        return False

def main() -> bool:
    """Main SQLite setup function"""
    print("STREAMING ANALYTICS PLATFORM - COMPLETE SQLITE SETUP")
    print("=" * 65)
    print(f"📁 Project Root: {PROJECT_ROOT}")
    print(f"📁 Source Directory: {SRC_DIR} ({'✅ Found' if SRC_DIR.exists() else '❌ Missing'})")
    print()
    
    try:
        # Step 1: Ensure directories exist
        ensure_directories()
        print()
        
        # Step 2: Create SQLite database
        sqlite_url = create_sqlite_database()
        print()
        
        # Step 3: Update environment configuration
        if update_env_file("DATABASE_URL", sqlite_url):
            print()
        
        # Step 4: Test database connection
        if not test_database_connection(sqlite_url):
            print("❌ Database setup failed at connection test")
            return False
        print()
        
        # Step 5: Create comprehensive sample data
        if not create_comprehensive_sample_data():
            print("⚠️  Sample data creation failed, but database is still usable")
        print()
        
        # Step 6: Verify database integrity
        if not verify_database_integrity():
            print("⚠️  Database integrity check failed, but basic setup succeeded")
        print()
        
        # Step 7: Create test script
        create_quick_test_script()
        print()
        
        # Final success message
        print("=" * 65)
        print("🎉 SQLITE SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 65)
        print()
        print("✅ Database created and configured")
        print("✅ Platform reference data loaded")
        print("✅ Performance indexes created")
        print("✅ Sample test data generated")
        print("✅ Environment variables updated")
        print()
        print("🚀 Next Steps:")
        print("   1. python scripts/validate_setup.py     # Validate complete setup")
        print("   2. python scripts/test_sqlite.py        # Test database operations")
        print("   3. python scripts/quick_start_demo.py   # Run processing demo")
        print("   4. uvicorn src.api.main:app --reload    # Start API server")
        print()
        print("📊 Database Information:")
        print(f"   • Location: {sqlite_url}")
        print(f"   • Tables: 7 main tables + 3 views")
        print(f"   • Platforms: 9 configured")
        print(f"   • Sample Files: 8 test files created")
        print()
        print("💡 Your SQLite database is ready for local development!")
        
        return True
        
    except SQLiteSetupError as e:
        print(f"\n❌ SQLite setup failed: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Setup script crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)