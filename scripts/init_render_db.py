#!/usr/bin/env python3
"""
Render Database Initialization Script
Sets up PostgreSQL + TimescaleDB on Render for production deployment
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add src to path - absolute path approach
script_dir = Path(__file__).parent
project_root = script_dir.parent
src_dir = project_root / "src"

# Ensure src directory exists
if not src_dir.exists():
    print(f"❌ Source directory not found: {src_dir}")
    print("   Please run this script from the project root directory")
    sys.exit(1)

# Add to path with absolute path
src_path_str = str(src_dir.absolute())
if src_path_str not in sys.path:
    sys.path.insert(0, src_path_str)

# Verify models file exists
models_file = src_dir / "database" / "models.py"
if not models_file.exists():
    print(f"❌ Models file not found: {models_file}")
    print("   Please ensure the database models file exists")
    sys.exit(1)

# Now import - if this fails, there's a deeper issue
try:
    from database.models import DatabaseManager, initialize_database, Platform
    from sqlalchemy import text, select, func
except ImportError as e:
    print(f"❌ Import failed even with correct path setup: {e}")
    print(f"   Source path: {src_path_str}")
    print(f"   Models file exists: {models_file.exists()}")
    print("   Please check if there are syntax errors in models.py")
    sys.exit(1)


def verify_render_connection() -> bool:
    """Verify connection to Render PostgreSQL database"""
    print("🔄 Verifying Render database connection...")
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL environment variable not set")
        print("   Please set DATABASE_URL to your Render PostgreSQL connection string")
        print("   Example: postgresql://username:password@dpg-xxxxx-a.oregon-postgres.render.com/database_name")
        return False
    
    if 'sqlite' in db_url.lower():
        print("❌ DATABASE_URL points to SQLite, not PostgreSQL")
        print("   Please update DATABASE_URL to your Render PostgreSQL connection string")
        return False
    
    if 'render' not in db_url.lower() and 'postgres' not in db_url.lower():
        print("⚠️  DATABASE_URL doesn't appear to be a Render PostgreSQL URL")
        print(f"   Current URL: {db_url[:50]}...")
        response = input("   Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return False
    
    try:
        db_manager = DatabaseManager(db_url)
        
        with db_manager.get_session() as session:
            result = session.execute(text("SELECT version();"))
            version = result.fetchone()
            if version:
                version_str = version[0]
                print(f"✅ Connected to PostgreSQL: {version_str[:50]}...")
            else:
                print("✅ Connected to PostgreSQL successfully")
            
        return True
        
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("   Please check your DATABASE_URL and ensure the database is accessible")
        return False


def setup_timescaledb_extension() -> bool:
    """Enable TimescaleDB extension on Render PostgreSQL"""
    print("🔄 Setting up TimescaleDB extension...")
    
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not set")
            return False
            
        db_manager = DatabaseManager(db_url)
        
        with db_manager.engine.connect() as conn:
            # Enable TimescaleDB extension
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            conn.commit()
            
            # Verify extension is installed
            result = conn.execute(text("""
                SELECT extname, extversion 
                FROM pg_extension 
                WHERE extname = 'timescaledb';
            """))
            
            extension_info = result.fetchone()
            if extension_info:
                print(f"✅ TimescaleDB extension enabled: {extension_info[0]} v{extension_info[1]}")
            else:
                print("❌ TimescaleDB extension not found")
                print("   Note: TimescaleDB may not be available on all Render PostgreSQL plans")
                print("   The system will work without it, but with reduced performance")
                return False
        
        return True
        
    except Exception as e:
        print(f"⚠️  TimescaleDB setup failed: {e}")
        print("   Note: TimescaleDB may not be available on your Render plan")
        print("   The system will work with regular PostgreSQL tables")
        return False


def initialize_database_schema() -> bool:
    """Initialize database tables and reference data"""
    print("🔄 Initializing database schema...")
    
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not set")
            return False
            
        db_manager = initialize_database(db_url)
        
        print("✅ Database schema initialized successfully")
        
        # Verify tables were created
        with db_manager.get_session() as session:
            platforms = session.execute(select(Platform)).scalars().all()
            print(f"✅ Found {len(platforms)} platforms configured:")
            
            for platform in platforms:
                print(f"   - {platform.code}: {platform.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def setup_production_optimizations() -> bool:
    """Apply production-specific database optimizations"""
    print("🔄 Applying production optimizations...")
    
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not set")
            return False
            
        db_manager = DatabaseManager(db_url)
        
        with db_manager.engine.connect() as conn:
            # Check if TimescaleDB is available
            result = conn.execute(text("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
                );
            """))
            timescale_available = result.fetchone()
            timescale_enabled = timescale_available[0] if timescale_available else False
            
            if timescale_enabled:
                print("✅ TimescaleDB detected - applying time-series optimizations...")
                
                # Create hypertable for streaming_records
                try:
                    conn.execute(text("""
                        SELECT create_hypertable('streaming_records', 'date', 
                                                chunk_time_interval => INTERVAL '1 month',
                                                if_not_exists => TRUE);
                    """))
                    print("✅ Hypertable created for streaming_records")
                except Exception as e:
                    if "already a hypertable" in str(e):
                        print("✅ Hypertable already exists for streaming_records")
                    else:
                        print(f"⚠️  Hypertable creation failed: {e}")
                
                # Create continuous aggregates for common queries
                try:
                    conn.execute(text("""
                        CREATE MATERIALIZED VIEW IF NOT EXISTS daily_platform_metrics
                        WITH (timescaledb.continuous) AS
                        SELECT 
                            time_bucket('1 day', date) AS day,
                            platform_id,
                            metric_type,
                            SUM(metric_value) as total_value,
                            COUNT(*) as record_count,
                            AVG(data_quality_score) as avg_quality_score
                        FROM streaming_records
                        GROUP BY day, platform_id, metric_type
                        WITH NO DATA;
                    """))
                    print("✅ Continuous aggregates created")
                except Exception as e:
                    if "already exists" in str(e):
                        print("✅ Continuous aggregates already exist")
                    else:
                        print(f"⚠️  Continuous aggregates creation failed: {e}")
            
            else:
                print("✅ Regular PostgreSQL detected - applying standard optimizations...")
                
                # Apply standard PostgreSQL optimizations
                optimizations = [
                    "SET shared_preload_libraries = 'pg_stat_statements';",
                    "SET log_statement = 'mod';", 
                    "SET log_min_duration_statement = 1000;",  # Log slow queries > 1s
                ]
                
                for opt in optimizations:
                    try:
                        conn.execute(text(opt))
                    except Exception as e:
                        print(f"⚠️  Optimization skipped: {opt[:30]}... ({e})")
            
            conn.commit()
            
        print("✅ Production optimizations applied")
        return True
        
    except Exception as e:
        print(f"⚠️  Production optimizations failed: {e}")
        return False


def verify_production_readiness() -> bool:
    """Verify the database is ready for production use"""
    print("🔄 Verifying production readiness...")
    
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print("❌ DATABASE_URL not set")
            return False
            
        db_manager = DatabaseManager(db_url)
        
        checks = []
        
        with db_manager.get_session() as session:
            # Check 1: Platform data - use count query properly
            platform_count_result = session.execute(select(func.count(Platform.id))).scalar()
            platform_count = platform_count_result if platform_count_result is not None else 0
            checks.append(("Platform data", platform_count >= 9, f"{platform_count} platforms"))
            
            # Check 2: Database connectivity
            checks.append(("Database connectivity", True, "Connection successful"))
            
            # Check 3: Table structure
            try:
                # Import StreamingRecord with same pattern
                try:
                    from database.models import StreamingRecord
                except ImportError:
                    # If import fails, skip this check
                    checks.append(("Table structure", False, "Could not import StreamingRecord"))
                    StreamingRecord = None
                
                if StreamingRecord is not None:
                    # Test table structure by attempting basic operations
                    test_query = session.execute(select(StreamingRecord).limit(1))
                    test_query.scalars().all()  # This will work even if no records exist
                    checks.append(("Table structure", True, "All tables accessible"))
            except Exception as e:
                checks.append(("Table structure", False, f"Error: {e}"))
        
        # Check 4: Environment variables
        required_env_vars = ['DATABASE_URL']
        env_checks = []
        for var in required_env_vars:
            value = os.getenv(var)
            env_checks.append((var, bool(value), "Set" if value else "Missing"))
        
        # Print results
        print("\n📋 PRODUCTION READINESS CHECKLIST:")
        print("-" * 40)
        
        all_passed = True
        for check_name, passed, details in checks:
            status = "✅" if passed else "❌"
            print(f"{status} {check_name}: {details}")
            if not passed:
                all_passed = False
        
        print("\n📋 ENVIRONMENT VARIABLES:")
        print("-" * 40)
        for var_name, is_set, status in env_checks:
            status_icon = "✅" if is_set else "❌"
            print(f"{status_icon} {var_name}: {status}")
            if not is_set:
                all_passed = False
        
        if all_passed:
            print("\n🚀 PRODUCTION READY!")
            print("✅ All checks passed")
            print("✅ Database is ready for production use")
        else:
            print("\n⚠️  PRODUCTION READINESS ISSUES FOUND")
            print("❌ Please resolve the failed checks above")
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Production readiness check failed: {e}")
        return False


def main() -> None:
    """Main initialization function"""
    print("STREAMING ANALYTICS PLATFORM - RENDER DATABASE INITIALIZATION")
    print("=" * 70)
    print("Setting up PostgreSQL + TimescaleDB on Render for production deployment...")
    print()
    
    # Step 1: Verify connection
    if not verify_render_connection():
        return
    
    # Step 2: Setup TimescaleDB (optional - may not be available on all plans)
    timescale_success = setup_timescaledb_extension()
    
    # Step 3: Initialize schema
    if not initialize_database_schema():
        return
    
    # Step 4: Apply optimizations
    setup_production_optimizations()
    
    # Step 5: Verify readiness
    ready = verify_production_readiness()
    
    print()
    if ready:
        print("🎉 RENDER DATABASE INITIALIZATION COMPLETE!")
        print("=" * 50)
        print("Your production database is ready for use!")
        print()
        print("Next steps:")
        print("  1. Deploy your application to Render")
        print("  2. Test API endpoints")
        print("  3. Begin processing production data")
        print()
        if timescale_success:
            print("💡 TimescaleDB is enabled - you'll get optimal time-series performance")
        else:
            print("💡 Using regular PostgreSQL - performance will be good for moderate volumes")
    else:
        print("❌ INITIALIZATION INCOMPLETE")
        print("Please resolve the issues above and run again")


if __name__ == "__main__":
    main()