# scripts/init_render_db.py
"""
Render Database Initialization Script
Sets up PostgreSQL + TimescaleDB on Render for production deployment
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def verify_render_connection():
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
        from database.models import DatabaseManager
        db_manager = DatabaseManager(db_url)
        
        with db_manager.get_session() as session:
            result = session.execute("SELECT version();")
            version = result.fetchone()[0]
            print(f"✅ Connected to PostgreSQL: {version[:50]}...")
            
        return True
        
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("   Please check your DATABASE_URL and ensure the database is accessible")
        return False

def setup_timescaledb_extension():
    """Enable TimescaleDB extension on Render PostgreSQL"""
    print("🔄 Setting up TimescaleDB extension...")
    
    try:
        from database.models import DatabaseManager
        from sqlalchemy import text
        
        db_url = os.getenv('DATABASE_URL')
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

def initialize_database_schema():
    """Initialize database tables and reference data"""
    print("🔄 Initializing database schema...")
    
    try:
        from database.models import initialize_database
        
        db_url = os.getenv('DATABASE_URL')
        db_manager = initialize_database(db_url)
        
        print("✅ Database schema initialized successfully")
        
        # Verify tables were created
        with db_manager.get_session() as session:
            from database.models import Platform
            
            platforms = session.query(Platform).all()
            print(f"✅ Found {len(platforms)} platforms configured:")
            
            for platform in platforms:
                print(f"   - {platform.code}: {platform.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def setup_production_optimizations():
    """Apply production-specific database optimizations"""
    print("🔄 Applying production optimizations...")
    
    try:
        from database.models import DatabaseManager
        from sqlalchemy import text
        
        db_url = os.getenv('DATABASE_URL')
        db_manager = DatabaseManager(db_url)
        
        with db_manager.engine.connect() as conn:
            # Check if TimescaleDB is available
            result = conn.execute(text("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
                );
            """))
            timescale_available = result.fetchone()[0]
            
            if timescale_available:
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

def verify_production_readiness():
    """Verify the database is ready for production use"""
    print("🔄 Verifying production readiness...")
    
    try:
        from database.models import DatabaseManager
        
        db_url = os.getenv('DATABASE_URL')
        db_manager = DatabaseManager(db_url)
        
        checks = []
        
        with db_manager.get_session() as session:
            from database.models import Platform, Artist, Track, StreamingRecord
            
            # Check 1: Platform data
            platform_count = session.query(Platform).count()
            checks.append(("Platform data", platform_count >= 9, f"{platform_count} platforms"))
            
            # Check 2: Database connectivity
            checks.append(("Database connectivity", True, "Connection successful"))
            
            # Check 3: Table structure
            try:
                # Test table structure by attempting basic operations
                session.query(StreamingRecord).limit(1).all()
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

def main():
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