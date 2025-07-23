# scripts/init_production_db.py
import os
from src.database.models import initialize_database

if __name__ == "__main__":
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not set")
        exit(1)
    
    print("🔄 Initializing production database...")
    db = initialize_database(db_url)
    print("✅ Production database ready!")