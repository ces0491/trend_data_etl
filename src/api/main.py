# src/api/main.py - UPDATED VERSION
# Replace the route imports and includes sections with this:

# Import our modules (UPDATED)
from database.models import DatabaseManager, initialize_database
from api.routes import platforms, artists, tracks, streaming_records, data_quality, health
from api.dependencies import set_db_manager  # ADD THIS LINE
from api.models import APIError

# In the lifespan function, ADD this line after db initialization:
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown events"""
    global db_manager
    
    # Startup
    print("🚀 Starting Streaming Analytics Platform API...")
    
    # Initialize database
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    
    try:
        db_manager = initialize_database(db_url)
        set_db_manager(db_manager)  # ADD THIS LINE
        print("✅ Database initialized successfully")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        raise
    
    yield
    
    # Shutdown
    print("🛑 Shutting down Streaming Analytics Platform API...")

# Include API routes (UPDATED - ADD NEW ROUTES)
app.include_router(health.router, tags=["Health"])
app.include_router(platforms.router, prefix="/platforms", tags=["Platforms"])
app.include_router(artists.router, prefix="/artists", tags=["Artists"])  # NEW
app.include_router(tracks.router, prefix="/tracks", tags=["Tracks"])  # NEW
app.include_router(streaming_records.router, prefix="/streaming-records", tags=["Streaming Records"])  # NEW
app.include_router(data_quality.router, prefix="/data-quality", tags=["Data Quality"])  # NEW