# src/api/main.py
"""
FastAPI Application for Streaming Analytics Platform
Production-ready API with proper structure and error handling
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GzipMiddleware
import uvicorn

# Setup Python path
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent
src_dir = project_root / "src"

if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Import our modules
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from database.models import DatabaseManager, initialize_database
from api.routes import platforms, artists, tracks, streaming_records, data_quality, health
from api.dependencies import get_db_manager
from api.models import APIError

# Global database manager
db_manager = None


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
        print("✅ Database initialized successfully")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        raise
    
    yield
    
    # Shutdown
    print("🛑 Shutting down Streaming Analytics Platform API...")


# Create FastAPI application
app = FastAPI(
    title="Streaming Analytics Platform API",
    description="""
    **Phase 1: Data Platform Foundation + Data Access API**
    
    This API provides access to processed streaming data from multiple platforms including:
    - Spotify, Apple Music, Facebook/Meta, SoundCloud
    - Boomplay, AWA, Vevo, Peloton, Deezer
    
    ## Features
    - **Real-time data access** with quality scoring
    - **Cross-platform analytics** and insights
    - **Comprehensive validation** and processing audit trail
    - **Time-series optimized** for streaming data patterns
    
    ## Quality Standards
    - 95%+ parsing success rate
    - 90%+ data quality scores
    - <500ms API response times
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GzipMiddleware, minimum_size=1000)

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=APIError(
            error=exc.detail,
            status_code=exc.status_code,
            path=str(request.url.path)
        ).model_dump()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content=APIError(
            error="Internal server error",
            status_code=500,
            path=str(request.url.path),
            details=str(exc) if os.getenv('DEBUG') == 'true' else None
        ).model_dump()
    )

# Dependency to provide database manager
def get_database_manager() -> DatabaseManager:
    """Dependency to get database manager"""
    if db_manager is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db_manager

# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Streaming Analytics Platform API",
        "version": "1.0.0",
        "description": "Phase 1: Data Platform Foundation + Data Access API",
        "supported_platforms": [
            "apl-apple", "awa-awa", "boo-boomplay", "dzr-deezer",
            "fbk-facebook", "plt-peloton", "scu-soundcloud", 
            "spo-spotify", "vvo-vevo"
        ],
        "docs": "/docs",
        "health": "/health"
    }

# Include API routes
app.include_router(health.router, tags=["Health"])
app.include_router(platforms.router, prefix="/platforms", tags=["Platforms"])
app.include_router(artists.router, prefix="/artists", tags=["Artists"])
app.include_router(tracks.router, prefix="/tracks", tags=["Tracks"])
app.include_router(streaming_records.router, prefix="/streaming-records", tags=["Streaming Records"])
app.include_router(data_quality.router, prefix="/data-quality", tags=["Data Quality"])

# File processing endpoint (for demo/testing)
@app.post("/process-file", tags=["Processing"])
async def process_file(
    file: UploadFile = File(...),
    force_reprocess: bool = Query(False, description="Force reprocessing of already processed files"),
    db: DatabaseManager = Depends(get_database_manager)
):
    """
    Process an uploaded streaming data file
    **Note:** This endpoint is for testing/demo purposes
    """
    from etl.data_processor import StreamingDataProcessor
    import tempfile
    
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp_file:
        content = await file.read()
        tmp_file.write(content)
        tmp_file_path = Path(tmp_file.name)
    
    try:
        # Process the file
        processor = StreamingDataProcessor(db)
        result = processor.process_file(tmp_file_path, force_reprocess)
        
        # Clean up
        tmp_file_path.unlink()
        
        return {
            "success": result.success,
            "filename": file.filename,
            "platform": result.platform,
            "records_processed": result.records_processed,
            "records_failed": result.records_failed,
            "quality_score": result.quality_score,
            "processing_time": result.processing_time,
            "error_message": result.error_message
        }
        
    except Exception as e:
        # Clean up on error
        if tmp_file_path.exists():
            tmp_file_path.unlink()
        raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")


# Development server
def run_dev_server():
    """Run development server"""
    uvicorn.run(
        "src.api.main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=True,
        reload_dirs=["src"]
    )


if __name__ == "__main__":
    run_dev_server()