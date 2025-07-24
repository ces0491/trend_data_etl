# src/api/main.py
"""
FastAPI application for Streaming Analytics Platform
Fixed version with proper imports and error handling
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Import our modules
from database.models import DatabaseManager, initialize_database
from api.routes import platforms, artists, tracks, streaming_records, data_quality, health
from api.dependencies import set_db_manager
from api.models import APIError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global database manager
db_manager: DatabaseManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown events"""
    global db_manager
    
    # Startup
    logger.info("🚀 Starting Streaming Analytics Platform API...")
    
    # Initialize database
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.error("DATABASE_URL environment variable not set")
        raise RuntimeError("DATABASE_URL environment variable not set")
    
    try:
        db_manager = initialize_database(db_url)
        set_db_manager(db_manager)
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down Streaming Analytics Platform API...")


# Create FastAPI application
app = FastAPI(
    title="Streaming Analytics Platform API",
    description="Comprehensive analytics platform for streaming data across multiple platforms",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "path": str(request.url.path),
            "details": "An unexpected error occurred"
        }
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    """Handler for HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "path": str(request.url.path)
        }
    )


# Include API routes
app.include_router(health.router, tags=["Health"])
app.include_router(platforms.router, prefix="/platforms", tags=["Platforms"])
app.include_router(artists.router, prefix="/artists", tags=["Artists"])
app.include_router(tracks.router, prefix="/tracks", tags=["Tracks"])
app.include_router(streaming_records.router, prefix="/streaming-records", tags=["Streaming Records"])
app.include_router(data_quality.router, prefix="/data-quality", tags=["Data Quality"])


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Streaming Analytics Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    reload = os.getenv("API_RELOAD", "true").lower() == "true"
    
    logger.info(f"Starting server on {host}:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )