# src/api/routes/health.py
"""
Health check and status endpoints
"""
from __future__ import annotations

import time
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db_session, get_db_manager
from api.models import HealthResponse
from database.models import DatabaseManager, Platform

# Global variable to track startup time
startup_time = time.time()

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check(
    session: Session = Depends(get_db_session),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """
    Comprehensive health check endpoint
    
    Checks:
    - Database connectivity
    - Platform configuration
    - Basic query functionality
    """
    try:
        # Test basic database connectivity
        result = session.execute(text("SELECT 1 as test")).fetchone()
        if not result or result[0] != 1:
            raise Exception("Database query test failed")
        
        database_status = "healthy"
        
        # Count configured platforms
        platforms_count = session.query(Platform).filter(Platform.is_active == True).count()
        
        # Calculate uptime
        uptime_seconds = time.time() - startup_time
        
        return HealthResponse(
            status="healthy",
            timestamp=datetime.utcnow(),
            database_status=database_status,
            platforms_configured=platforms_count,
            api_version="1.0.0",
            uptime_seconds=uptime_seconds
        )
        
    except Exception as e:
        # Return unhealthy status but don't crash
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.utcnow(),
            database_status=f"error: {str(e)}",
            platforms_configured=0,
            api_version="1.0.0",
            uptime_seconds=time.time() - startup_time
        )


@router.get("/health/database")
async def database_health_check(session: Session = Depends(get_db_session)):
    """
    Detailed database health check
    """
    try:
        # Test multiple database operations
        checks = {}
        
        # Basic connectivity
        result = session.execute(text("SELECT 1 as test")).fetchone()
        checks["connectivity"] = result[0] == 1 if result else False
        
        # Test platform table
        try:
            platforms_count = session.query(Platform).count()
            checks["platforms_table"] = True
            checks["platforms_count"] = platforms_count
        except Exception as e:
            checks["platforms_table"] = False
            checks["platforms_error"] = str(e)
        
        # Test database version
        try:
            version_result = session.execute(text("SELECT version()")).fetchone()
            if version_result:
                checks["database_version"] = version_result[0][:100]  # Truncate for readability
        except Exception:
            checks["database_version"] = "unknown"
        
        # Check if TimescaleDB is available (for PostgreSQL)
        try:
            timescale_result = session.execute(text("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
                );
            """)).fetchone()
            checks["timescaledb_available"] = timescale_result[0] if timescale_result else False
        except Exception:
            checks["timescaledb_available"] = False
        
        overall_status = "healthy" if all([
            checks.get("connectivity", False),
            checks.get("platforms_table", False)
        ]) else "degraded"
        
        return {
            "status": overall_status,
            "timestamp": datetime.utcnow(),
            "checks": checks
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=503, 
            detail=f"Database health check failed: {str(e)}"
        )


@router.get("/health/platforms")
async def platforms_health_check(session: Session = Depends(get_db_session)):
    """
    Platform configuration health check
    """
    try:
        platforms = session.query(Platform).all()
        
        platform_status = []
        for platform in platforms:
            platform_info = {
                "code": platform.code,
                "name": platform.name,
                "is_active": platform.is_active,
                "has_file_patterns": platform.file_patterns is not None,
                "has_date_formats": platform.date_formats is not None,
                "delimiter_type": platform.delimiter_type,
                "encoding": platform.encoding
            }
            platform_status.append(platform_info)
        
        active_count = sum(1 for p in platforms if p.is_active)
        
        return {
            "status": "healthy" if active_count > 0 else "warning",
            "timestamp": datetime.utcnow(),
            "total_platforms": len(platforms),
            "active_platforms": active_count,
            "platforms": platform_status
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Platform health check failed: {str(e)}"
        )


@router.get("/health/ready")
async def readiness_check(
    session: Session = Depends(get_db_session),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """
    Kubernetes-style readiness probe
    Returns 200 if the service is ready to handle requests
    """
    try:
        # Quick database test
        session.execute(text("SELECT 1")).fetchone()
        
        # Check minimum platform configuration
        platform_count = session.query(Platform).filter(Platform.is_active == True).count()
        
        if platform_count < 1:
            raise HTTPException(status_code=503, detail="No active platforms configured")
        
        return {"status": "ready", "timestamp": datetime.utcnow()}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service not ready: {str(e)}")


@router.get("/health/live")
async def liveness_check():
    """
    Kubernetes-style liveness probe
    Returns 200 if the service is alive (basic process health)
    """
    return {
        "status": "alive",
        "timestamp": datetime.utcnow(),
        "uptime_seconds": time.time() - startup_time
    }