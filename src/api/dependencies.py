# src/api/dependencies.py
"""
FastAPI dependencies for database connections and common functionality
"""
from __future__ import annotations

import os
from typing import Generator
from fastapi import Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database.models import DatabaseManager


# Global database manager (will be set by main.py)
_db_manager: DatabaseManager | None = None


def set_db_manager(db_manager: DatabaseManager) -> None:
    """Set the global database manager"""
    global _db_manager
    _db_manager = db_manager


def get_db_manager() -> DatabaseManager:
    """Get the database manager dependency"""
    if _db_manager is None:
        raise HTTPException(
            status_code=503, 
            detail="Database manager not initialized"
        )
    return _db_manager


def get_db_session(db_manager: DatabaseManager = Depends(get_db_manager)) -> Generator[Session, None, None]:
    """Get database session dependency"""
    with db_manager.get_session() as session:
        try:
            yield session
        except Exception:
            session.rollback()
            raise


class PaginationParams:
    """Pagination parameters dependency"""
    
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number (1-based)"),
        page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    ):
        self.page = page
        self.page_size = page_size
        self.offset = (page - 1) * page_size
        self.limit = page_size


def get_pagination_params(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
) -> PaginationParams:
    """Get pagination parameters"""
    return PaginationParams(page=page, page_size=page_size)


class QualityFilter:
    """Quality filter parameters"""
    
    def __init__(
        self,
        min_quality_score: float | None = Query(
            None, 
            ge=0, 
            le=100, 
            description="Minimum quality score (0-100)"
        ),
        quality_threshold: float = Query(
            float(os.getenv('QUALITY_THRESHOLD', '90')),
            ge=0,
            le=100,
            description="Quality threshold for filtering"
        )
    ):
        self.min_quality_score = min_quality_score
        self.quality_threshold = quality_threshold


def get_quality_filter(
    min_quality_score: float | None = Query(
        None, 
        ge=0, 
        le=100, 
        description="Minimum quality score (0-100)"
    ),
    quality_threshold: float = Query(
        float(os.getenv('QUALITY_THRESHOLD', '90')),
        ge=0,
        le=100,
        description="Quality threshold for filtering"
    )
) -> QualityFilter:
    """Get quality filter parameters"""
    return QualityFilter(
        min_quality_score=min_quality_score,
        quality_threshold=quality_threshold
    )


def get_current_user():
    """
    Get current user (placeholder for future authentication)
    Currently returns None as authentication is not implemented
    """
    # TODO: Implement authentication when needed
    return None


def require_admin():
    """
    Require admin privileges (placeholder for future authorization)
    Currently allows all requests as authorization is not implemented
    """
    # TODO: Implement authorization when needed
    return True