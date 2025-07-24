# src/api/routes/platforms.py
"""
Platform management and information endpoints
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from api.dependencies import get_db_session, get_pagination_params, PaginationParams
from api.models import PlatformResponse, PaginatedResponse, PaginationResponse
from database.models import Platform, StreamingRecord, DataProcessingLog

router = APIRouter()


@router.get("", response_model=list[PlatformResponse])
async def get_platforms(
    active_only: bool = Query(True, description="Return only active platforms"),
    session: Session = Depends(get_db_session)
):
    """
    Get all streaming platforms
    
    Returns a list of all configured streaming platforms with their settings
    and processing configurations.
    """
    query = session.query(Platform)
    
    if active_only:
        query = query.filter(Platform.is_active == True)
    
    platforms = query.order_by(Platform.code).all()
    
    return [
        PlatformResponse(
            id=p.id,
            code=p.code,
            name=p.name,
            description=p.description,
            is_active=p.is_active,
            file_patterns=p.file_patterns,
            date_formats=p.date_formats,
            delimiter_type=p.delimiter_type,
            encoding=p.encoding,
            created_at=p.created_at,
            updated_at=p.updated_at
        )
        for p in platforms
    ]


@router.get("/{platform_code}", response_model=PlatformResponse)
async def get_platform(
    platform_code: str,
    session: Session = Depends(get_db_session)
):
    """
    Get specific platform by code
    
    Returns detailed information about a specific streaming platform
    including processing configuration and statistics.
    """
    platform = session.query(Platform).filter(Platform.code == platform_code).first()
    
    if not platform:
        raise HTTPException(
            status_code=404, 
            detail=f"Platform '{platform_code}' not found"
        )
    
    return PlatformResponse(
        id=platform.id,
        code=platform.code,
        name=platform.name,
        description=platform.description,
        is_active=platform.is_active,
        file_patterns=platform.file_patterns,
        date_formats=platform.date_formats,
        delimiter_type=platform.delimiter_type,
        encoding=platform.encoding,
        created_at=platform.created_at,
        updated_at=platform.updated_at
    )


@router.get("/{platform_code}/statistics")
async def get_platform_statistics(
    platform_code: str,
    session: Session = Depends(get_db_session)
):
    """
    Get processing statistics for a specific platform
    
    Returns detailed statistics about data processing for this platform
    including record counts, quality scores, and processing history.
    """
    platform = session.query(Platform).filter(Platform.code == platform_code).first()
    
    if not platform:
        raise HTTPException(
            status_code=404, 
            detail=f"Platform '{platform_code}' not found"
        )
    
    # Get streaming records statistics
    streaming_stats = session.query(
        func.count(StreamingRecord.id).label('total_records'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality_score'),
        func.min(StreamingRecord.date).label('earliest_date'),
        func.max(StreamingRecord.date).label('latest_date'),
        func.sum(StreamingRecord.metric_value).label('total_metric_value')
    ).filter(StreamingRecord.platform_id == platform.id).first()
    
    # Get processing logs statistics
    processing_stats = session.query(
        func.count(DataProcessingLog.id).label('total_files_processed'),
        func.count(
            DataProcessingLog.id.distinct()
        ).filter(
            DataProcessingLog.processing_status == 'completed'
        ).label('successful_files'),
        func.avg(DataProcessingLog.quality_score).label('avg_file_quality'),
        func.sum(DataProcessingLog.records_processed).label('total_records_processed'),
        func.sum(DataProcessingLog.records_failed).label('total_records_failed')
    ).filter(DataProcessingLog.platform_id == platform.id).first()
    
    return {
        "platform_code": platform.code,
        "platform_name": platform.name,
        "is_active": platform.is_active,
        "streaming_data": {
            "total_records": streaming_stats.total_records or 0,
            "average_quality_score": float(streaming_stats.avg_quality_score) if streaming_stats.avg_quality_score else 0.0,
            "date_range": {
                "earliest": streaming_stats.earliest_date.isoformat() if streaming_stats.earliest_date else None,
                "latest": streaming_stats.latest_date.isoformat() if streaming_stats.latest_date else None
            },
            "total_metric_value": float(streaming_stats.total_metric_value) if streaming_stats.total_metric_value else 0.0
        },
        "processing_history": {
            "total_files_processed": processing_stats.total_files_processed or 0,
            "successful_files": processing_stats.successful_files or 0,
            "average_file_quality": float(processing_stats.avg_file_quality) if processing_stats.avg_file_quality else 0.0,
            "total_records_processed": processing_stats.total_records_processed or 0,
            "total_records_failed": processing_stats.total_records_failed or 0,
            "success_rate": (
                (processing_stats.successful_files / processing_stats.total_files_processed * 100) 
                if processing_stats.total_files_processed and processing_stats.successful_files
                else 0.0
            )
        }
    }


@router.get("/{platform_code}/recent-activity")
async def get_platform_recent_activity(
    platform_code: str,
    limit: int = Query(10, ge=1, le=100, description="Number of recent activities to return"),
    session: Session = Depends(get_db_session)
):
    """
    Get recent processing activity for a platform
    
    Returns the most recent file processing activities for this platform
    including success/failure status and quality scores.
    """
    platform = session.query(Platform).filter(Platform.code == platform_code).first()
    
    if not platform:
        raise HTTPException(
            status_code=404, 
            detail=f"Platform '{platform_code}' not found"
        )
    
    recent_logs = session.query(DataProcessingLog).filter(
        DataProcessingLog.platform_id == platform.id
    ).order_by(
        DataProcessingLog.created_at.desc()
    ).limit(limit).all()
    
    activities = []
    for log in recent_logs:
        activities.append({
            "file_name": log.file_name,
            "processing_status": log.processing_status,
            "records_processed": log.records_processed,
            "records_failed": log.records_failed,
            "quality_score": float(log.quality_score) if log.quality_score else None,
            "started_at": log.started_at.isoformat(),
            "completed_at": log.completed_at.isoformat() if log.completed_at else None,
            "processing_duration_ms": log.processing_duration_ms,
            "error_message": log.error_message
        })
    
    return {
        "platform_code": platform.code,
        "platform_name": platform.name,
        "recent_activities": activities,
        "total_activities_shown": len(activities)
    }


@router.get("/{platform_code}/health")
async def get_platform_health(
    platform_code: str,
    session: Session = Depends(get_db_session)
):
    """
    Get health status for a specific platform
    
    Returns health indicators for this platform including recent processing
    success rates and data quality trends.
    """
    platform = session.query(Platform).filter(Platform.code == platform_code).first()
    
    if not platform:
        raise HTTPException(
            status_code=404, 
            detail=f"Platform '{platform_code}' not found"
        )
    
    # Get recent processing success rate (last 10 files)
    recent_logs = session.query(DataProcessingLog).filter(
        DataProcessingLog.platform_id == platform.id
    ).order_by(
        DataProcessingLog.created_at.desc()
    ).limit(10).all()
    
    if recent_logs:
        successful_recent = sum(1 for log in recent_logs if log.processing_status == 'completed')
        recent_success_rate = (successful_recent / len(recent_logs)) * 100
        recent_avg_quality = sum(
            float(log.quality_score) for log in recent_logs 
            if log.quality_score is not None
        ) / len([log for log in recent_logs if log.quality_score is not None]) if any(
            log.quality_score for log in recent_logs
        ) else 0.0
    else:
        recent_success_rate = 0.0
        recent_avg_quality = 0.0
    
    # Determine health status
    health_status = "healthy"
    health_issues = []
    
    if not platform.is_active:
        health_status = "inactive"
        health_issues.append("Platform is not active")
    elif recent_success_rate < 80:
        health_status = "degraded"
        health_issues.append(f"Recent success rate is {recent_success_rate:.1f}% (below 80%)")
    elif recent_avg_quality < 70:
        health_status = "warning"
        health_issues.append(f"Recent quality score is {recent_avg_quality:.1f} (below 70)")
    
    return {
        "platform_code": platform.code,
        "platform_name": platform.name,
        "health_status": health_status,
        "is_active": platform.is_active,
        "metrics": {
            "recent_success_rate": recent_success_rate,
            "recent_average_quality": recent_avg_quality,
            "recent_files_processed": len(recent_logs)
        },
        "issues": health_issues,
        "last_activity": recent_logs[0].created_at.isoformat() if recent_logs else None
    }