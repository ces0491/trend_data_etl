# src/api/routes/streaming_records.py
"""
Streaming records data access endpoints
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, desc

from api.dependencies import get_db_session, get_pagination_params, PaginationParams
from api.models import (
    StreamingRecordResponse, PaginatedResponse, PaginationResponse,
    MetricsResponse, MetricType, DeviceType, SubscriptionType
)
from database.models import StreamingRecord, Track, Artist, Platform

router = APIRouter()


@router.get("", response_model=list[StreamingRecordResponse])
async def get_streaming_records(
    platform: str | None = Query(None, description="Platform code filter"),
    artist_name: str | None = Query(None, description="Artist name filter (partial match)"),
    track_title: str | None = Query(None, description="Track title filter (partial match)"),
    date_from: date | None = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: date | None = Query(None, description="End date filter (YYYY-MM-DD)"),
    geography: str | None = Query(None, description="Geography/country filter"),
    metric_type: MetricType | None = Query(None, description="Metric type filter"),
    device_type: DeviceType | None = Query(None, description="Device type filter"),
    subscription_type: SubscriptionType | None = Query(None, description="Subscription type filter"),
    min_quality_score: float | None = Query(None, ge=0, le=100, description="Minimum quality score"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    session: Session = Depends(get_db_session)
):
    """
    Get streaming records with comprehensive filtering
    
    Returns streaming records matching the specified filters.
    Results are ordered by date (newest first) and include related
    platform, track, and artist information.
    """
    # Build query with joins
    query = session.query(StreamingRecord).join(Platform).outerjoin(Track).outerjoin(Artist)
    
    # Apply filters
    if platform:
        query = query.filter(Platform.code == platform)
    
    if artist_name and artist_name.strip():
        artist_term = artist_name.strip()
        # Search in both Artist table and StreamingRecord artist_name field
        query = query.filter(
            or_(
                Artist.name.ilike(f"%{artist_term}%"),
                StreamingRecord.artist_name.ilike(f"%{artist_term}%")
            )
        )
    
    if track_title and track_title.strip():
        track_term = track_title.strip()
        # Search in both Track table and StreamingRecord track_title field
        query = query.filter(
            or_(
                Track.title.ilike(f"%{track_term}%"),
                StreamingRecord.track_title.ilike(f"%{track_term}%")
            )
        )
    
    if date_from:
        query = query.filter(StreamingRecord.date >= date_from)
    
    if date_to:
        query = query.filter(StreamingRecord.date <= date_to)
    
    if geography:
        query = query.filter(StreamingRecord.geography == geography)
    
    if metric_type:
        query = query.filter(StreamingRecord.metric_type == metric_type.value)
    
    if device_type:
        query = query.filter(StreamingRecord.device_type == device_type.value)
    
    if subscription_type:
        query = query.filter(StreamingRecord.subscription_type == subscription_type.value)
    
    if min_quality_score is not None:
        query = query.filter(StreamingRecord.data_quality_score >= min_quality_score)
    
    # Execute query with ordering and limits
    records = query.order_by(desc(StreamingRecord.date)).limit(limit).offset(offset).all()
    
    # Build response
    response_data = []
    for record in records:
        # Get artist name from either Artist table or StreamingRecord field
        artist_name_val = None
        if record.artist_name:
            artist_name_val = record.artist_name
        elif record.track and record.track.artist:
            artist_name_val = record.track.artist.name
        
        # Get track title from either Track table or StreamingRecord field
        track_title_val = None
        if record.track_title:
            track_title_val = record.track_title
        elif record.track:
            track_title_val = record.track.title
        
        response_data.append(StreamingRecordResponse(
            id=str(record.id),
            date=record.date.date(),
            platform_code=record.platform.code,
            platform_name=record.platform.name,
            track_id=record.track_id,
            track_title=track_title_val,
            artist_name=artist_name_val,
            metric_type=MetricType(record.metric_type),
            metric_value=float(record.metric_value),
            geography=record.geography,
            device_type=DeviceType(record.device_type) if record.device_type else None,
            subscription_type=SubscriptionType(record.subscription_type) if record.subscription_type else None,
            context_type=record.context_type,
            user_demographic=record.user_demographic,
            data_quality_score=float(record.data_quality_score) if record.data_quality_score else None,
            created_at=record.created_at
        ))
    
    return response_data


@router.get("/paginated")
async def get_streaming_records_paginated(
    platform: str | None = Query(None, description="Platform code filter"),
    artist_name: str | None = Query(None, description="Artist name filter"),
    date_from: date | None = Query(None, description="Start date filter"),
    date_to: date | None = Query(None, description="End date filter"),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: Session = Depends(get_db_session)
):
    """
    Get streaming records with pagination
    
    Returns paginated streaming records with metadata for building
    UI pagination controls. Includes total count and page information.
    """
    # Build base query
    query = session.query(StreamingRecord).join(Platform).outerjoin(Track).outerjoin(Artist)
    
    # Apply filters (simplified for pagination example)
    if platform:
        query = query.filter(Platform.code == platform)
    
    if artist_name and artist_name.strip():
        artist_term = artist_name.strip()
        query = query.filter(
            or_(
                Artist.name.ilike(f"%{artist_term}%"),
                StreamingRecord.artist_name.ilike(f"%{artist_term}%")
            )
        )
    
    if date_from:
        query = query.filter(StreamingRecord.date >= date_from)
    
    if date_to:
        query = query.filter(StreamingRecord.date <= date_to)
    
    # Get total count
    total_count = query.count()
    
    # Get paginated results
    records = query.order_by(desc(StreamingRecord.date)).limit(pagination.limit).offset(pagination.offset).all()
    
    # Build response data (same logic as above)
    response_data = []
    for record in records:
        artist_name_val = record.artist_name or (record.track.artist.name if record.track and record.track.artist else None)
        track_title_val = record.track_title or (record.track.title if record.track else None)
        
        response_data.append(StreamingRecordResponse(
            id=str(record.id),
            date=record.date.date(),
            platform_code=record.platform.code,
            platform_name=record.platform.name,
            track_id=record.track_id,
            track_title=track_title_val,
            artist_name=artist_name_val,
            metric_type=MetricType(record.metric_type),
            metric_value=float(record.metric_value),
            geography=record.geography,
            device_type=DeviceType(record.device_type) if record.device_type else None,
            subscription_type=SubscriptionType(record.subscription_type) if record.subscription_type else None,
            context_type=record.context_type,
            user_demographic=record.user_demographic,
            data_quality_score=float(record.data_quality_score) if record.data_quality_score else None,
            created_at=record.created_at
        ))
    
    # Calculate pagination metadata
    total_pages = (total_count + pagination.page_size - 1) // pagination.page_size
    
    pagination_meta = PaginationResponse(
        page=pagination.page,
        page_size=pagination.page_size,
        total_items=total_count,
        total_pages=total_pages,
        has_next=pagination.page < total_pages,
        has_previous=pagination.page > 1
    )
    
    return PaginatedResponse(data=response_data, pagination=pagination_meta)


@router.get("/time-series")
async def get_time_series_data(
    platforms: list[str] = Query(None, description="Platform codes to include"),
    metric_types: list[MetricType] = Query(None, description="Metric types to include"),
    date_from: date = Query(..., description="Start date for analysis"),
    date_to: date = Query(..., description="End date for analysis"),
    aggregation: str = Query("daily", regex="^(daily|weekly|monthly)$", description="Time aggregation"),
    geography: str | None = Query(None, description="Geography filter"),
    session: Session = Depends(get_db_session)
):
    """
    Get time-series analytics data
    
    Returns aggregated streaming data over time with the specified
    granularity. Useful for building charts and trend analysis.
    """
    # Validate date range
    if date_from >= date_to:
        raise HTTPException(status_code=400, detail="date_from must be before date_to")
    
    if (date_to - date_from).days > 365:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 365 days")
    
    # Build base query
    query = session.query(StreamingRecord).join(Platform)
    
    # Apply filters
    if platforms:
        query = query.filter(Platform.code.in_(platforms))
    
    if metric_types:
        metric_values = [mt.value for mt in metric_types]
        query = query.filter(StreamingRecord.metric_type.in_(metric_values))
    
    if geography:
        query = query.filter(StreamingRecord.geography == geography)
    
    query = query.filter(
        and_(
            StreamingRecord.date >= date_from,
            StreamingRecord.date <= date_to
        )
    )
    
    # Build aggregation query based on period
    if aggregation == "daily":
        date_trunc = func.date(StreamingRecord.date)
    elif aggregation == "weekly":
        date_trunc = func.date_trunc('week', StreamingRecord.date)
    else:  # monthly
        date_trunc = func.date_trunc('month', StreamingRecord.date)
    
    # Get aggregated results
    results = query.with_entities(
        date_trunc.label('period'),
        Platform.code.label('platform_code'),
        StreamingRecord.metric_type,
        func.sum(StreamingRecord.metric_value).label('total_value'),
        func.count(StreamingRecord.id).label('record_count'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality')
    ).group_by(
        date_trunc, Platform.code, StreamingRecord.metric_type
    ).order_by(date_trunc).all()
    
    # Group results by platform and metric type
    data_points = []
    for result in results:
        data_points.append({
            "period": result.period.isoformat() if result.period else None,
            "platform_code": result.platform_code,
            "metric_type": result.metric_type,
            "total_value": float(result.total_value) if result.total_value else 0.0,
            "record_count": result.record_count or 0,
            "average_quality_score": float(result.avg_quality) if result.avg_quality else None
        })
    
    return {
        "time_range": {
            "from_date": date_from.isoformat(),
            "to_date": date_to.isoformat()
        },
        "aggregation_method": aggregation,
        "filters_applied": {
            "platforms": platforms or [],
            "metric_types": [mt.value for mt in metric_types] if metric_types else [],
            "geography": geography
        },
        "total_data_points": len(data_points),
        "data_points": data_points
    }


@router.get("/summary")
async def get_streaming_summary(
    date_from: date | None = Query(None, description="Start date for summary"),
    date_to: date | None = Query(None, description="End date for summary"),
    session: Session = Depends(get_db_session)
):
    """
    Get high-level streaming data summary
    
    Returns aggregate statistics across all platforms and time periods.
    Useful for dashboard overviews and quick insights.
    """
    # Default to last 30 days if no dates provided
    if not date_from:
        date_from = datetime.utcnow().date() - timedelta(days=30)
    if not date_to:
        date_to = datetime.utcnow().date()
    
    # Build base query
    query = session.query(StreamingRecord).join(Platform)
    
    if date_from and date_to:
        query = query.filter(
            and_(
                StreamingRecord.date >= date_from,
                StreamingRecord.date <= date_to
            )
        )
    
    # Overall statistics
    overall_stats = query.with_entities(
        func.count(StreamingRecord.id).label('total_records'),
        func.sum(StreamingRecord.metric_value).label('total_streams'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality'),
        func.count(func.distinct(StreamingRecord.artist_name)).label('unique_artists'),
        func.count(func.distinct(StreamingRecord.track_title)).label('unique_tracks')
    ).first()
    
    # Platform breakdown
    platform_breakdown = query.with_entities(
        Platform.code,
        Platform.name,
        func.sum(StreamingRecord.metric_value).label('platform_streams'),
        func.count(StreamingRecord.id).label('platform_records')
    ).group_by(Platform.code, Platform.name).order_by(
        desc('platform_streams')
    ).all()
    
    # Geographic breakdown
    geographic_breakdown = query.filter(
        StreamingRecord.geography.isnot(None)
    ).with_entities(
        StreamingRecord.geography,
        func.sum(StreamingRecord.metric_value).label('geo_streams'),
        func.count(StreamingRecord.id).label('geo_records')
    ).group_by(StreamingRecord.geography).order_by(
        desc('geo_streams')
    ).limit(10).all()
    
    # Device breakdown
    device_breakdown = query.filter(
        StreamingRecord.device_type.isnot(None)
    ).with_entities(
        StreamingRecord.device_type,
        func.sum(StreamingRecord.metric_value).label('device_streams'),
        func.count(StreamingRecord.id).label('device_records')
    ).group_by(StreamingRecord.device_type).order_by(
        desc('device_streams')
    ).all()
    
    return {
        "summary_period": {
            "from_date": date_from.isoformat(),
            "to_date": date_to.isoformat(),
            "days": (date_to - date_from).days
        },
        "overall_statistics": {
            "total_records": overall_stats.total_records or 0,
            "total_streams": float(overall_stats.total_streams) if overall_stats.total_streams else 0.0,
            "average_quality_score": float(overall_stats.avg_quality) if overall_stats.avg_quality else 0.0,
            "unique_artists": overall_stats.unique_artists or 0,
            "unique_tracks": overall_stats.unique_tracks or 0
        },
        "platform_breakdown": [
            {
                "platform_code": p.code,
                "platform_name": p.name,
                "streams": float(p.platform_streams) if p.platform_streams else 0.0,
                "records": p.platform_records or 0,
                "percentage": (float(p.platform_streams) / float(overall_stats.total_streams) * 100) if overall_stats.total_streams else 0.0
            } for p in platform_breakdown
        ],
        "geographic_breakdown": [
            {
                "geography": g.geography,
                "streams": float(g.geo_streams) if g.geo_streams else 0.0,
                "records": g.geo_records or 0
            } for g in geographic_breakdown
        ],
        "device_breakdown": [
            {
                "device_type": d.device_type,
                "streams": float(d.device_streams) if d.device_streams else 0.0,
                "records": d.device_records or 0
            } for d in device_breakdown
        ]
    }


@router.get("/{record_id}")
async def get_streaming_record(
    record_id: str,
    session: Session = Depends(get_db_session)
):
    """
    Get specific streaming record by ID
    
    Returns detailed information about a single streaming record
    including all metadata and related platform/track/artist information.
    """
    record = session.query(StreamingRecord).join(Platform).outerjoin(Track).outerjoin(Artist).filter(
        StreamingRecord.id == record_id
    ).first()
    
    if not record:
        raise HTTPException(
            status_code=404, 
            detail=f"Streaming record with ID '{record_id}' not found"
        )
    
    # Get artist name from either source
    artist_name_val = record.artist_name or (record.track.artist.name if record.track and record.track.artist else None)
    track_title_val = record.track_title or (record.track.title if record.track else None)
    
    return {
        "id": str(record.id),
        "date": record.date.isoformat(),
        "platform": {
            "code": record.platform.code,
            "name": record.platform.name
        },
        "track": {
            "id": record.track_id,
            "title": track_title_val,
            "isrc": record.track.isrc if record.track else None,
            "album_name": record.track.album_name if record.track else None
        } if record.track or track_title_val else None,
        "artist": {
            "id": record.track.artist_id if record.track else None,
            "name": artist_name_val
        } if artist_name_val else None,
        "metrics": {
            "type": record.metric_type,
            "value": float(record.metric_value)
        },
        "dimensions": {
            "geography": record.geography,
            "device_type": record.device_type,
            "subscription_type": record.subscription_type,
            "context_type": record.context_type,
            "user_demographic": record.user_demographic
        },
        "metadata": {
            "data_quality_score": float(record.data_quality_score) if record.data_quality_score else None,
            "raw_data_source": record.raw_data_source,
            "file_hash": record.file_hash,
            "processing_timestamp": record.processing_timestamp.isoformat() if record.processing_timestamp else None,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat()
        }
    }