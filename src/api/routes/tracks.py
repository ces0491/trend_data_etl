# src/api/routes/tracks.py
"""
Tracks management endpoints
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, desc

from api.dependencies import get_db_session, get_pagination_params, PaginationParams
from api.models import TrackResponse, PaginatedResponse, PaginationResponse
from database.models import Track, Artist, StreamingRecord, Platform

router = APIRouter()


@router.get("", response_model=list[TrackResponse])
async def get_tracks(
    search: str | None = Query(None, description="Search term for track titles"),
    artist_search: str | None = Query(None, description="Search term for artist names"),
    isrc: str | None = Query(None, description="Filter by ISRC code"),
    genre: str | None = Query(None, description="Filter by genre"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    session: Session = Depends(get_db_session)
):
    """
    Search for tracks
    
    Returns a list of tracks matching the search criteria.
    Supports searching by track title, artist name, ISRC, or genre.
    """
    query = session.query(Track).join(Artist)
    
    # Apply filters
    if search and search.strip():
        search_term = search.strip().lower()
        query = query.filter(
            or_(
                Track.title_normalized.contains(search_term),
                Track.title.ilike(f"%{search}%")
            )
        )
    
    if artist_search and artist_search.strip():
        artist_term = artist_search.strip().lower()
        query = query.filter(
            or_(
                Artist.name_normalized.contains(artist_term),
                Artist.name.ilike(f"%{artist_search}%")
            )
        )
    
    if isrc and isrc.strip():
        query = query.filter(Track.isrc == isrc.strip().upper())
    
    if genre and genre.strip():
        query = query.filter(Track.genre.ilike(f"%{genre}%"))
    
    tracks = query.order_by(Track.title).limit(limit).offset(offset).all()
    
    return [
        TrackResponse(
            id=t.id,
            title=t.title,
            title_normalized=t.title_normalized,
            isrc=t.isrc,
            album_name=t.album_name,
            duration_ms=t.duration_ms,
            genre=t.genre,
            artist_id=t.artist_id,
            artist_name=t.artist.name,
            external_ids=t.external_ids,
            created_at=t.created_at,
            updated_at=t.updated_at
        ) for t in tracks
    ]


@router.get("/paginated")
async def get_tracks_paginated(
    search: str | None = Query(None, description="Search term for track titles"),
    artist_search: str | None = Query(None, description="Search term for artist names"),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: Session = Depends(get_db_session)
):
    """
    Get tracks with pagination
    
    Returns paginated results with metadata for building UI pagination controls.
    """
    query = session.query(Track).join(Artist)
    
    # Apply filters
    if search and search.strip():
        search_term = search.strip().lower()
        query = query.filter(
            or_(
                Track.title_normalized.contains(search_term),
                Track.title.ilike(f"%{search}%")
            )
        )
    
    if artist_search and artist_search.strip():
        artist_term = artist_search.strip().lower()
        query = query.filter(
            or_(
                Artist.name_normalized.contains(artist_term),
                Artist.name.ilike(f"%{artist_search}%")
            )
        )
    
    # Get total count
    total_count = query.count()
    
    # Get paginated results
    tracks = query.order_by(Track.title).limit(pagination.limit).offset(pagination.offset).all()
    
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
    
    track_data = [
        TrackResponse(
            id=t.id,
            title=t.title,
            title_normalized=t.title_normalized,
            isrc=t.isrc,
            album_name=t.album_name,
            duration_ms=t.duration_ms,
            genre=t.genre,
            artist_id=t.artist_id,
            artist_name=t.artist.name,
            external_ids=t.external_ids,
            created_at=t.created_at,
            updated_at=t.updated_at
        ) for t in tracks
    ]
    
    return PaginatedResponse(data=track_data, pagination=pagination_meta)


@router.get("/{track_id}", response_model=TrackResponse)
async def get_track(
    track_id: int,
    session: Session = Depends(get_db_session)
):
    """
    Get specific track by ID
    
    Returns detailed information about a specific track including
    artist information and metadata.
    """
    track = session.query(Track).join(Artist).filter(Track.id == track_id).first()
    
    if not track:
        raise HTTPException(
            status_code=404, 
            detail=f"Track with ID {track_id} not found"
        )
    
    return TrackResponse(
        id=track.id,
        title=track.title,
        title_normalized=track.title_normalized,
        isrc=track.isrc,
        album_name=track.album_name,
        duration_ms=track.duration_ms,
        genre=track.genre,
        artist_id=track.artist_id,
        artist_name=track.artist.name,
        external_ids=track.external_ids,
        created_at=track.created_at,
        updated_at=track.updated_at
    )


@router.get("/by-isrc/{isrc}", response_model=TrackResponse)
async def get_track_by_isrc(
    isrc: str,
    session: Session = Depends(get_db_session)
):
    """
    Get track by ISRC code
    
    ISRC (International Standard Recording Code) is a unique identifier
    for recordings. This endpoint allows lookup by ISRC.
    """
    isrc_upper = isrc.upper().strip()
    
    track = session.query(Track).join(Artist).filter(Track.isrc == isrc_upper).first()
    
    if not track:
        raise HTTPException(
            status_code=404, 
            detail=f"Track with ISRC '{isrc_upper}' not found"
        )
    
    return TrackResponse(
        id=track.id,
        title=track.title,
        title_normalized=track.title_normalized,
        isrc=track.isrc,
        album_name=track.album_name,
        duration_ms=track.duration_ms,
        genre=track.genre,
        artist_id=track.artist_id,
        artist_name=track.artist.name,
        external_ids=track.external_ids,
        created_at=track.created_at,
        updated_at=track.updated_at
    )


@router.get("/{track_id}/statistics")
async def get_track_statistics(
    track_id: int,
    session: Session = Depends(get_db_session)
):
    """
    Get streaming statistics for a specific track
    
    Returns aggregated streaming data across all platforms for this track
    including total streams, platform breakdown, and geographic distribution.
    """
    # Verify track exists
    track = session.query(Track).join(Artist).filter(Track.id == track_id).first()
    if not track:
        raise HTTPException(
            status_code=404, 
            detail=f"Track with ID {track_id} not found"
        )
    
    # Get total streaming statistics
    streaming_stats = session.query(
        func.count(StreamingRecord.id).label('total_records'),
        func.sum(StreamingRecord.metric_value).label('total_streams'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality_score'),
        func.min(StreamingRecord.date).label('earliest_date'),
        func.max(StreamingRecord.date).label('latest_date')
    ).filter(StreamingRecord.track_id == track_id).first()
    
    # Get platform breakdown
    platform_stats = session.query(
        Platform.code,
        Platform.name,
        func.sum(StreamingRecord.metric_value).label('platform_streams'),
        func.count(StreamingRecord.id).label('platform_records')
    ).join(StreamingRecord).filter(
        StreamingRecord.track_id == track_id
    ).group_by(Platform.code, Platform.name).order_by(
        desc('platform_streams')
    ).all()
    
    # Get geographic breakdown
    geographic_stats = session.query(
        StreamingRecord.geography,
        func.sum(StreamingRecord.metric_value).label('geo_streams'),
        func.count(StreamingRecord.id).label('geo_records')
    ).filter(
        StreamingRecord.track_id == track_id,
        StreamingRecord.geography.isnot(None)
    ).group_by(StreamingRecord.geography).order_by(
        desc('geo_streams')
    ).limit(10).all()
    
    # Get device type breakdown
    device_stats = session.query(
        StreamingRecord.device_type,
        func.sum(StreamingRecord.metric_value).label('device_streams'),
        func.count(StreamingRecord.id).label('device_records')
    ).filter(
        StreamingRecord.track_id == track_id,
        StreamingRecord.device_type.isnot(None)
    ).group_by(StreamingRecord.device_type).order_by(
        desc('device_streams')
    ).all()
    
    return {
        "track_id": track_id,
        "track_title": track.title,
        "artist_name": track.artist.name,
        "isrc": track.isrc,
        "overall_statistics": {
            "total_streaming_records": streaming_stats.total_records or 0,
            "total_streams": float(streaming_stats.total_streams) if streaming_stats.total_streams else 0.0,
            "average_quality_score": float(streaming_stats.avg_quality_score) if streaming_stats.avg_quality_score else 0.0,
            "date_range": {
                "earliest": streaming_stats.earliest_date.isoformat() if streaming_stats.earliest_date else None,
                "latest": streaming_stats.latest_date.isoformat() if streaming_stats.latest_date else None
            }
        },
        "platform_breakdown": [
            {
                "platform_code": stat.code,
                "platform_name": stat.name,
                "streams": float(stat.platform_streams) if stat.platform_streams else 0.0,
                "records": stat.platform_records or 0
            } for stat in platform_stats
        ],
        "geographic_breakdown": [
            {
                "geography": stat.geography,
                "streams": float(stat.geo_streams) if stat.geo_streams else 0.0,
                "records": stat.geo_records or 0
            } for stat in geographic_stats
        ],
        "device_breakdown": [
            {
                "device_type": stat.device_type,
                "streams": float(stat.device_streams) if stat.device_streams else 0.0,
                "records": stat.device_records or 0
            } for stat in device_stats
        ]
    }


@router.get("/{track_id}/recent-activity")
async def get_track_recent_activity(
    track_id: int,
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    session: Session = Depends(get_db_session)
):
    """
    Get recent streaming activity for a track
    
    Returns the most recent streaming records for this track
    across all platforms with geographic and device breakdowns.
    """
    # Verify track exists
    track = session.query(Track).join(Artist).filter(Track.id == track_id).first()
    if not track:
        raise HTTPException(
            status_code=404, 
            detail=f"Track with ID {track_id} not found"
        )
    
    # Calculate date threshold
    from datetime import datetime, timedelta
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Get recent streaming records
    recent_records = session.query(
        StreamingRecord, Platform
    ).join(Platform, StreamingRecord.platform_id == Platform.id
    ).filter(
        StreamingRecord.track_id == track_id,
        StreamingRecord.date >= date_threshold
    ).order_by(
        desc(StreamingRecord.date)
    ).limit(limit).all()
    
    activities = []
    for record, platform in recent_records:
        activities.append({
            "date": record.date.isoformat(),
            "platform_code": platform.code,
            "platform_name": platform.name,
            "metric_type": record.metric_type,
            "metric_value": float(record.metric_value),
            "geography": record.geography,
            "device_type": record.device_type,
            "subscription_type": record.subscription_type,
            "context_type": record.context_type,
            "data_quality_score": float(record.data_quality_score) if record.data_quality_score else None
        })
    
    return {
        "track_id": track_id,
        "track_title": track.title,
        "artist_name": track.artist.name,
        "time_period": {
            "days": days,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "recent_activities": activities,
        "total_activities_shown": len(activities)
    }


@router.get("/{track_id}/trends")
async def get_track_trends(
    track_id: int,
    days: int = Query(90, ge=7, le=365, description="Number of days for trend analysis"),
    aggregation: str = Query("daily", regex="^(daily|weekly|monthly)$", description="Aggregation period"),
    session: Session = Depends(get_db_session)
):
    """
    Get streaming trends for a track over time
    
    Returns time-series data showing how streaming activity for this track
    has changed over the specified period, aggregated by day, week, or month.
    """
    # Verify track exists
    track = session.query(Track).join(Artist).filter(Track.id == track_id).first()
    if not track:
        raise HTTPException(
            status_code=404, 
            detail=f"Track with ID {track_id} not found"
        )
    
    # Calculate date threshold
    from datetime import datetime, timedelta
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build query based on aggregation period
    if aggregation == "daily":
        date_trunc = func.date(StreamingRecord.date)
    elif aggregation == "weekly":
        date_trunc = func.date_trunc('week', StreamingRecord.date)
    else:  # monthly
        date_trunc = func.date_trunc('month', StreamingRecord.date)
    
    # Get trend data
    trend_data = session.query(
        date_trunc.label('period'),
        func.sum(StreamingRecord.metric_value).label('total_streams'),
        func.count(StreamingRecord.id).label('total_records'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality')
    ).filter(
        StreamingRecord.track_id == track_id,
        StreamingRecord.date >= date_threshold
    ).group_by(date_trunc).order_by(date_trunc).all()
    
    trends = []
    for period, streams, records, quality in trend_data:
        trends.append({
            "period": period.isoformat() if period else None,
            "total_streams": float(streams) if streams else 0.0,
            "total_records": records or 0,
            "average_quality_score": float(quality) if quality else None
        })
    
    return {
        "track_id": track_id,
        "track_title": track.title,
        "artist_name": track.artist.name,
        "analysis_period": {
            "days": days,
            "aggregation": aggregation,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "trend_data": trends,
        "data_points": len(trends)
    }