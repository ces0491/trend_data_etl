# src/api/routes/artists.py
"""
Artists management endpoints
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, desc

from api.dependencies import get_db_session, get_pagination_params, PaginationParams
from api.models import ArtistResponse, PaginatedResponse, PaginationResponse
from database.models import Artist, Track, StreamingRecord, Platform

router = APIRouter()


@router.get("", response_model=list[ArtistResponse])
async def get_artists(
    search: str | None = Query(None, description="Search term for artist names"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    session: Session = Depends(get_db_session)
):
    """
    Search for artists
    
    Returns a list of artists matching the search criteria.
    Results are ordered by name and limited to prevent large responses.
    """
    query = session.query(Artist)
    
    if search and search.strip():
        search_term = search.strip().lower()
        query = query.filter(
            or_(
                Artist.name_normalized.contains(search_term),
                Artist.name.ilike(f"%{search}%")
            )
        )
    
    artists = query.order_by(Artist.name).limit(limit).offset(offset).all()
    
    return [
        ArtistResponse(
            id=a.id,
            name=a.name,
            name_normalized=a.name_normalized,
            external_ids=a.external_ids,
            created_at=a.created_at,
            updated_at=a.updated_at
        ) for a in artists
    ]


@router.get("/paginated")
async def get_artists_paginated(
    search: str | None = Query(None, description="Search term for artist names"),
    pagination: PaginationParams = Depends(get_pagination_params),
    session: Session = Depends(get_db_session)
):
    """
    Get artists with pagination
    
    Returns paginated results with metadata for building UI pagination controls.
    """
    query = session.query(Artist)
    
    if search and search.strip():
        search_term = search.strip().lower()
        query = query.filter(
            or_(
                Artist.name_normalized.contains(search_term),
                Artist.name.ilike(f"%{search}%")
            )
        )
    
    # Get total count for pagination
    total_count = query.count()
    
    # Get paginated results
    artists = query.order_by(Artist.name).limit(pagination.limit).offset(pagination.offset).all()
    
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
    
    artist_data = [
        ArtistResponse(
            id=a.id,
            name=a.name,
            name_normalized=a.name_normalized,
            external_ids=a.external_ids,
            created_at=a.created_at,
            updated_at=a.updated_at
        ) for a in artists
    ]
    
    return PaginatedResponse(data=artist_data, pagination=pagination_meta)


@router.get("/{artist_id}", response_model=ArtistResponse)
async def get_artist(
    artist_id: int,
    session: Session = Depends(get_db_session)
):
    """
    Get specific artist by ID
    
    Returns detailed information about a specific artist including
    metadata and external platform identifiers.
    """
    artist = session.query(Artist).filter(Artist.id == artist_id).first()
    
    if not artist:
        raise HTTPException(
            status_code=404, 
            detail=f"Artist with ID {artist_id} not found"
        )
    
    return ArtistResponse(
        id=artist.id,
        name=artist.name,
        name_normalized=artist.name_normalized,
        external_ids=artist.external_ids,
        created_at=artist.created_at,
        updated_at=artist.updated_at
    )


@router.get("/{artist_id}/tracks")
async def get_artist_tracks(
    artist_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of tracks to return"),
    session: Session = Depends(get_db_session)
):
    """
    Get tracks for a specific artist
    
    Returns all tracks associated with this artist, ordered by title.
    """
    # Verify artist exists
    artist = session.query(Artist).filter(Artist.id == artist_id).first()
    if not artist:
        raise HTTPException(
            status_code=404, 
            detail=f"Artist with ID {artist_id} not found"
        )
    
    tracks = session.query(Track).filter(
        Track.artist_id == artist_id
    ).order_by(Track.title).limit(limit).all()
    
    return {
        "artist_id": artist_id,
        "artist_name": artist.name,
        "total_tracks": len(tracks),
        "tracks": [
            {
                "id": t.id,
                "title": t.title,
                "isrc": t.isrc,
                "album_name": t.album_name,
                "duration_ms": t.duration_ms,
                "genre": t.genre,
                "external_ids": t.external_ids,
                "created_at": t.created_at.isoformat(),
                "updated_at": t.updated_at.isoformat()
            } for t in tracks
        ]
    }


@router.get("/{artist_id}/statistics")
async def get_artist_statistics(
    artist_id: int,
    session: Session = Depends(get_db_session)
):
    """
    Get streaming statistics for a specific artist
    
    Returns aggregated streaming data across all platforms and tracks
    for this artist including total streams, top tracks, and platform breakdown.
    """
    # Verify artist exists
    artist = session.query(Artist).filter(Artist.id == artist_id).first()
    if not artist:
        raise HTTPException(
            status_code=404, 
            detail=f"Artist with ID {artist_id} not found"
        )
    
    # Get total streaming statistics
    streaming_stats = session.query(
        func.count(StreamingRecord.id).label('total_records'),
        func.sum(StreamingRecord.metric_value).label('total_streams'),
        func.avg(StreamingRecord.data_quality_score).label('avg_quality_score'),
        func.min(StreamingRecord.date).label('earliest_date'),
        func.max(StreamingRecord.date).label('latest_date')
    ).join(Track).filter(Track.artist_id == artist_id).first()
    
    # Get platform breakdown
    platform_stats = session.query(
        Platform.code,
        Platform.name,
        func.sum(StreamingRecord.metric_value).label('platform_streams'),
        func.count(StreamingRecord.id).label('platform_records')
    ).join(StreamingRecord).join(Track).filter(
        Track.artist_id == artist_id
    ).group_by(Platform.code, Platform.name).order_by(
        desc('platform_streams')
    ).all()
    
    # Get top tracks
    top_tracks = session.query(
        Track.id,
        Track.title,
        func.sum(StreamingRecord.metric_value).label('track_streams'),
        func.count(StreamingRecord.id).label('track_records')
    ).join(StreamingRecord).filter(
        Track.artist_id == artist_id
    ).group_by(Track.id, Track.title).order_by(
        desc('track_streams')
    ).limit(10).all()
    
    return {
        "artist_id": artist_id,
        "artist_name": artist.name,
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
        "top_tracks": [
            {
                "track_id": track.id,
                "track_title": track.title,
                "streams": float(track.track_streams) if track.track_streams else 0.0,
                "records": track.track_records or 0
            } for track in top_tracks
        ]
    }


@router.get("/{artist_id}/recent-activity")
async def get_artist_recent_activity(
    artist_id: int,
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of records to return"),
    session: Session = Depends(get_db_session)
):
    """
    Get recent streaming activity for an artist
    
    Returns the most recent streaming records for this artist
    across all platforms and tracks.
    """
    # Verify artist exists
    artist = session.query(Artist).filter(Artist.id == artist_id).first()
    if not artist:
        raise HTTPException(
            status_code=404, 
            detail=f"Artist with ID {artist_id} not found"
        )
    
    # Calculate date threshold
    from datetime import datetime, timedelta
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Get recent streaming records
    recent_records = session.query(
        StreamingRecord, Track, Platform
    ).join(Track, StreamingRecord.track_id == Track.id
    ).join(Platform, StreamingRecord.platform_id == Platform.id
    ).filter(
        Track.artist_id == artist_id,
        StreamingRecord.date >= date_threshold
    ).order_by(
        desc(StreamingRecord.date)
    ).limit(limit).all()
    
    activities = []
    for record, track, platform in recent_records:
        activities.append({
            "date": record.date.isoformat(),
            "platform_code": platform.code,
            "platform_name": platform.name,
            "track_title": track.title,
            "metric_type": record.metric_type,
            "metric_value": float(record.metric_value),
            "geography": record.geography,
            "device_type": record.device_type,
            "data_quality_score": float(record.data_quality_score) if record.data_quality_score else None
        })
    
    return {
        "artist_id": artist_id,
        "artist_name": artist.name,
        "time_period": {
            "days": days,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "recent_activities": activities,
        "total_activities_shown": len(activities)
    }