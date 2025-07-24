# src/api/models.py
"""
Pydantic models for API request/response schemas
SEPARATE from database models to fix import issues
"""

from datetime import datetime, date
from typing import Any, Optional, List
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class ProcessingStatus(str, Enum):
    """Processing status enum"""
    PENDING = "pending"
    PROCESSING = "processing"  
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class MetricType(str, Enum):
    """Metric type enum"""
    STREAMS = "streams"
    PLAYS = "plays"
    SAVES = "saves"
    SHARES = "shares"
    VIDEO_VIEWS = "video_views"
    SOCIAL_INTERACTIONS = "social_interactions"
    FITNESS_PLAYS = "fitness_plays"


class DeviceType(str, Enum):
    """Device type enum"""
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    TV = "tv"
    UNKNOWN = "unknown"


class SubscriptionType(str, Enum):
    """Subscription type enum"""
    FREE = "free"
    PAID = "paid"
    TRIAL = "trial"
    UNKNOWN = "unknown"


# Response Models
class PlatformResponse(BaseModel):
    """Platform information response"""
    id: int
    code: str
    name: str
    description: Optional[str] = None
    is_active: bool
    file_patterns: Optional[List[str]] = None
    date_formats: Optional[List[str]] = None
    delimiter_type: Optional[str] = None
    encoding: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ArtistResponse(BaseModel):
    """Artist information response"""
    id: int
    name: str
    name_normalized: str
    external_ids: Optional[dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TrackResponse(BaseModel):
    """Track information response"""
    id: int
    title: str
    title_normalized: str
    isrc: Optional[str] = None
    album_name: Optional[str] = None
    duration_ms: Optional[int] = None
    genre: Optional[str] = None
    artist_id: int
    artist_name: Optional[str] = None
    external_ids: Optional[dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StreamingRecordResponse(BaseModel):
    """Streaming record response"""
    id: str
    date: date
    platform_code: str
    platform_name: str
    track_id: Optional[int] = None
    track_title: Optional[str] = None
    artist_name: Optional[str] = None
    metric_type: MetricType
    metric_value: float
    geography: Optional[str] = None
    device_type: Optional[DeviceType] = None
    subscription_type: Optional[SubscriptionType] = None
    context_type: Optional[str] = None
    user_demographic: Optional[dict[str, Any]] = None
    data_quality_score: Optional[float] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class QualitySummaryResponse(BaseModel):
    """Data quality summary response"""
    total_files_processed: int
    average_quality_score: float
    files_above_threshold: int
    quality_threshold: float
    platforms_analyzed: int
    total_records_processed: int
    last_updated: datetime


class QualityDetailResponse(BaseModel):
    """Detailed quality information"""
    id: int
    platform_code: str
    platform_name: str
    file_path: Optional[str] = None
    overall_score: float
    completeness_score: Optional[float] = None
    consistency_score: Optional[float] = None
    validity_score: Optional[float] = None
    accuracy_score: Optional[float] = None
    issues_found: Optional[List[dict[str, Any]]] = None
    recommendations: Optional[List[str]] = None
    measured_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProcessingLogResponse(BaseModel):
    """Processing log response"""
    id: int
    file_name: str
    file_path: str
    file_size: Optional[int] = None
    platform_code: str
    platform_name: str
    processing_status: ProcessingStatus
    records_processed: int
    records_failed: int
    records_skipped: int
    quality_score: Optional[float] = None
    error_message: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None
    processing_duration_ms: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    database_status: str
    platforms_configured: int
    api_version: str
    uptime_seconds: Optional[float] = None


class MetricsResponse(BaseModel):
    """Time-series metrics response"""
    platform: str
    metric_type: MetricType
    time_range: dict[str, date]
    data_points: List[dict[str, Any]]
    total_records: int
    aggregation_method: str


class PaginationResponse(BaseModel):
    """Pagination metadata"""
    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_previous: bool


class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper"""
    data: List[Any]
    pagination: PaginationResponse


# Error Models
class APIError(BaseModel):
    """API error response"""
    error: str
    status_code: int
    path: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[str] = None


class ValidationError(BaseModel):
    """Validation error details"""
    field: str
    message: str
    value: Optional[Any] = None


class ValidationErrorResponse(APIError):
    """Validation error response with field details"""
    validation_errors: List[ValidationError]


# Request Models
class StreamingRecordFilter(BaseModel):
    """Filters for streaming records query"""
    platform: Optional[str] = None
    artist_name: Optional[str] = None
    track_title: Optional[str] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    geography: Optional[str] = None
    metric_type: Optional[MetricType] = None
    device_type: Optional[DeviceType] = None
    subscription_type: Optional[SubscriptionType] = None
    min_quality_score: Optional[float] = Field(None, ge=0, le=100)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "platform": "spo-spotify",
                "artist_name": "Taylor Swift",
                "date_from": "2024-01-01",
                "date_to": "2024-12-31",
                "min_quality_score": 80
            }
        }
    )


class TimeSeriesRequest(BaseModel):
    """Time-series analytics request"""
    platforms: Optional[List[str]] = None
    metric_types: Optional[List[MetricType]] = None
    date_from: date
    date_to: date
    aggregation: str = Field("daily", pattern=r"^(daily|weekly|monthly)$")
    geography: Optional[str] = None
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "platforms": ["spo-spotify", "apl-apple"],
                "metric_types": ["streams", "plays"],
                "date_from": "2024-01-01",
                "date_to": "2024-12-31",
                "aggregation": "monthly"
            }
        }
    )