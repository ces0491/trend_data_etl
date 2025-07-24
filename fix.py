# scripts/windows_fix_script.py
"""
Windows-compatible script to apply critical fixes
Handles encoding issues and path setup properly
"""

import sys
import os
from pathlib import Path

def setup_python_path():
    """Setup Python path correctly for Windows"""
    # Get absolute path to src directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    src_dir = project_root / "src"
    
    # Add to Python path if it exists
    if src_dir.exists():
        src_path_str = str(src_dir.absolute())
        if src_path_str not in sys.path:
            sys.path.insert(0, src_path_str)
        print(f"✅ Added to Python path: {src_path_str}")
        return True, src_dir
    else:
        print(f"❌ Source directory not found: {src_dir}")
        return False, src_dir

def create_api_models_file():
    """Create the API models file with proper content"""
    print("🔧 Creating src/api/models.py...")
    
    api_models_content = '''# src/api/models.py
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


class TimeSeriesRequest(BaseModel):
    """Time-series analytics request"""
    platforms: Optional[List[str]] = None
    metric_types: Optional[List[MetricType]] = None
    date_from: date
    date_to: date
    aggregation: str = Field("daily", pattern=r"^(daily|weekly|monthly)$")
    geography: Optional[str] = None
'''
    
    try:
        # Ensure directories exist
        api_dir = Path("src/api")
        api_dir.mkdir(parents=True, exist_ok=True)
        
        # Create the API models file
        api_models_file = api_dir / "models.py"
        with open(api_models_file, 'w', encoding='utf-8') as f:
            f.write(api_models_content)
        
        print(f"✅ Created: {api_models_file}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create API models file: {e}")
        return False

def fix_database_models_encoding():
    """Fix the database models file encoding issues"""
    print("🔧 Fixing database models file encoding...")
    
    db_models_path = Path("src/database/models.py")
    
    if not db_models_path.exists():
        print(f"❌ Database models file not found: {db_models_path}")
        return False
    
    try:
        # Try to read with different encodings
        content = None
        for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
            try:
                with open(db_models_path, 'r', encoding=encoding) as f:
                    content = f.read()
                print(f"✅ Successfully read file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            print("❌ Could not read database models file with any encoding")
            return False
        
        # Check if file has API models mixed in (the problem)
        if "from pydantic import BaseModel" in content:
            print("⚠️  Database models file contains API models - this needs to be cleaned")
            print("   You need to replace the entire file with the cleaned version")
            return False
        
        # Write back with UTF-8 encoding to fix any encoding issues
        with open(db_models_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ Database models file encoding fixed")
        return True
        
    except Exception as e:
        print(f"❌ Error fixing database models file: {e}")
        return False

def create_missing_files():
    """Create all missing required files"""
    print("🔧 Creating missing required files...")
    
    # Ensure all __init__.py files exist
    required_dirs = [
        "src",
        "src/api",
        "src/api/routes", 
        "src/database",
        "src/etl",
        "src/etl/parsers",
        "src/etl/validators"
    ]
    
    created_dirs = []
    for dir_path in required_dirs:
        full_dir = Path(dir_path)
        full_dir.mkdir(parents=True, exist_ok=True)
        
        init_file = full_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            created_dirs.append(str(init_file))
    
    if created_dirs:
        print(f"✅ Created {len(created_dirs)} __init__.py files")
    
    return True

def test_imports_safely():
    """Test imports with better error handling"""
    print("🔍 Testing imports...")
    
    try:
        # Test if API models file was created correctly
        api_models_file = Path("src/api/models.py")
        if not api_models_file.exists():
            print("❌ API models file doesn't exist")
            return False
        
        # Test basic import
        from api.models import PlatformResponse
        print("✅ API models import successfully")
        
        # Test database models
        from database.models import Platform, DatabaseManager
        print("✅ Database models import successfully")  
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        print("   This might be due to missing files or path issues")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during import test: {e}")
        return False

def show_manual_fixes():
    """Show what needs to be done manually"""
    print("\n🛠️  MANUAL FIXES NEEDED:")
    print("=" * 50)
    print("1. REPLACE src/database/models.py:")
    print("   - Your current file has encoding issues and mixed API/DB models")
    print("   - Replace the ENTIRE file with the cleaned SQLAlchemy-only version")
    print("   - I provided this in the previous message")
    print()
    print("2. MISSING ROUTE FILES:")
    print("   Create these files with the provided route code:")
    
    route_files = [
        "src/api/routes/artists.py",
        "src/api/routes/tracks.py", 
        "src/api/routes/streaming_records.py",
        "src/api/routes/data_quality.py"
    ]
    
    for route_file in route_files:
        if not Path(route_file).exists():
            print(f"   - {route_file}")
    
    print()
    print("3. UPDATE src/api/main.py:")
    print("   - Add imports for new routes")
    print("   - Add set_db_manager(db_manager) call")
    print("   - Include new route modules")
    print()
    print("4. AFTER FIXES:")
    print("   python scripts/validate_setup.py")
    print("   uvicorn src.api.main:app --reload")

def main():
    """Main function with Windows-specific handling"""
    print("WINDOWS-COMPATIBLE CRITICAL FIXES")
    print("=" * 40)
    
    # Setup Python path
    path_success, src_dir = setup_python_path()
    if not path_success:
        print("❌ Cannot set up Python path")
        return False
    
    print()
    
    # Create missing files
    create_missing_files()
    print()
    
    # Create API models file  
    api_models_success = create_api_models_file()
    print()
    
    # Try to fix database models encoding
    db_models_success = fix_database_models_encoding()
    print()
    
    # Test imports
    import_success = test_imports_safely()
    print()
    
    # Summary
    print("=" * 40)
    fixes_applied = sum([api_models_success, db_models_success, import_success])
    
    if fixes_applied == 3:
        print("🎉 ALL AUTOMATIC FIXES APPLIED!")
        print("✅ Ready to run: python scripts/validate_setup.py")
    else:
        print(f"⚠️  Applied {fixes_applied}/3 automatic fixes")
        show_manual_fixes()
    
    return fixes_applied == 3

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)