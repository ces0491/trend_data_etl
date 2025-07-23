# scripts/quick_start_demo.py
"""
Quick Start Demo for Streaming Analytics Platform
Demonstrates end-to-end functionality with sample data
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from database.models import initialize_database, DatabaseManager
from etl.data_processor import StreamingDataProcessor

# Load environment
load_dotenv()


def create_sample_data():
    """Create sample streaming data files for testing"""
    sample_dir = Path("data/sample")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Apple Music sample (quote-wrapped TSV)
    apple_data = '''
"artist_name\ttrack_title\treport_date\tquantity\tcustomer_currency\tvendor_identifier"
"Taylor Swift\tShake It Off\t2024-01-15\t1250\tUSD\tPADPIDA2021030304M_191061307952_USCGH1743953"
"Ed Sheeran\tShape of You\t2024-01-15\t890\tUSD\tPADPIDA2021030304M_191061307952_GBUM71505078"
"Billie Eilish\tbad guy\t2024-01-15\t2100\tUSD\tPADPIDA2021030304M_191061307952_USIR12000001"
'''
    
    with open(sample_dir / "apl-apple_sample.txt", 'w') as f:
        f.write(apple_data.strip())
    
    # Spotify sample (standard TSV)
    spotify_data = '''artist_name	track_name	streams	date	country
Taylor Swift	Anti-Hero	45000	2024-01-15	US
Bad Bunny	Tití Me Preguntó	38000	2024-01-15	US
Harry Styles	As It Was	29000	2024-01-15	US
'''
    
    with open(sample_dir / "spo-spotify_sample.tsv", 'w') as f:
        f.write(spotify_data)
    
    # Facebook sample (quoted CSV)
    facebook_data = '''"isrc","date","product_type","plays"
"USRC17607839","2024-01-15","FB_REELS","1200"
"GBUM71505078","2024-01-15","IG_MUSIC_STICKER","890"
"USIR12000001","2024-01-15","FB_FROM_IG_CROSSPOST","1500"
'''
    
    with open(sample_dir / "fbk-facebook_sample.csv", 'w') as f:
        f.write(facebook_data)
    
    print(f"Created sample data files in {sample_dir}")
    return sample_dir


def run_processing_demo(sample_dir: Path, db_manager: DatabaseManager):
    """Run the complete processing demo"""
    print("\nRUNNING PROCESSING DEMO")
    print("=" * 50)
    
    processor = StreamingDataProcessor(db_manager)
    
    # Process all sample files
    results = processor.process_directory(sample_dir)
    
    # Print detailed results
    print(f"\nPROCESSING RESULTS:")
    for result in results:
        status = "[SUCCESS]" if result.success else "[FAILED]"
        print(f"{status} - {Path(result.file_path).name}")
        print(f"  Platform: {result.platform}")
        print(f"  Records: {result.records_processed}")
        print(f"  Quality: {result.quality_score:.1f}/100")
        if result.processing_time:
            print(f"  Time: {result.processing_time:.2f}s")
        if result.error_message:
            print(f"  Error: {result.error_message}")
        print()
    
    return results


def query_data_demo(db_manager: DatabaseManager):
    """Demonstrate data querying"""
    print("\nDATA QUERYING DEMO")
    print("=" * 50)
    
    with db_manager.get_session() as session:
        from database.models import StreamingRecord, Platform, Artist, Track
        
        # Query total records
        total_records = session.query(StreamingRecord).count()
        print(f"Total streaming records: {total_records:,}")
        
        # Query by platform
        platforms = session.query(Platform).all()
        print(f"\nRecords by platform:")
        for platform in platforms:
            count = session.query(StreamingRecord).filter_by(platform_id=platform.id).count()
            if count > 0:
                print(f"  {platform.name}: {count:,} records")
        
        # Top artists
        print(f"\nTop artists by records:")
        top_artists = session.query(
            Artist.name,
            session.query(StreamingRecord).join(Track).filter(Track.artist_id == Artist.id).count().label('record_count')
        ).join(Track).join(StreamingRecord).group_by(Artist.id, Artist.name).order_by(
            session.query(StreamingRecord).join(Track).filter(Track.artist_id == Artist.id).count().desc()
        ).limit(5).all()
        
        for artist_name, count in top_artists:
            print(f"  {artist_name}: {count} records")
        
        # Recent records
        print(f"\nRecent streaming records:")
        recent = session.query(StreamingRecord).join(Platform).join(Track).join(Artist).order_by(
            StreamingRecord.created_at.desc()
        ).limit(5).all()
        
        for record in recent:
            print(f"  {record.track.artist.name} - {record.track.title} ({record.platform.name}): {record.metric_value}")


# Basic FastAPI implementation for Phase 1 Data Access API
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import date
from typing import Optional

app = FastAPI(
    title="Trend Data ETL Platform - Data Access API",
    description="Phase 1: Data extraction, transformation, loading and access for streaming platform data",
    version="1.0.0"
)

# Dependency to get database session
def get_db():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise HTTPException(status_code=500, detail="Database not configured")
    db_manager = DatabaseManager(db_url)
    return db_manager

# Response models
class PlatformResponse(BaseModel):
    id: int
    code: str
    name: str
    is_active: bool

class ArtistResponse(BaseModel):
    id: int
    name: str

class TrackResponse(BaseModel):
    id: int
    title: str
    isrc: Optional[str]
    artist_name: str

class StreamingRecordResponse(BaseModel):
    id: str
    date: str
    platform_name: str
    track_title: str
    artist_name: str
    metric_type: str
    metric_value: float
    geography: Optional[str]
    quality_score: Optional[float]

class QualitySummaryResponse(BaseModel):
    total_files_processed: int
    average_quality_score: float
    files_above_threshold: int
    quality_threshold: float

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Trend Data ETL Platform - Phase 1 Data Access API", "version": "1.0.0"}

@app.get("/platforms", response_model=List[PlatformResponse])
async def get_platforms(db: DatabaseManager = Depends(get_db)):
    """Get all streaming platforms"""
    with db.get_session() as session:
        from database.models import Platform
        platforms = session.query(Platform).filter_by(is_active=True).all()
        return [
            PlatformResponse(id=p.id, code=p.code, name=p.name, is_active=p.is_active)
            for p in platforms
        ]

@app.get("/artists", response_model=List[ArtistResponse])
async def get_artists(
    search: Optional[str] = Query(None, description="Search term for artist names"),
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of results"),
    db: DatabaseManager = Depends(get_db)
):
    """Search for artists"""
    with db.get_session() as session:
        from database.models import Artist
        query = session.query(Artist)
        
        if search:
            query = query.filter(Artist.name_normalized.contains(search.lower()))
        
        artists = query.limit(limit).all()
        return [ArtistResponse(id=a.id, name=a.name) for a in artists]

@app.get("/artists/{artist_id}/tracks", response_model=List[TrackResponse])
async def get_artist_tracks(
    artist_id: int,
    limit: int = Query(100, ge=1, le=1000),
    db: DatabaseManager = Depends(get_db)
):
    """Get tracks for a specific artist"""
    with db.get_session() as session:
        from database.models import Track, Artist
        tracks = session.query(Track).join(Artist).filter(
            Track.artist_id == artist_id
        ).limit(limit).all()
        
        if not tracks:
            raise HTTPException(status_code=404, detail="Artist not found or no tracks")
        
        return [
            TrackResponse(
                id=t.id, 
                title=t.title, 
                isrc=t.isrc, 
                artist_name=t.artist.name
            ) for t in tracks
        ]

@app.get("/streaming-records", response_model=List[StreamingRecordResponse])
async def get_streaming_records(
    platform: Optional[str] = Query(None, description="Platform code filter"),
    artist_name: Optional[str] = Query(None, description="Artist name filter"),
    date_from: Optional[date] = Query(None, description="Start date filter"),
    date_to: Optional[date] = Query(None, description="End date filter"),
    limit: int = Query(100, ge=1, le=10000),
    db: DatabaseManager = Depends(get_db)
):
    """Get streaming records with filters"""
    with db.get_session() as session:
        from database.models import StreamingRecord, Platform, Track, Artist
        
        query = session.query(StreamingRecord).join(Platform).join(Track).join(Artist)
        
        if platform:
            query = query.filter(Platform.code == platform)
        
        if artist_name:
            query = query.filter(Artist.name_normalized.contains(artist_name.lower()))
        
        if date_from:
            query = query.filter(StreamingRecord.date >= date_from)
        
        if date_to:
            query = query.filter(StreamingRecord.date <= date_to)
        
        records = query.order_by(StreamingRecord.date.desc()).limit(limit).all()
        
        return [
            StreamingRecordResponse(
                id=str(r.id),
                date=r.date.isoformat(),
                platform_name=r.platform.name,
                track_title=r.track.title,
                artist_name=r.track.artist.name,
                metric_type=r.metric_type,
                metric_value=float(r.metric_value),
                geography=r.geography,
                quality_score=float(r.data_quality_score) if r.data_quality_score else None
            ) for r in records
        ]

@app.get("/data-quality/summary", response_model=QualitySummaryResponse)
async def get_quality_summary(db: DatabaseManager = Depends(get_db)):
    """Get data quality summary"""
    with db.get_session() as session:
        from database.models import QualityScore
        
        quality_records = session.query(QualityScore).all()
        
        if not quality_records:
            return QualitySummaryResponse(
                total_files_processed=0,
                average_quality_score=0.0,
                files_above_threshold=0,
                quality_threshold=90.0
            )
        
        threshold = 90.0
        total_files = len(quality_records)
        avg_score = sum(float(q.overall_score) for q in quality_records) / total_files
        above_threshold = sum(1 for q in quality_records if float(q.overall_score) >= threshold)
        
        return QualitySummaryResponse(
            total_files_processed=total_files,
            average_quality_score=round(avg_score, 2),
            files_above_threshold=above_threshold,
            quality_threshold=threshold
        )

# Health check endpoint
@app.get("/health")
async def health_check(db: DatabaseManager = Depends(get_db)):
    """Health check endpoint"""
    try:
        with db.get_session() as session:
            from database.models import Platform
            platform_count = session.query(Platform).count()
            return {"status": "healthy", "platforms_configured": platform_count}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")


def main():
    """Main demo function"""
    print("TREND DATA ETL PLATFORM - QUICK START DEMO")
    print("=" * 60)
    
    # Check environment
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not found in environment")
        print("Please run: cp .env.template .env and configure database settings")
        return
    
    try:
        # Step 1: Initialize database
        print("Initializing database...")
        db_manager = initialize_database(db_url)
        print("Database initialized successfully")
        
        # Step 2: Create sample data
        print("\nCreating sample data...")
        sample_dir = create_sample_data()
        
        # Step 3: Process sample data
        results = run_processing_demo(sample_dir, db_manager)
        
        # Step 4: Query data
        query_data_demo(db_manager)
        
        # Step 5: Show API info
        print("\nAPI SERVER READY")
        print("=" * 50)
        print("Start the API server with:")
        print("  uvicorn scripts.quick_start_demo:app --reload --host 0.0.0.0 --port 8000")
        print("\nAPI endpoints available at http://localhost:8000:")
        print("  • GET /platforms - List all platforms")
        print("  • GET /artists?search=<name> - Search artists")
        print("  • GET /streaming-records - Get streaming data")
        print("  • GET /data-quality/summary - Quality metrics")
        print("  • GET /docs - Interactive API documentation")
        
        # Summary
        successful_files = sum(1 for r in results if r.success)
        total_records = sum(r.records_processed for r in results if r.success)
        
        print(f"\nDEMO SUMMARY")
        print("=" * 50)
        print(f"Files processed: {successful_files}/{len(results)}")
        print(f"Records stored: {total_records:,}")
        print(f"Database tables: Created and populated")
        print(f"API endpoints: Ready to use")
        print(f"Validation framework: Active")
        
        if successful_files == len(results) and total_records > 0:
            print("\nPhase 1 implementation SUCCESSFUL!")
            print("Ready to begin Phase 2 development or process production data.")
        else:
            print("\nSome issues found - check logs above")
            
    except Exception as e:
        print(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()