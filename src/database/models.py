from __future__ import annotations

# src/database/models.py - CLEANED VERSION
"""
Database models and schema for Streaming Analytics Platform
SQLAlchemy models only - API models moved to src/api/models.py
"""

from datetime import datetime
import uuid
import os
import logging
import json
from typing import Any

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, 
    Boolean, Index, ForeignKey,
    create_engine, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import TypeDecorator, CHAR
from contextlib import contextmanager

logger = logging.getLogger(__name__)
Base = declarative_base()

# SQLite-compatible UUID type
class GUID(TypeDecorator):
    """Platform-independent GUID type."""
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            value = uuid.uuid4()
        if dialect.name == 'postgresql':
            return str(value)
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                return uuid.UUID(value)
            return value

# Cross-database JSON type
class JSONType(TypeDecorator):
    """Cross-database JSON type that handles serialization"""
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                return value
        return value

def get_json_type():
    """Return appropriate JSON type for the database"""
    db_url = os.getenv('DATABASE_URL', '')
    if 'sqlite' in db_url.lower():
        return JSONType  # Use custom JSON type for SQLite
    else:
        from sqlalchemy import JSON
        return JSON  # Use native JSON in PostgreSQL

class Platform(Base):
    """Reference table for streaming platforms"""
    __tablename__ = 'platforms'
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    
    # Configuration stored as JSON/Text
    file_patterns = Column(get_json_type())
    date_formats = Column(get_json_type())
    delimiter_type = Column(String(20), default='auto')
    encoding = Column(String(20), default='utf-8')
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    streaming_records = relationship("StreamingRecord", back_populates="platform")
    processing_logs = relationship("DataProcessingLog", back_populates="platform")
    quality_scores = relationship("QualityScore", back_populates="platform")

class Artist(Base):
    """Deduplicated artist data"""
    __tablename__ = 'artists'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(500), nullable=False, index=True)
    name_normalized = Column(String(500), nullable=False, index=True)
    external_ids = Column(get_json_type())
    artist_metadata = Column(get_json_type())  # Renamed from metadata to avoid conflict
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    tracks = relationship("Track", back_populates="artist")

class Track(Base):
    """Track metadata with ISRC as primary identifier"""
    __tablename__ = 'tracks'
    
    id = Column(Integer, primary_key=True)
    isrc = Column(String(12), unique=True, nullable=True, index=True)
    title = Column(String(1000), nullable=False, index=True)
    title_normalized = Column(String(1000), nullable=False, index=True)
    album_name = Column(String(1000))
    duration_ms = Column(Integer)
    genre = Column(String(200))
    
    # Foreign Keys
    artist_id = Column(Integer, ForeignKey('artists.id'), nullable=False)
    
    # Metadata - renamed to avoid SQLAlchemy conflict
    external_ids = Column(get_json_type())
    track_metadata = Column(get_json_type())  # Renamed from metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    artist = relationship("Artist", back_populates="tracks")
    streaming_records = relationship("StreamingRecord", back_populates="track")

class StreamingRecord(Base):
    """Main hypertable for streaming data - optimized for time-series queries"""
    __tablename__ = 'streaming_records'
    
    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    date = Column(DateTime, nullable=False, index=True)
    
    # Foreign Keys
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    track_id = Column(Integer, ForeignKey('tracks.id'), nullable=True)
    
    # Store denormalized data for performance
    artist_name = Column(String(500))  # Denormalized for performance
    track_title = Column(String(1000))  # Denormalized for performance
    album_name = Column(String(1000))
    
    # Core metrics
    metric_type = Column(String(50), nullable=False)
    metric_value = Column(Numeric(15, 2), nullable=False)
    
    # Dimensions
    geography = Column(String(10))
    device_type = Column(String(20))
    subscription_type = Column(String(20))
    context_type = Column(String(50))
    
    # Demographics and other metadata
    user_demographic = Column(get_json_type())
    genre = Column(String(200))
    
    # Data quality and lineage
    data_quality_score = Column(Numeric(5, 2))
    raw_data_source = Column(String(500))
    file_hash = Column(String(64))
    processing_timestamp = Column(DateTime, default=datetime.utcnow)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform", back_populates="streaming_records")
    track = relationship("Track", back_populates="streaming_records")
    
    # Indexes
    __table_args__ = (
        Index('ix_streaming_records_date_platform', 'date', 'platform_id'),
        Index('ix_streaming_records_track_date', 'track_id', 'date'),
        Index('ix_streaming_records_geography_date', 'geography', 'date'),
        Index('ix_streaming_records_file_hash', 'file_hash'),
        Index('ix_streaming_records_metric_type_date', 'metric_type', 'date'),
        Index('ix_streaming_records_artist_name', 'artist_name'),
        Index('ix_streaming_records_track_title', 'track_title'),
    )

class DataProcessingLog(Base):
    """Audit trail for ETL processing"""
    __tablename__ = 'data_processing_logs'
    
    id = Column(Integer, primary_key=True)
    file_path = Column(String(1000), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_size = Column(Integer)
    file_hash = Column(String(64), nullable=False, index=True)
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    
    # Processing details
    processing_status = Column(String(20), nullable=False)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    quality_score = Column(Numeric(5, 2))
    error_message = Column(Text)
    error_details = Column(get_json_type())
    
    # Processing metadata
    processing_config = Column(get_json_type())
    performance_metrics = Column(get_json_type())
    
    # Timestamps
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    processing_duration_ms = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform", back_populates="processing_logs")

class QualityScore(Base):
    """Data quality tracking by file and platform"""
    __tablename__ = 'quality_scores'
    
    id = Column(Integer, primary_key=True)
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)
    file_path = Column(String(1000))
    
    # Quality metrics
    overall_score = Column(Numeric(5, 2), nullable=False)
    completeness_score = Column(Numeric(5, 2))
    consistency_score = Column(Numeric(5, 2))
    validity_score = Column(Numeric(5, 2))
    accuracy_score = Column(Numeric(5, 2))
    
    # Detailed quality info
    quality_details = Column(get_json_type())
    validation_results = Column(get_json_type())
    issues_found = Column(get_json_type())
    recommendations = Column(get_json_type())
    
    # Timestamps
    measured_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform", back_populates="quality_scores")

class FileProcessingQueue(Base):
    """File processing queue for batch processing"""
    __tablename__ = 'file_processing_queue'
    
    id = Column(Integer, primary_key=True)
    file_path = Column(String(1000), nullable=False)
    file_hash = Column(String(64))
    platform_id = Column(Integer, ForeignKey('platforms.id'))
    
    priority = Column(Integer, default=5)
    status = Column(String(20), default='pending')
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    error_message = Column(Text)
    
    scheduled_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform")

class DatabaseManager:
    """Manages database connections and TimescaleDB setup"""
    
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not provided")
        
        # Create engine with appropriate settings
        engine_kwargs: dict[str, Any] = {
            'pool_pre_ping': True,
            'echo': os.getenv('DATABASE_DEBUG', 'false').lower() == 'true'
        }
        
        # Add PostgreSQL-specific settings - Fixed type issues
        if 'postgresql' in self.database_url.lower():
            # Create a separate dict for PostgreSQL settings to avoid type conflicts
            postgres_settings = {
                'pool_size': 10,
                'max_overflow': 20,
            }
            engine_kwargs.update(postgres_settings)
        
        self.engine = create_engine(self.database_url, **engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_all_tables(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created")
    
    def setup_timescaledb(self):
        """Setup TimescaleDB hypertable and optimizations"""
        # Fixed: Ensure database_url is not None before calling .lower()
        if self.database_url and 'sqlite' in self.database_url.lower():
            logger.info("SQLite detected - skipping TimescaleDB setup")
            return
        
        try:
            with self.engine.connect() as conn:
                # Enable TimescaleDB extension
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                conn.commit()
                
                # Create hypertable for streaming_records
                try:
                    conn.execute(text("""
                        SELECT create_hypertable('streaming_records', 'date', 
                                                chunk_time_interval => INTERVAL '1 month',
                                                if_not_exists => TRUE);
                    """))
                    logger.info("TimescaleDB hypertable created for streaming_records")
                except Exception as e:
                    if "already a hypertable" in str(e):
                        logger.info("TimescaleDB hypertable already exists")
                    else:
                        logger.error(f"Failed to create hypertable: {e}")
                
                conn.commit()
        except Exception as e:
            logger.warning(f"TimescaleDB setup failed (continuing with regular PostgreSQL): {e}")
    
    def initialize_reference_data(self):
        """Initialize reference data for platforms"""
        with self.get_session() as session:
            # Check if platforms already exist
            existing_count = session.query(Platform).count()
            if existing_count > 0:
                logger.info("Reference data already exists")
                return
            
            platforms = [
                Platform(code="apl-apple", name="Apple Music/iTunes", 
                        description="Apple Music and iTunes Store streaming data"),
                Platform(code="awa-awa", name="AWA", 
                        description="AWA Japanese streaming platform data"),
                Platform(code="boo-boomplay", name="Boomplay", 
                        description="Boomplay African streaming platform data"),
                Platform(code="dzr-deezer", name="Deezer", 
                        description="Deezer streaming platform data"),
                Platform(code="fbk-facebook", name="Facebook/Meta", 
                        description="Facebook and Instagram music usage data"),
                Platform(code="plt-peloton", name="Peloton", 
                        description="Peloton fitness platform music data"),
                Platform(code="scu-soundcloud", name="SoundCloud", 
                        description="SoundCloud streaming and user interaction data"),
                Platform(code="spo-spotify", name="Spotify", 
                        description="Spotify streaming data with demographics"),
                Platform(code="vvo-vevo", name="Vevo", 
                        description="Vevo video streaming and view data"),
            ]
            
            session.add_all(platforms)
            session.commit()
            logger.info(f"Initialized {len(platforms)} platform references")
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_platform_by_code(self, code: str) -> Platform | None:
        """Get platform by code"""
        if not code:
            return None
        
        with self.get_session() as session:
            return session.query(Platform).filter(Platform.code == code).first()

def initialize_database(database_url: str | None = None) -> DatabaseManager:
    """Initialize database with all required setup"""
    if not database_url:
        database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        raise ValueError("DATABASE_URL not provided and not found in environment")
    
    db = DatabaseManager(database_url)
    
    logger.info("🔄 Initializing database...")
    
    # Create tables
    db.create_all_tables()
    
    # Setup TimescaleDB (only for PostgreSQL)
    if 'postgresql' in database_url.lower():
        db.setup_timescaledb()
    else:
        logger.info("Non-PostgreSQL database - skipping TimescaleDB setup")
    
    # Initialize reference data
    db.initialize_reference_data()
    
    logger.info("✅ Database initialization complete")
    
    return db

# Example usage for testing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not found in environment")
        exit(1)
    
    try:
        db = initialize_database(db_url)
        
        # Test query
        with db.get_session() as session:
            platforms = session.query(Platform).all()
            print(f"Found {len(platforms)} platforms:")
            for platform in platforms:
                print(f"  - {platform.code}: {platform.name}")
                
    except Exception as e:
        print(f"Database initialization failed: {e}")
        import traceback
        traceback.print_exc()