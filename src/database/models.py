# src/database/models.py
"""
Database models and schema for Streaming Analytics Platform
Using SQLAlchemy with TimescaleDB hypertables for time-series optimization
"""

from datetime import datetime
from typing import Optional
from decimal import Decimal

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, 
    Boolean, JSON, Index, ForeignKey, UniqueConstraint,
    create_engine, text
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()


class Platform(Base):
    """Reference table for streaming platforms"""
    __tablename__ = 'platforms'
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False, index=True)  # e.g., 'spo-spotify'
    name = Column(String(100), nullable=False)  # e.g., 'Spotify'
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    streaming_records = relationship("StreamingRecord", back_populates="platform")


class Artist(Base):
    """Deduplicated artist data"""
    __tablename__ = 'artists'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(500), nullable=False, index=True)
    name_normalized = Column(String(500), nullable=False, index=True)  # For fuzzy matching
    external_ids = Column(JSON)  # Store platform-specific IDs
    metadata = Column(JSON)  # Additional artist info
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    tracks = relationship("Track", back_populates="artist")


class Track(Base):
    """Track metadata with ISRC as primary identifier"""
    __tablename__ = 'tracks'
    
    id = Column(Integer, primary_key=True)
    isrc = Column(String(12), unique=True, nullable=True, index=True)  # International Standard Recording Code
    title = Column(String(1000), nullable=False, index=True)
    title_normalized = Column(String(1000), nullable=False, index=True)
    album_name = Column(String(1000))
    duration_ms = Column(Integer)  # Duration in milliseconds
    genre = Column(String(200))
    
    # Foreign Keys
    artist_id = Column(Integer, ForeignKey('artists.id'), nullable=False)
    
    # Metadata
    external_ids = Column(JSON)  # Platform-specific track IDs
    metadata = Column(JSON)  # Additional track info
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    artist = relationship("Artist", back_populates="tracks")
    streaming_records = relationship("StreamingRecord", back_populates="track")


class StreamingRecord(Base):
    """
    Main hypertable for streaming data - optimized for time-series queries
    This will be converted to a TimescaleDB hypertable
    """
    __tablename__ = 'streaming_records'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Time dimension (partition key for TimescaleDB)
    date = Column(DateTime, nullable=False, index=True)
    
    # Foreign Keys
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    track_id = Column(Integer, ForeignKey('tracks.id'), nullable=True)  # May be null during processing
    
    # Core metrics
    metric_type = Column(String(50), nullable=False)  # streams, plays, saves, shares, etc.
    metric_value = Column(Numeric(15, 2), nullable=False)
    
    # Dimensions
    geography = Column(String(10))  # Country code
    device_type = Column(String(20))  # mobile, desktop, tablet, tv, unknown
    subscription_type = Column(String(20))  # free, paid, trial, unknown
    context_type = Column(String(50))  # playlist, radio, search, social
    
    # Demographics (JSON for flexibility)
    user_demographic = Column(JSON)  # age_range, gender, etc.
    
    # Data quality and lineage
    data_quality_score = Column(Numeric(5, 2))  # 0-100 quality score
    raw_data_source = Column(String(500))  # Original filename
    file_hash = Column(String(64))  # For deduplication
    processing_timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform", back_populates="streaming_records")
    track = relationship("Track", back_populates="streaming_records")
    
    # Indexes for common query patterns
    __table_args__ = (
        Index('ix_streaming_records_date_platform', 'date', 'platform_id'),
        Index('ix_streaming_records_track_date', 'track_id', 'date'),
        Index('ix_streaming_records_geography_date', 'geography', 'date'),
        Index('ix_streaming_records_file_hash', 'file_hash'),  # For deduplication
        Index('ix_streaming_records_metric_type_date', 'metric_type', 'date'),
    )


class DataProcessingLog(Base):
    """Audit trail for ETL processing"""
    __tablename__ = 'data_processing_logs'
    
    id = Column(Integer, primary_key=True)
    file_path = Column(String(1000), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    
    # Processing details
    processing_status = Column(String(20), nullable=False)  # started, completed, failed
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    quality_score = Column(Numeric(5, 2))
    error_message = Column(Text)
    
    # Processing metadata
    processing_config = Column(JSON)  # Parser config used
    performance_metrics = Column(JSON)  # Processing time, memory usage, etc.
    
    # Timestamps
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform")


class QualityScore(Base):
    """Data quality tracking by file and platform"""
    __tablename__ = 'quality_scores'
    
    id = Column(Integer, primary_key=True)
    platform_id = Column(Integer, ForeignKey('platforms.id'), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)
    
    # Quality metrics
    overall_score = Column(Numeric(5, 2), nullable=False)  # 0-100
    completeness_score = Column(Numeric(5, 2))
    consistency_score = Column(Numeric(5, 2))
    validity_score = Column(Numeric(5, 2))
    
    # Detailed quality info
    quality_details = Column(JSON)  # Specific quality issues found
    validation_results = Column(JSON)  # Validation rule results
    
    # Timestamps
    measured_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    platform = relationship("Platform")


# src/database/connection.py
"""
Database connection and TimescaleDB setup
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and TimescaleDB setup"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not provided")
            
        self.engine = create_engine(
            self.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=os.getenv('DATABASE_DEBUG', 'false').lower() == 'true'
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)
                    logger.info("Database tables created")
    
    def setup_timescaledb(self):
        """Setup TimescaleDB hypertable and optimizations"""
        with self.engine.connect() as conn:
            # Enable TimescaleDB extension
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            conn.commit()
            
            # Create hypertable for streaming_records
            try:
                conn.execute(text("""
                    SELECT create_hypertable('streaming_records', 'date', 
                                            chunk_time_interval => INTERVAL '1 month');
                """))
                logger.info("TimescaleDB hypertable created for streaming_records")
            except Exception as e:
                if "already a hypertable" in str(e):
                    logger.info("TimescaleDB hypertable already exists")
                else:
                    logger.error(f"Failed to create hypertable: {e}")
                    raise
            
            # Create continuous aggregates for common queries
            self._create_continuous_aggregates(conn)
            conn.commit()
    
    def _create_continuous_aggregates(self, conn):
        """Create continuous aggregates for performance"""
        
        # Daily aggregates by platform
        try:
            conn.execute(text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS daily_platform_metrics
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 day', date) AS day,
                    platform_id,
                    metric_type,
                    SUM(metric_value) as total_value,
                    COUNT(*) as record_count,
                    AVG(data_quality_score) as avg_quality_score
                FROM streaming_records
                GROUP BY day, platform_id, metric_type
                WITH NO DATA;
            """))
            
            # Add refresh policy
            conn.execute(text("""
                SELECT add_continuous_aggregate_policy('daily_platform_metrics',
                    start_offset => INTERVAL '1 month',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour');
            """))
            
            logger.info("Continuous aggregates created")
            
        except Exception as e:
            if "already exists" in str(e):
                logger.info("Continuous aggregates already exist")
            else:
                logger.warning(f"Failed to create continuous aggregates: {e}")
    
    def initialize_reference_data(self):
        """Initialize reference data for platforms"""
        with self.get_session() as session:
            # Check if platforms already exist
            existing_platforms = session.query(Platform).count()
            if existing_platforms > 0:
                logger.info("Reference data already exists")
                return
            
            platforms = [
                Platform(code="apl-apple", name="Apple Music/iTunes"),
                Platform(code="awa-awa", name="AWA"),
                Platform(code="boo-boomplay", name="Boomplay"),
                Platform(code="dzr-deezer", name="Deezer"),
                Platform(code="fbk-facebook", name="Facebook/Meta"),
                Platform(code="plt-peloton", name="Peloton"),
                Platform(code="scu-soundcloud", name="SoundCloud"),
                Platform(code="spo-spotify", name="Spotify"),
                Platform(code="vvo-vevo", name="Vevo"),
            ]
            
            session.add_all(platforms)
            session.commit()
            logger.info("Reference data initialized")
    
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
    
    def get_platform_by_code(self, code: str) -> Optional[Platform]:
        """Get platform by code"""
        with self.get_session() as session:
            return session.query(Platform).filter(Platform.code == code).first()


# Database initialization script
def initialize_database(database_url: str = None):
    """Initialize database with all required setup"""
    db = DatabaseManager(database_url)
    
    logger.info("🔄 Initializing database...")
    
    # Create tables
    db.create_tables()
    
    # Setup TimescaleDB
    db.setup_timescaledb()
    
    # Initialize reference data
    db.initialize_reference_data()
    
    logger.info("Database initialization complete")
    
    return db


# Example usage and testing
if __name__ == "__main__":
    # Test database setup
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    db_url = os.getenv('DATABASE_URL', 'postgresql://analytics_user:analytics_password@localhost:5432/streaming_analytics')
    
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