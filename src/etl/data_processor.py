# src/etl/data_processor.py - UPDATED WITH CORRECT SPOTIFY MAPPINGS
"""
Updated Data Processor with correct Spotify column mappings based on real file analysis
"""

import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import IntegrityError

from ..database.models import DatabaseManager, StreamingRecord, Artist, Track, Platform, QualityScore
from .parsers.enhanced_parser import EnhancedETLParser

logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    """Result from processing a file"""
    success: bool
    records_processed: int = 0
    records_failed: int = 0
    quality_score: float = 0.0
    processing_time: float = 0.0
    error_message: Optional[str] = None

class StreamingDataProcessor:
    """
    Updated data processor with correct Spotify column mappings from real file analysis
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.parser = EnhancedETLParser()
        
        # Platform-specific column mappings - UPDATED WITH REAL COLUMN NAMES
        self.column_mappings = {
            'spo-spotify': {
                # For weekly TOPD files (track-level data)
                'artist_name': ['artists', 'artist_name', 'Artist Name', 'artist'],
                'track_title': ['track_name', 'track_title', 'Track Name', 'track', 'song'],
                'metric_value': ['streams30s', 'streams', 'Stream Count', 'stream_count', 'plays'],
                'date': ['week_start_date', 'week_end_date', 'date', 'week', 'period'],
                'isrc': ['isrc', 'ISRC', 'track_isrc'],
                'geography': ['country', 'Country', 'territory', 'market'],
                'device_type': ['device', 'device_type'],
                'subscription_type': ['subscription', 'subscription_type'],
                'user_demographic': ['age_bucket', 'gender'],
                
                # For monthly MSED/MSEN files (playlist-level data)
                'playlist_name': ['playlist_name'],
                'playlist_uri': ['playlist_uri'],
                'streamshare': ['streamshare'],
            },
            'apl-apple': {
                'artist_name': ['Artist Name', 'artist_name', 'Artist'],
                'track_title': ['Song', 'song', 'Track Name', 'track_name'],
                'album_name': ['Album', 'album', 'Album Name'],
                'metric_value': ['Quantity', 'quantity', 'Units', 'units'],
                'date': ['Period Start', 'period_start', 'Date', 'date'],
            },
            'fbk-facebook': {
                'artist_name': ['Artist Name', 'artist_name'],
                'track_title': ['Track Name', 'track_name'],
                'metric_value': ['Plays', 'plays', 'Interactions', 'interactions'],
                'date': ['Date', 'date'],
                'isrc': ['ISRC', 'isrc'],
            }
        }
    
    def _find_column(self, df: pd.DataFrame, column_mappings: List[str]) -> Optional[str]:
        """Find the actual column name from a list of possible names"""
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for possible_name in column_mappings:
            if possible_name.lower() in df_columns_lower:
                return df_columns_lower[possible_name.lower()]
        
        return None
    
    def _extract_columns(self, df: pd.DataFrame, platform_code: str) -> Dict[str, Optional[str]]:
        """Extract the actual column names for a platform"""
        mappings = self.column_mappings.get(platform_code, {})
        result = {}
        
        for standard_name, possible_names in mappings.items():
            actual_column = self._find_column(df, possible_names)
            result[standard_name] = actual_column
            
            if actual_column:
                logger.debug(f"Mapped {standard_name} -> {actual_column}")
            else:
                logger.debug(f"Could not find column for {standard_name} in {platform_code} data")
        
        return result
    
    def _detect_spotify_file_type(self, df: pd.DataFrame) -> str:
        """Detect the type of Spotify file based on columns"""
        columns = [col.lower() for col in df.columns]
        
        if 'artists' in columns and 'track_name' in columns and 'streams30s' in columns:
            return 'topd'  # Weekly track data
        elif 'playlist_name' in columns and 'streamshare' in columns:
            return 'playlist'  # Monthly playlist data
        else:
            return 'unknown'
    
    def _get_or_create_artist(self, session, artist_name: str) -> Optional[Artist]:
        """Get existing artist or create new one"""
        if not artist_name or pd.isna(artist_name):
            return None
        
        # Clean artist name
        artist_name = str(artist_name).strip()
        if not artist_name:
            return None
        
        # Normalize for search
        artist_name_normalized = artist_name.lower().strip()
        
        # Try to find existing artist
        artist = session.query(Artist).filter(
            Artist.name_normalized == artist_name_normalized
        ).first()
        
        if not artist:
            # Create new artist
            try:
                artist = Artist(
                    name=artist_name,
                    name_normalized=artist_name_normalized,
                    metadata={}
                )
                session.add(artist)
                session.flush()  # Get the ID
                logger.debug(f"Created new artist: {artist_name} (ID: {artist.id})")
            except Exception as e:
                logger.error(f"Failed to create artist {artist_name}: {e}")
                return None
        
        return artist
    
    def _get_or_create_track(self, session, track_title: str, artist: Optional[Artist], album_name: Optional[str] = None, isrc: Optional[str] = None) -> Optional[Track]:
        """Get existing track or create new one"""
        if not track_title or pd.isna(track_title):
            return None
        
        # Clean track title
        track_title = str(track_title).strip()
        if not track_title:
            return None
        
        # Normalize for search
        track_title_normalized = track_title.lower().strip()
        
        # Try to find existing track
        query = session.query(Track).filter(Track.title_normalized == track_title_normalized)
        
        if artist:
            query = query.filter(Track.artist_id == artist.id)
        
        track = query.first()
        
        if not track:
            # Create new track
            try:
                track = Track(
                    title=track_title,
                    title_normalized=track_title_normalized,
                    album_name=album_name if album_name and not pd.isna(album_name) else None,
                    isrc=isrc if isrc and not pd.isna(isrc) else None,
                    artist_id=artist.id if artist else None
                )
                session.add(track)
                session.flush()  # Get the ID
                logger.debug(f"Created new track: {track_title} by {artist.name if artist else 'Unknown'} (ID: {track.id})")
            except Exception as e:
                logger.error(f"Failed to create track {track_title}: {e}")
                return None
        
        return track
    
    def _process_spotify_playlist_data(self, df: pd.DataFrame, platform_id: int, file_path: str, session) -> tuple[int, int]:
        """Process Spotify playlist data (MSED/MSEN files)"""
        records_processed = 0
        records_failed = 0
        
        logger.info(f"Processing {len(df)} playlist records from {file_path}")
        
        for index, row in df.iterrows():
            try:
                playlist_name = row.get('playlist_name', '')
                streamshare = row.get('streamshare', 0)
                
                if not playlist_name or pd.isna(playlist_name):
                    logger.debug(f"Skipping row {index}: missing playlist_name")
                    records_failed += 1
                    continue
                
                # Convert streamshare to numeric
                try:
                    if streamshare is not None and not pd.isna(streamshare):
                        streamshare = float(str(streamshare))
                    else:
                        streamshare = 0.0
                except (ValueError, TypeError):
                    streamshare = 0.0
                
                # Create a "playlist" artist and track for playlist data
                playlist_artist = self._get_or_create_artist(session, "Playlist Data")
                if not playlist_artist:
                    records_failed += 1
                    continue
                
                playlist_track = self._get_or_create_track(session, playlist_name, playlist_artist)
                if not playlist_track:
                    records_failed += 1
                    continue
                
                # Create streaming record for playlist data
                streaming_record = StreamingRecord(
                    date=datetime.now().date(),  # Use current date for playlist data
                    platform_id=platform_id,
                    track_id=playlist_track.id,
                    artist_name=playlist_artist.name,
                    track_title=playlist_track.title,
                    album_name=None,
                    metric_type='playlist_share',  # Different metric type for playlist data
                    metric_value=streamshare,
                    geography=None,
                    device_type=None,
                    subscription_type=None,
                    raw_data_source=os.path.basename(file_path),
                    data_quality_score=85.0,  # Lower score for playlist data
                    processing_timestamp=datetime.utcnow()
                )
                
                session.add(streaming_record)
                records_processed += 1
                
                # Commit in batches
                if records_processed % 50 == 0:
                    session.commit()
                    logger.debug(f"Committed batch at {records_processed} records")
            
            except Exception as e:
                logger.error(f"Failed to process playlist row {index}: {e}")
                records_failed += 1
                continue
        
        return records_processed, records_failed
    
    def _process_spotify_track_data(self, df: pd.DataFrame, platform_id: int, file_path: str, session, column_map: Dict[str, Optional[str]]) -> tuple[int, int]:
        """Process Spotify track data (TOPD files)"""
        records_processed = 0
        records_failed = 0
        
        logger.info(f"Processing {len(df)} track records from {file_path}")
        
        for index, row in df.iterrows():
            try:
                # Extract basic data using column mappings
                artist_name = None
                if column_map.get('artist_name'):
                    artist_name = row.get(column_map['artist_name'])
                
                track_title = None
                if column_map.get('track_title'):
                    track_title = row.get(column_map['track_title'])
                
                metric_value = None
                if column_map.get('metric_value'):
                    metric_value = row.get(column_map['metric_value'])
                    # Convert to numeric
                    try:
                        if metric_value is not None and not pd.isna(metric_value):
                            metric_value = float(str(metric_value).replace(',', ''))
                    except (ValueError, TypeError):
                        metric_value = 0.0
                
                # Skip rows without essential data
                if not artist_name or not track_title or pd.isna(artist_name) or pd.isna(track_title):
                    logger.debug(f"Skipping row {index}: missing artist_name or track_title")
                    records_failed += 1
                    continue
                
                # Get or create artist
                artist = self._get_or_create_artist(session, artist_name)
                if not artist:
                    logger.warning(f"Failed to get/create artist for row {index}: {artist_name}")
                    records_failed += 1
                    continue
                
                # Get additional fields
                isrc = None
                if column_map.get('isrc'):
                    isrc = row.get(column_map['isrc'])
                
                # Get or create track
                track = self._get_or_create_track(session, track_title, artist, None, isrc)
                if not track:
                    logger.warning(f"Failed to get/create track for row {index}: {track_title}")
                    records_failed += 1
                    continue
                
                # Extract date
                date_value = None
                if column_map.get('date'):
                    date_raw = row.get(column_map['date'])
                    if date_raw and not pd.isna(date_raw):
                        try:
                            if isinstance(date_raw, str):
                                from dateutil import parser as date_parser
                                date_value = date_parser.parse(date_raw).date()
                            else:
                                date_value = pd.to_datetime(date_raw).date()
                        except:
                            logger.warning(f"Could not parse date: {date_raw}")
                            date_value = datetime.now().date()
                    else:
                        date_value = datetime.now().date()
                else:
                    date_value = datetime.now().date()
                
                # Extract other fields
                geography = None
                if column_map.get('geography'):
                    geography = row.get(column_map['geography'])
                
                # Create user demographic info if available
                user_demographic = {}
                if column_map.get('user_demographic'):
                    # Handle age_bucket and gender
                    age_bucket = row.get('age_bucket')
                    gender = row.get('gender')
                    if age_bucket and not pd.isna(age_bucket):
                        user_demographic['age_bucket'] = str(age_bucket)
                    if gender and not pd.isna(gender):
                        user_demographic['gender'] = str(gender)
                
                user_demographic_str = str(user_demographic) if user_demographic else None
                
                # Create streaming record
                streaming_record = StreamingRecord(
                    date=date_value,
                    platform_id=platform_id,
                    track_id=track.id,
                    artist_name=artist.name,
                    track_title=track.title,
                    album_name=None,
                    metric_type='streams',
                    metric_value=metric_value or 0.0,
                    geography=geography,
                    device_type=None,
                    subscription_type=None,
                    user_demographic=user_demographic_str,
                    raw_data_source=os.path.basename(file_path),
                    data_quality_score=95.0,
                    processing_timestamp=datetime.utcnow()
                )
                
                session.add(streaming_record)
                records_processed += 1
                
                # Commit in batches
                if records_processed % 100 == 0:
                    session.commit()
                    logger.debug(f"Committed batch at {records_processed} records")
            
            except Exception as e:
                logger.error(f"Failed to process track row {index}: {e}")
                records_failed += 1
                continue
        
        return records_processed, records_failed
    
    def _process_dataframe(self, df: pd.DataFrame, platform_code: str, file_path: str) -> ProcessingResult:
        """Process a parsed DataFrame into database records"""
        start_time = datetime.now()
        
        try:
            with self.db_manager.get_session() as session:
                
                # Get platform
                platform = session.query(Platform).filter(Platform.code == platform_code).first()
                if not platform:
                    return ProcessingResult(
                        success=False,
                        error_message=f"Platform {platform_code} not found in database"
                    )
                
                # For Spotify, detect file type and handle accordingly
                if platform_code == 'spo-spotify':
                    spotify_file_type = self._detect_spotify_file_type(df)
                    logger.info(f"Detected Spotify file type: {spotify_file_type}")
                    
                    if spotify_file_type == 'playlist':
                        # Process playlist data (MSED/MSEN files)
                        records_processed, records_failed = self._process_spotify_playlist_data(
                            df, platform.id, file_path, session
                        )
                    elif spotify_file_type == 'topd':
                        # Process track data (TOPD files) 
                        column_map = self._extract_columns(df, platform_code)
                        records_processed, records_failed = self._process_spotify_track_data(
                            df, platform.id, file_path, session, column_map
                        )
                    else:
                        return ProcessingResult(
                            success=False,
                            error_message=f"Unknown Spotify file type with columns: {list(df.columns)}"
                        )
                else:
                    # Handle other platforms with original logic
                    column_map = self._extract_columns(df, platform_code)
                    records_processed, records_failed = self._process_spotify_track_data(
                        df, platform.id, file_path, session, column_map
                    )
                
                # Final commit
                session.commit()
                
                processing_time = (datetime.now() - start_time).total_seconds()
                
                logger.info(f"Successfully processed {records_processed} records in {processing_time:.2f}s")
                
                return ProcessingResult(
                    success=True,
                    records_processed=records_processed,
                    records_failed=records_failed,
                    quality_score=95.0 if records_processed > 0 else 0.0,
                    processing_time=processing_time
                )
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Processing failed: {e}")
            return ProcessingResult(
                success=False,
                error_message=str(e),
                processing_time=processing_time,
                records_failed=len(df) if df is not None else 0
            )
    
    def process_file(self, file_path: str) -> ProcessingResult:
        """Process a single file"""
        logger.info(f"Processing file: {file_path}")
        
        try:
            file_path_obj = Path(file_path)
            
            # Parse the file
            parse_result = self.parser.parse_file(file_path_obj)
            
            if not parse_result.success:
                return ProcessingResult(
                    success=False,
                    error_message=f"Parsing failed: {parse_result.error_message}"
                )
            
            if parse_result.data is None or parse_result.data.empty:
                return ProcessingResult(
                    success=False,
                    error_message="No data found in file"
                )
            
            # Detect platform
            platform_code = self.parser.detect_platform(file_path_obj)
            if not platform_code:
                return ProcessingResult(
                    success=False,
                    error_message="Could not detect platform from file path"
                )
            
            logger.info(f"Detected platform: {platform_code}")
            logger.info(f"Data shape: {parse_result.data.shape}")
            logger.info(f"Columns: {list(parse_result.data.columns)}")
            
            # Process the data
            result = self._process_dataframe(parse_result.data, platform_code, file_path)
            
            # Update quality score from parsing
            if result.success:
                result.quality_score = parse_result.quality_score
            
            return result
            
        except Exception as e:
            logger.error(f"File processing failed: {e}")
            return ProcessingResult(
                success=False,
                error_message=str(e)
            )

# Keep the existing process_file function for backward compatibility
def process_file(file_path: str, db_manager: DatabaseManager = None) -> ProcessingResult:
    """Legacy function for backward compatibility"""
    if db_manager is None:
        db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
    
    processor = StreamingDataProcessor(db_manager)
    return processor.process_file(file_path)