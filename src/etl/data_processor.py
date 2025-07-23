# src/etl/data_processor.py
"""
Complete Data Processing Pipeline for Streaming Analytics Platform
Integrates parsing, validation, standardization, and database storage
"""

import os
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

import pandas as pd
from sqlalchemy.exc import IntegrityError

# Import our custom modules (these would be in separate files)
from src.etl.parsers.enhanced_parser import EnhancedETLParser, ParseResult
from src.etl.validators.data_validator import StreamingDataValidator, ValidationResult
from src.database.models import (
    DatabaseManager, StreamingRecord, Artist, Track, Platform,
    DataProcessingLog, QualityScore
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result from processing a single file"""
    success: bool
    file_path: str
    platform: str
    records_processed: int = 0
    records_failed: int = 0
    quality_score: float = 0.0
    processing_time: float = 0.0
    error_message: Optional[str] = None
    validation_result: Optional[ValidationResult] = None


class StreamingDataProcessor:
    """
    Complete data processing pipeline that handles:
    1. File parsing with platform-specific logic
    2. Data validation and quality scoring
    3. Data standardization and transformation
    4. Database storage with deduplication
    5. Processing audit trail
    """
    
    def __init__(self, database_manager: DatabaseManager):
        self.db_manager = database_manager
        self.parser = EnhancedETLParser()
        self.validator = StreamingDataValidator()
        self.standardizer = DataStandardizer()
    
    def process_file(self, file_path: Path, force_reprocess: bool = False) -> ProcessingResult:
        """Process a single streaming data file"""
        start_time = datetime.utcnow()
        file_path = Path(file_path)
        
        logger.info(f"Processing file: {file_path}")
        
        # Calculate file hash for deduplication
        file_hash = self._calculate_file_hash(file_path)
        
        # Check if file was already processed
        if not force_reprocess and self._is_file_processed(file_hash):
            logger.info(f"File already processed, skipping: {file_path}")
            return ProcessingResult(
                success=True,
                file_path=str(file_path),
                platform="unknown",
                error_message="File already processed"
            )
        
        try:
            # Step 1: Parse the file
            parse_result = self.parser.parse_file(file_path)
            if not parse_result.success:
                return self._create_failed_result(
                    file_path, "unknown", f"Parsing failed: {parse_result.error_message}"
                )
            
            platform = self.parser.detect_platform(file_path)
            if not platform:
                return self._create_failed_result(
                    file_path, "unknown", "Could not detect platform"
                )
            
            logger.info(f"Parsed {parse_result.records_parsed} records from {platform}")
            
            # Step 2: Validate the data
            validation_result = self.validator.validate_dataset(
                parse_result.data, platform, str(file_path)
            )
            
            logger.info(f"Quality score: {validation_result.overall_score:.1f}/100")
            
            # Step 3: Check if quality meets threshold
            quality_threshold = float(os.getenv('QUALITY_THRESHOLD', '70'))
            if validation_result.overall_score < quality_threshold:
                logger.warning(f"Quality score {validation_result.overall_score:.1f} below threshold {quality_threshold}")
                # Continue processing but log the issue
            
            # Step 4: Standardize the data
            standardized_data = self.standardizer.standardize_dataset(
                parse_result.data, platform
            )
            
            # Step 5: Store in database
            records_stored, records_failed = self._store_data(
                standardized_data, platform, file_hash, str(file_path)
            )
            
            # Step 6: Log processing results
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            self._log_processing_result(
                file_path, file_hash, platform, parse_result, 
                validation_result, processing_time, records_stored, records_failed
            )
            
            logger.info(f"Successfully processed {records_stored} records in {processing_time:.2f}s")
            
            return ProcessingResult(
                success=True,
                file_path=str(file_path),
                platform=platform,
                records_processed=records_stored,
                records_failed=records_failed,
                quality_score=validation_result.overall_score,
                processing_time=processing_time,
                validation_result=validation_result
            )
            
        except Exception as e:
            logger.error(f"Processing failed for {file_path}: {str(e)}")
            return self._create_failed_result(file_path, platform or "unknown", str(e))
    
    def process_directory(self, directory_path: Path, file_pattern: str = "*") -> List[ProcessingResult]:
        """Process all files in a directory matching the pattern"""
        directory_path = Path(directory_path)
        results = []
        
        if not directory_path.exists():
            logger.error(f"Directory not found: {directory_path}")
            return results
        
        # Find all matching files
        matching_files = list(directory_path.glob(file_pattern))
        if not matching_files:
            logger.warning(f"No files found matching pattern '{file_pattern}' in {directory_path}")
            return results
        
        logger.info(f"Processing {len(matching_files)} files from {directory_path}")
        
        # Process each file
        for file_path in matching_files:
            if file_path.is_file():
                result = self.process_file(file_path)
                results.append(result)
            else:
                logger.warning(f"Skipping non-file: {file_path}")
        
        # Summary
        successful = sum(1 for r in results if r.success)
        total_records = sum(r.records_processed for r in results if r.success)
        avg_quality = sum(r.quality_score for r in results if r.success and r.quality_score > 0) / max(successful, 1)
        
        logger.info(f"Batch processing complete: {successful}/{len(results)} files successful, "
                   f"{total_records:,} total records, avg quality: {avg_quality:.1f}")
        
        return results
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of file for deduplication"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            return ""
    
    def _is_file_processed(self, file_hash: str) -> bool:
        """Check if file was already processed based on hash"""
        if not file_hash:
            return False
            
        with self.db_manager.get_session() as session:
            existing = session.query(DataProcessingLog).filter(
                DataProcessingLog.file_hash == file_hash,
                DataProcessingLog.processing_status == 'completed'
            ).first()
            return existing is not None
    
    def _store_data(self, data: List[Dict], platform: str, file_hash: str, file_path: str) -> Tuple[int, int]:
        """Store standardized data in database"""
        records_stored = 0
        records_failed = 0
        
        # Get platform record
        platform_record = self.db_manager.get_platform_by_code(platform)
        if not platform_record:
            raise ValueError(f"Platform not found: {platform}")
        
        with self.db_manager.get_session() as session:
            for record_data in data:
                try:
                    # Create or get artist
                    artist = self._get_or_create_artist(session, record_data.get('artist_name'))
                    
                    # Create or get track
                    track = self._get_or_create_track(
                        session, record_data.get('track_title'), 
                        record_data.get('isrc'), artist.id if artist else None
                    )
                    
                    # Create streaming record
                    streaming_record = StreamingRecord(
                        date=record_data.get('date'),
                        platform_id=platform_record.id,
                        track_id=track.id if track else None,
                        metric_type=record_data.get('metric_type', 'streams'),
                        metric_value=record_data.get('metric_value', 0),
                        geography=record_data.get('geography'),
                        device_type=record_data.get('device_type'),
                        subscription_type=record_data.get('subscription_type'),
                        context_type=record_data.get('context_type'),
                        user_demographic=record_data.get('user_demographic'),
                        data_quality_score=record_data.get('data_quality_score'),
                        raw_data_source=file_path,
                        file_hash=file_hash
                    )
                    
                    session.add(streaming_record)
                    records_stored += 1
                    
                except Exception as e:
                    logger.error(f"Failed to store record: {e}")
                    records_failed += 1
                    session.rollback()
                    continue
            
            session.commit()
        
        return records_stored, records_failed
    
    def _get_or_create_artist(self, session, artist_name: str) -> Optional[Artist]:
        """Get existing artist or create new one"""
        if not artist_name:
            return None
            
        # Normalize name for matching
        normalized_name = self._normalize_string(artist_name)
        
        # Try to find existing artist
        existing = session.query(Artist).filter(
            Artist.name_normalized == normalized_name
        ).first()
        
        if existing:
            return existing
        
        # Create new artist
        new_artist = Artist(
            name=artist_name,
            name_normalized=normalized_name
        )
        session.add(new_artist)
        session.flush()  # Get the ID
        
        return new_artist
    
    def _get_or_create_track(self, session, track_title: str, isrc: str, artist_id: int) -> Optional[Track]:
        """Get existing track or create new one"""
        if not track_title:
            return None
        
        # Try to find by ISRC first
        if isrc:
            existing = session.query(Track).filter(Track.isrc == isrc).first()
            if existing:
                return existing
        
        # Try to find by title and artist
        normalized_title = self._normalize_string(track_title)
        existing = session.query(Track).filter(
            Track.title_normalized == normalized_title,
            Track.artist_id == artist_id
        ).first()
        
        if existing:
            return existing
        
        # Create new track
        new_track = Track(
            title=track_title,
            title_normalized=normalized_title,
            isrc=isrc,
            artist_id=artist_id
        )
        session.add(new_track)
        session.flush()  # Get the ID
        
        return new_track
    
    def _normalize_string(self, text: str) -> str:
        """Normalize string for matching (lowercase, no extra spaces)"""
        if not text:
            return ""
        return " ".join(text.lower().split())
    
    def _log_processing_result(self, file_path: Path, file_hash: str, platform: str,
                             parse_result: ParseResult, validation_result: ValidationResult,
                             processing_time: float, records_stored: int, records_failed: int):
        """Log processing results to database"""
        
        platform_record = self.db_manager.get_platform_by_code(platform)
        if not platform_record:
            return
        
        with self.db_manager.get_session() as session:
            # Create processing log
            log_entry = DataProcessingLog(
                file_path=str(file_path),
                file_hash=file_hash,
                platform_id=platform_record.id,
                processing_status='completed',
                records_processed=records_stored,
                records_failed=records_failed,
                quality_score=validation_result.overall_score,
                processing_config={
                    'parser_config': {
                        'encoding_detected': parse_result.encoding_detected,
                        'format_detected': parse_result.format_detected
                    },
                    'validation_config': {
                        'rules_passed': validation_result.passed_rules,
                        'total_rules': validation_result.total_rules
                    }
                },
                performance_metrics={
                    'processing_time_seconds': processing_time,
                    'records_per_second': records_stored / max(processing_time, 0.001),
                    'file_size_bytes': file_path.stat().st_size if file_path.exists() else 0
                },
                completed_at=datetime.utcnow()
            )
            session.add(log_entry)
            
            # Create quality score record
            quality_record = QualityScore(
                platform_id=platform_record.id,
                file_hash=file_hash,
                overall_score=validation_result.overall_score,
                completeness_score=validation_result.completeness_score,
                consistency_score=validation_result.consistency_score,
                validity_score=validation_result.validity_score,
                quality_details={
                    'issues': [
                        {
                            'rule_name': issue.rule_name,
                            'severity': issue.severity.value,
                            'message': issue.message,
                            'column': issue.column,
                            'row_count': issue.row_count
                        }
                        for issue in validation_result.issues
                    ]
                },
                validation_results=validation_result.metrics
            )
            session.add(quality_record)
            
            session.commit()
    
    def _create_failed_result(self, file_path: Path, platform: str, error_message: str) -> ProcessingResult:
        """Create a failed processing result"""
        return ProcessingResult(
            success=False,
            file_path=str(file_path),
            platform=platform,
            error_message=error_message
        )


class DataStandardizer:
    """
    Transforms platform-specific data into standardized format
    """
    
    def __init__(self):
        self.column_mappings = self._load_column_mappings()
    
    def _load_column_mappings(self) -> Dict[str, Dict[str, str]]:
        """Load platform-specific column mappings to standardized schema"""
        return {
            "apl-apple": {
                "artist_name": "artist_name",
                "song_name": "track_title", 
                "report_date": "date",
                "quantity": "metric_value",
                "customer_currency": "geography",  # Approximate mapping
            },
            "fbk-facebook": {
                "isrc": "isrc",
                "date": "date",
                "plays": "metric_value",
                "product_type": "context_type",
            },
            "scu-soundcloud": {
                "track_title": "track_title",
                "artist_name": "artist_name", 
                "timestamp": "date",
                "plays": "metric_value",
                "playlist_type": "context_type",
            },
            "spo-spotify": {
                "track_name": "track_title",
                "artist_name": "artist_name",
                "streams": "metric_value",
                "date": "date",
            },
            # Add more mappings as needed
        }
    
    def standardize_dataset(self, df: pd.DataFrame, platform: str) -> List[Dict]:
        """Transform dataset to standardized format"""
        mappings = self.column_mappings.get(platform, {})
        standardized_records = []
        
        for _, row in df.iterrows():
            record = self._standardize_record(row, mappings, platform)
            if record:
                standardized_records.append(record)
        
        return standardized_records
    
    def _standardize_record(self, row: pd.Series, mappings: Dict[str, str], platform: str) -> Optional[Dict]:
        """Transform a single record to standardized format"""
        try:
            standardized = {
                'platform_code': platform,
                'metric_type': 'streams',  # Default
                'data_quality_score': 85.0,  # Default, should be calculated
            }
            
            # Apply column mappings
            for source_col, target_col in mappings.items():
                if source_col in row.index and pd.notna(row[source_col]):
                    standardized[target_col] = row[source_col]
            
            # Set defaults for required fields
            if 'date' not in standardized:
                standardized['date'] = datetime.utcnow().date()
            
            if 'metric_value' not in standardized:
                standardized['metric_value'] = 0
            
            return standardized
            
        except Exception as e:
            logger.error(f"Failed to standardize record: {e}")
            return None


# Command line interface and testing
def main():
    """Main function for command line usage"""
    import argparse
    from dotenv import load_dotenv
    
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='Process streaming data files')
    parser.add_argument('path', help='File or directory path to process')
    parser.add_argument('--pattern', default='*', help='File pattern for directory processing')
    parser.add_argument('--force', action='store_true', help='Force reprocessing of already processed files')
    parser.add_argument('--db-url', help='Database URL (overrides env var)')
    
    args = parser.parse_args()
    
    # Initialize database
    db_url = args.db_url or os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not provided")
        return
    
    try:
        db_manager = DatabaseManager(db_url)
        processor = StreamingDataProcessor(db_manager)
        
        path = Path(args.path)
        
        if path.is_file():
            # Process single file
            result = processor.process_file(path, args.force)
            if result.success:
                print(f"Successfully processed {result.records_processed} records")
                print(f"   Quality Score: {result.quality_score:.1f}")
                print(f"   Processing Time: {result.processing_time:.2f}s")
            else:
                print(f"Processing failed: {result.error_message}")
        
        elif path.is_dir():
            # Process directory
            results = processor.process_directory(path, args.pattern)
            
            # Print summary
            successful = sum(1 for r in results if r.success)
            total_records = sum(r.records_processed for r in results if r.success)
            
            print(f"\nPROCESSING SUMMARY")
            print(f"Files processed: {successful}/{len(results)}")
            print(f"Total records: {total_records:,}")
            
            if successful > 0:
                avg_quality = sum(r.quality_score for r in results if r.success) / successful
                print(f"Average quality score: {avg_quality:.1f}")
            
            # Show failed files
            failed = [r for r in results if not r.success]
            if failed:
                print(f"\nFAILED FILES ({len(failed)}):")
                for result in failed:
                    print(f"  - {result.file_path}: {result.error_message}")
        
        else:
            print(f"Path not found: {path}")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()