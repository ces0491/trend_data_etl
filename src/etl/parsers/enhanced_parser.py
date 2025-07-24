# src/etl/parsers/enhanced_parser.py - FIXED VERSION
"""
Enhanced ETL Parser System for Streaming Analytics Platform - FIXED

Key fixes:
1. Fixed type annotations and None handling
2. Better delimiter detection
3. Enhanced CSV format handling
4. Fixed date parsing logic
5. Proper error handling for edge cases
"""

import csv
import io
import re
import chardet
import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict, Any

import pandas as pd
from dateutil import parser as date_parser

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PlatformCode(Enum):
    """Platform codes from real data analysis"""
    APPLE = "apl-apple"
    AWA = "awa-awa" 
    BOOMPLAY = "boo-boomplay"
    DEEZER = "dzr-deezer"
    FACEBOOK = "fbk-facebook"
    PELOTON = "plt-peloton"
    SOUNDCLOUD = "scu-soundcloud"
    SPOTIFY = "spo-spotify"
    VEVO = "vvo-vevo"


@dataclass
class ParseResult:
    """Result from parsing operation"""
    success: bool
    data: Optional[pd.DataFrame] = None
    error_message: Optional[str] = None
    quality_score: float = 0.0
    records_parsed: int = 0
    records_failed: int = 0
    encoding_detected: Optional[str] = None
    format_detected: Optional[str] = None


class EnhancedETLParser:
    """
    Enhanced parser that handles real-world streaming data format inconsistencies - FIXED
    """
    
    def __init__(self):
        self.date_formats = [
            "%Y-%m-%d",           # Standard ISO: 2024-12-01
            "%m/%d/%y",           # Apple short: 12/01/24  
            "%Y-%m-%d %H:%M:%S",  # With time: 2024-12-01 17:18:10
            "%Y-%m-%d %H:%M:%S.%f%z",  # SoundCloud: 2024-12-01 17:18:10.040+00
            "%d/%m/%Y",           # Boomplay European: 01/12/2024
            "%Y%m%d",             # AWA compact: 20241201
            "%m/%d/%Y",           # US format: 12/01/2024
        ]
        
        self.platform_configs = self._load_platform_configs()
    
    def _load_platform_configs(self) -> Dict[str, Dict[str, Any]]:
        """Load platform-specific parsing configurations"""
        return {
            PlatformCode.APPLE.value: {
                "delimiter": "\t",
                "quote_wrapped": True,
                "encoding_priority": ["utf-8", "cp1252", "latin1"],
                "date_columns": ["report_date", "period_start", "period_end"],
                "expected_columns": ["vendor_identifier", "customer_identifier", "report_date"],
                "numeric_columns": ["quantity", "price", "proceeds"],
            },
            PlatformCode.FACEBOOK.value: {
                "delimiter": ",",
                "quoting": csv.QUOTE_ALL,
                "encoding_priority": ["utf-8", "cp1252"],
                "date_columns": ["date"],
                "expected_columns": ["isrc", "date", "product_type"],
                "numeric_columns": ["plays", "interactions"],
            },
            PlatformCode.SOUNDCLOUD.value: {
                "delimiter": "\t",
                "encoding_priority": ["utf-8"],
                "date_columns": ["timestamp", "created_at"],
                "expected_columns": ["track_id", "user_id", "timestamp"],
                "numeric_columns": ["duration", "plays"],
            },
            PlatformCode.BOOMPLAY.value: {
                "delimiter": "\t",
                "encoding_priority": ["utf-8", "cp1252"],
                "date_columns": ["date"],
                "date_format": "%d/%m/%Y",  # European format
                "expected_columns": ["song_id", "country", "date"],
                "numeric_columns": ["streams", "duration"],
            },
            PlatformCode.AWA.value: {
                "delimiter": "\t", 
                "encoding_priority": ["utf-8", "shift_jis"],
                "date_columns": ["date"],
                "date_format": "%Y%m%d",  # Compact format
                "expected_columns": ["track_id", "prefecture", "date"],
                "numeric_columns": ["plays", "users"],
            },
            PlatformCode.SPOTIFY.value: {
                "delimiter": "\t",
                "encoding_priority": ["utf-8"],
                "date_columns": ["date", "week"],
                "expected_columns": ["track_name", "artist_name", "streams"],
                "numeric_columns": ["streams", "stream_share"],
            },
            PlatformCode.VEVO.value: {
                "delimiter": ",",  # Vevo uses CSV
                "encoding_priority": ["utf-8"],
                "date_columns": ["date"],
                "expected_columns": ["video_id", "views", "date"],
                "numeric_columns": ["views", "watch_time"],
            },
            PlatformCode.PELOTON.value: {
                "delimiter": "\t",
                "encoding_priority": ["utf-8"],
                "date_columns": ["date"],
                "expected_columns": ["track_id", "class_id", "plays"],
                "numeric_columns": ["plays", "duration"],
            },
            PlatformCode.DEEZER.value: {
                "delimiter": ",",  # Deezer uses CSV based on sample
                "encoding_priority": ["utf-8", "cp1252"],
                "date_columns": ["date"],
                "expected_columns": ["isrc", "track_name", "streams"],
                "numeric_columns": ["streams"],
            }
        }
    
    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding with fallback strategy"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                result = chardet.detect(raw_data)
                encoding = result.get('encoding') if result else None
                confidence = result.get('confidence', 0.0) if result else 0.0
                
                logger.debug(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
                
                if encoding and confidence > 0.7:
                    return encoding
                else:
                    logger.debug(f"Low confidence in encoding detection, using utf-8")
                    return 'utf-8'
                    
        except Exception as e:
            logger.error(f"Encoding detection failed: {e}")
            return 'utf-8'
    
    def detect_platform(self, file_path: Path) -> Optional[str]:
        """Detect platform from file path and name"""
        path_str = str(file_path).lower()
        
        # Platform detection from path
        platform_patterns = {
            PlatformCode.APPLE.value: ['apple', 'apl-apple', 'itunes'],
            PlatformCode.FACEBOOK.value: ['facebook', 'fbk-facebook', 'meta'],
            PlatformCode.SOUNDCLOUD.value: ['soundcloud', 'scu-soundcloud'],
            PlatformCode.SPOTIFY.value: ['spotify', 'spo-spotify'],
            PlatformCode.BOOMPLAY.value: ['boomplay', 'boo-boomplay'],
            PlatformCode.AWA.value: ['awa', 'awa-awa'],
            PlatformCode.VEVO.value: ['vevo', 'vvo-vevo'],
            PlatformCode.PELOTON.value: ['peloton', 'plt-peloton'],
            PlatformCode.DEEZER.value: ['deezer', 'dzr-deezer'],
        }
        
        for platform, patterns in platform_patterns.items():
            if any(pattern in path_str for pattern in patterns):
                return platform
                
        logger.warning(f"Could not detect platform from path: {file_path}")
        return None
    
    def _detect_delimiter(self, file_path: Path, encoding: str, platform: str) -> str:
        """Detect the actual delimiter used in the file"""
        config = self.platform_configs.get(platform, {})
        expected_delimiter = config.get('delimiter', '\t')
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                # Read first few lines to detect delimiter
                sample_lines = []
                for i, line in enumerate(f):
                    if i >= 5:  # Only check first 5 lines
                        break
                    sample_lines.append(line.strip())
            
            if not sample_lines:
                return expected_delimiter
            
            # Test different delimiters
            delimiters = ['\t', ',', ';', '|']
            delimiter_scores = {}
            
            for delimiter in delimiters:
                score = 0
                for line in sample_lines:
                    if delimiter in line:
                        parts = line.split(delimiter)
                        if len(parts) > 1:  # Must split into multiple parts
                            score += len(parts)
                
                delimiter_scores[delimiter] = score
            
            # Choose delimiter with highest score - Fixed max() usage
            if delimiter_scores:
                best_delimiter = max(delimiter_scores.keys(), key=lambda k: delimiter_scores[k])
                
                # Only use detected delimiter if it actually splits the data
                if delimiter_scores[best_delimiter] > 0:
                    logger.debug(f"Detected delimiter: '{best_delimiter}' (score: {delimiter_scores[best_delimiter]})")
                    return best_delimiter
            
            logger.debug(f"Using expected delimiter: '{expected_delimiter}'")
            return expected_delimiter
                
        except Exception as e:
            logger.debug(f"Delimiter detection failed: {e}, using expected: '{expected_delimiter}'")
            return expected_delimiter
    
    def _parse_apple_format(self, file_path: Path, encoding: str) -> ParseResult:
        """Handle Apple's quote-wrapped tab-delimited format - FIXED"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # Split into lines
            lines = content.strip().split('\n')
            if not lines:
                return ParseResult(success=False, error_message="Empty file")
            
            processed_lines = []
            
            for line in lines:
                line = line.strip()
                
                # Handle quote-wrapped lines
                if line.startswith('"') and line.endswith('"'):
                    # Remove outer quotes
                    line = line[1:-1]
                    
                    # Handle escaped quotes within the content
                    line = line.replace('""', '"')
                
                processed_lines.append(line)
            
            # Join back and parse as TSV
            processed_content = '\n'.join(processed_lines)
            
            # Use StringIO to parse as TSV
            df = pd.read_csv(
                io.StringIO(processed_content),
                delimiter='\t',
                on_bad_lines='skip',
                dtype=str  # Keep everything as string initially
            )
            
            logger.debug(f"Apple parsing: {len(df)} rows, {len(df.columns)} columns")
            logger.debug(f"Columns: {list(df.columns)}")
            
            return ParseResult(
                success=True,
                data=df,
                records_parsed=len(df),
                encoding_detected=encoding,
                format_detected="apple_quote_wrapped_tsv"
            )
            
        except Exception as e:
            logger.error(f"Apple format parsing failed: {str(e)}")
            return ParseResult(
                success=False,
                error_message=f"Apple format parsing failed: {str(e)}"
            )
    
    def _parse_facebook_format(self, file_path: Path, encoding: str) -> ParseResult:
        """Handle Facebook's quoted CSV format - FIXED"""
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                quoting=csv.QUOTE_ALL,
                on_bad_lines='skip',
                dtype=str
            )
            
            logger.debug(f"Facebook parsing: {len(df)} rows, {len(df.columns)} columns")
            
            return ParseResult(
                success=True,
                data=df,
                records_parsed=len(df),
                encoding_detected=encoding,
                format_detected="facebook_quoted_csv"
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                error_message=f"Facebook format parsing failed: {str(e)}"
            )
    
    def _parse_standard_format(self, file_path: Path, platform: str, encoding: str) -> ParseResult:
        """Handle standard TSV/CSV formats - FIXED"""
        try:
            # Detect actual delimiter
            delimiter = self._detect_delimiter(file_path, encoding, platform)
            
            logger.debug(f"Using delimiter: '{delimiter}'")
            
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                on_bad_lines='skip',
                dtype=str
            )
            
            logger.debug(f"Standard parsing: {len(df)} rows, {len(df.columns)} columns")
            logger.debug(f"Columns: {list(df.columns)}")
            
            format_name = f"standard_{'csv' if delimiter == ',' else 'tsv'}"
            
            return ParseResult(
                success=True,
                data=df,
                records_parsed=len(df),
                encoding_detected=encoding,
                format_detected=format_name
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                error_message=f"Standard format parsing failed: {str(e)}"
            )
    
    def _standardize_dates(self, df: Optional[pd.DataFrame], platform: str) -> Optional[pd.DataFrame]:
        """Standardize date formats based on platform-specific patterns - IMPROVED"""
        if df is None:
            return None
            
        config = self.platform_configs.get(platform, {})
        date_columns = config.get('date_columns', [])
        
        # Find actual date columns in the data
        actual_date_columns = []
        for col in df.columns:
            col_lower = col.lower()
            
            # Platform-specific logic
            if platform == PlatformCode.VEVO.value:
                # For Vevo, only process columns named exactly 'date'
                if col_lower == 'date':
                    actual_date_columns.append(col)
            else:
                # For other platforms, use broader matching
                if any(date_col.lower() in col_lower for date_col in date_columns):
                    actual_date_columns.append(col)
                elif any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
                    # Exclude numeric columns that might have 'time' in name
                    if not any(numeric_word in col_lower for numeric_word in ['watch_time', 'duration', 'seconds', 'minutes']):
                        actual_date_columns.append(col)
        
        for col in actual_date_columns:
            df[col] = self._parse_date_column(df[col], platform)
        
        return df
    
    def _parse_date_column(self, series: pd.Series, platform: str) -> pd.Series:
        """Parse date column with multiple format attempts - FIXED"""
        parsed_dates = []
        
        for value in series:
            if pd.isna(value) or value == '':
                parsed_dates.append(None)
                continue
                
            # Convert to string and clean
            value_str = str(value).strip()
            
            # Try platform-specific format first
            config = self.platform_configs.get(platform, {})
            platform_format = config.get('date_format')
            
            if platform_format:
                try:
                    parsed_date = datetime.strptime(value_str, platform_format)
                    parsed_dates.append(parsed_date)
                    continue
                except:
                    pass
            
            # Try all known formats
            parsed = None
            for fmt in self.date_formats:
                try:
                    parsed = datetime.strptime(value_str, fmt)
                    break
                except:
                    continue
            
            # Last resort: use dateutil parser
            if not parsed:
                try:
                    parsed = date_parser.parse(value_str)
                except:
                    logger.warning(f"Could not parse date: {value}")
                    parsed = None
            
            parsed_dates.append(parsed)
        
        return pd.Series(parsed_dates)
    
    def _calculate_quality_score(self, df: Optional[pd.DataFrame], platform: str) -> float:
        """Calculate data quality score (0-100)"""
        if df is None or df.empty:
            return 0.0
        
        config = self.platform_configs.get(platform, {})
        expected_columns = config.get('expected_columns', [])
        
        scores = []
        
        # Column completeness (30% weight)
        if expected_columns:
            present_columns = sum(1 for col in expected_columns if col in df.columns)
            column_score = (present_columns / len(expected_columns)) * 100
            scores.append(column_score * 0.3)
        else:
            # If no expected columns defined, give full score
            scores.append(30.0)
        
        # Data completeness (40% weight)  
        if len(df.columns) > 0:
            non_null_ratio = (df.count().sum() / (len(df) * len(df.columns)))
            completeness_score = non_null_ratio * 100
        else:
            completeness_score = 0
        scores.append(completeness_score * 0.4)
        
        # Data consistency (30% weight)
        consistency_score = 100  # Base score
        
        # Check if we have multiple columns (not a parsing failure)
        if len(df.columns) == 1 and len(expected_columns) > 1:
            consistency_score -= 50  # Major penalty for single column when expecting multiple
        
        # Check numeric columns
        numeric_columns = config.get('numeric_columns', [])
        for col in numeric_columns:
            if col in df.columns:
                try:
                    pd.to_numeric(df[col], errors='coerce')
                except:
                    consistency_score -= 10
        
        scores.append(min(consistency_score, 100) * 0.3)
        
        return sum(scores)
    
    def parse_file(self, file_path: Path) -> ParseResult:
        """Main parsing method that handles all platform formats - FIXED"""
        if not file_path.exists():
            return ParseResult(
                success=False,
                error_message=f"File not found: {file_path}"
            )
        
        # Detect platform and encoding
        platform = self.detect_platform(file_path)
        if not platform:
            return ParseResult(
                success=False,
                error_message="Could not detect platform from file path"
            )
        
        encoding = self.detect_encoding(file_path)
        
        logger.info(f"Parsing {file_path.name} as {platform} with encoding {encoding}")
        
        # Use platform-specific parsing logic
        if platform == PlatformCode.APPLE.value:
            result = self._parse_apple_format(file_path, encoding)
        elif platform == PlatformCode.FACEBOOK.value:
            result = self._parse_facebook_format(file_path, encoding)
        else:
            result = self._parse_standard_format(file_path, platform, encoding)
        
        if not result.success:
            return result
        
        # Standardize dates
        try:
            result.data = self._standardize_dates(result.data, platform)
        except Exception as e:
            logger.warning(f"Date standardization failed: {e}")
        
        # Calculate quality score
        result.quality_score = self._calculate_quality_score(result.data, platform)
        
        logger.info(f"Parsing complete: {result.records_parsed} records, "
                   f"quality score: {result.quality_score:.1f}")
        
        return result


# Example usage and testing
if __name__ == "__main__":
    parser = EnhancedETLParser()
    
    # Test with sample file (replace with actual path)
    sample_file = Path("data/sample/spo-spotify_test_20241201.tsv")
    if sample_file.exists():
        result = parser.parse_file(sample_file)
        
        if result.success:
            print(f"Successfully parsed {result.records_parsed} records")
            print(f"Quality Score: {result.quality_score:.1f}")
            print(f"Format: {result.format_detected}")
            print(f"Encoding: {result.encoding_detected}")
            if result.data is not None:
                print("\nColumns:", list(result.data.columns))
                print("\nSample data:")
                print(result.data.head())
        else:
            print(f"Parsing failed: {result.error_message}")