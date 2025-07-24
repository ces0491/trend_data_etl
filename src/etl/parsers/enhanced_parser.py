# src/etl/parsers/enhanced_parser.py
"""
from __future__ import annotations

Enhanced ETL Parser System for Streaming Analytics Platform

Handles real-world data format inconsistencies identified from sample data analysis:
- Apple: Quote-wrapped tab-delimited rows  
- Facebook: Quoted CSV format
- Multiple date formats across platforms
- Various encodings and delimiters
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
    data: pd.DataFrame | None = None
    error_message: str | None = None
    quality_score: float = 0.0
    records_parsed: int = 0
    records_failed: int = 0
    encoding_detected: str | None = None
    format_detected: str | None = None


class EnhancedETLParser:
    """
    Enhanced parser that handles real-world streaming data format inconsistencies
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
    
    def _load_platform_configs(self) -> dict[str, dict]:
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
                "delimiter": "\t",
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
                "delimiter": "\t",
                "encoding_priority": ["utf-8", "cp1252"],
                "date_columns": ["date"],
                "expected_columns": ["isrc", "track_name", "streams"],
                "numeric_columns": ["streams"],
            }
        }
    
    def detect_encoding(self, file_path: str | Path) -> str:
        """Detect file encoding with fallback strategy"""
        file_path = Path(file_path)
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                result = chardet.detect(raw_data)
                encoding = result.get('encoding') if result else None
                confidence = result.get('confidence', 0.0) if result else 0.0
                
                logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
                
                if encoding and confidence > 0.7:
                    return encoding
                else:
                    logger.warning(f"Low confidence in encoding detection, using utf-8")
                    return 'utf-8'
                    
        except Exception as e:
            logger.error(f"Encoding detection failed: {e}")
            return 'utf-8'
    
    def detect_platform(self, file_path: str | Path) -> str | None:
        """Detect platform from file path and name"""
        file_path = Path(file_path)
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
    
    def _parse_apple_format(self, file_path: Path, encoding: str) -> ParseResult:
        """Handle Apple's quote-wrapped tab-delimited format"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                lines = f.readlines()
            
            # Remove quote wrapping from each line
            processed_lines = []
            for line in lines:
                line = line.strip()
                if line.startswith('"') and line.endswith('"'):
                    # Remove outer quotes and process
                    line = line[1:-1]
                processed_lines.append(line)
            
            # Parse as TSV
            df = pd.read_csv(
                io.StringIO('\n'.join(processed_lines)),
                delimiter='\t',
                encoding=encoding,
                on_bad_lines='skip'
            )
            
            return ParseResult(
                success=True,
                data=df,
                records_parsed=len(df),
                encoding_detected=encoding,
                format_detected="apple_quote_wrapped_tsv"
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                error_message=f"Apple format parsing failed: {str(e)}"
            )
    
    def _parse_facebook_format(self, file_path: Path, encoding: str) -> ParseResult:
        """Handle Facebook's quoted CSV format"""
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                quoting=csv.QUOTE_ALL,
                on_bad_lines='skip'
            )
            
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
        """Handle standard TSV/CSV formats"""
        try:
            config = self.platform_configs.get(platform, {})
            delimiter = config.get('delimiter', '\t')
            
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                on_bad_lines='skip'
            )
            
            return ParseResult(
                success=True,
                data=df,
                records_parsed=len(df),
                encoding_detected=encoding,
                format_detected=f"standard_{delimiter == ',' and 'csv' or 'tsv'}"
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                error_message=f"Standard format parsing failed: {str(e)}"
            )
    
    def _standardize_dates(self, df: pd.DataFrame | None, platform: str) -> pd.DataFrame | None:
        """Standardize date formats based on platform-specific patterns"""
        if df is None:
            return None
            
        config = self.platform_configs.get(platform, {})
        date_columns = config.get('date_columns', [])
        
        for col in date_columns:
            if col in df.columns:
                df[col] = self._parse_date_column(df[col], platform)
        
        return df
    
    def _parse_date_column(self, series: pd.Series, platform: str) -> pd.Series:
        """Parse date column with multiple format attempts"""
        parsed_dates = []
        
        for value in series:
            if pd.isna(value):
                parsed_dates.append(None)
                continue
                
            # Try platform-specific format first
            config = self.platform_configs.get(platform, {})
            platform_format = config.get('date_format')
            
            if platform_format:
                try:
                    parsed_date = datetime.strptime(str(value), platform_format)
                    parsed_dates.append(parsed_date)
                    continue
                except:
                    pass
            
            # Try all known formats
            parsed = None
            for fmt in self.date_formats:
                try:
                    parsed = datetime.strptime(str(value), fmt)
                    break
                except:
                    continue
            
            # Last resort: use dateutil parser
            if not parsed:
                try:
                    parsed = date_parser.parse(str(value))
                except:
                    logger.warning(f"Could not parse date: {value}")
                    parsed = None
            
            parsed_dates.append(parsed)
        
        return pd.Series(parsed_dates)
    
    def _calculate_quality_score(self, df: pd.DataFrame | None, platform: str) -> float:
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
        
        # Data completeness (40% weight)  
        non_null_ratio = (df.count().sum() / (len(df) * len(df.columns)))
        completeness_score = non_null_ratio * 100
        scores.append(completeness_score * 0.4)
        
        # Data consistency (30% weight)
        consistency_score = 100  # Base score
        
        # Check numeric columns
        numeric_columns = config.get('numeric_columns', [])
        for col in numeric_columns:
            if col in df.columns:
                try:
                    pd.to_numeric(df[col], errors='coerce')
                except:
                    consistency_score -= 20
        
        scores.append(min(consistency_score, 100) * 0.3)
        
        return sum(scores)
    
    def parse_file(self, file_path: str | Path) -> ParseResult:
        """Main parsing method that handles all platform formats"""
        file_path = Path(file_path)
        
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
        
        logger.info(f"Parsing {file_path} as {platform} with encoding {encoding}")
        
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
    sample_file = Path("data/sample/apple_sample.txt")
    if sample_file.exists():
        result = parser.parse_file(sample_file)
        
        if result.success:
            print(f"Successfully parsed {result.records_parsed} records")
            print(f"Quality Score: {result.quality_score:.1f}")
            print(f"Format: {result.format_detected}")
            print(f"Encoding: {result.encoding_detected}")
            if result.data is not None:
                print("\nSample data:")
                print(result.data.head())
        else:
            print(f"Parsing failed: {result.error_message}")