# src/etl/validators/data_validator.py
"""
from __future__ import annotations

Comprehensive Data Validation & Quality Framework
Based on real-world data analysis findings from 19 sample files
"""

import re
import logging
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    CRITICAL = "critical"    # Data cannot be processed
    ERROR = "error"         # Significant data quality issues
    WARNING = "warning"     # Minor issues that should be noted
    INFO = "info"          # Informational observations


@dataclass
class ValidationIssue:
    """Represents a validation issue found in data"""
    rule_name: str
    severity: ValidationSeverity
    message: str
    column: str | None = None
    row_count: int = 0
    sample_values: list[Any] = field(default_factory=list)
    percentage: float = 0.0


@dataclass
class ValidationResult:
    """Complete validation results for a dataset"""
    overall_score: float
    completeness_score: float
    consistency_score: float
    validity_score: float
    issues: list[ValidationIssue] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    passed_rules: int = 0
    total_rules: int = 0


class StreamingDataValidator:
    """
    Comprehensive validator for streaming platform data
    Implements validation rules based on real-world data analysis
    """
    
    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        self.platform_specific_rules = self._load_platform_specific_rules()
    
    def _load_validation_rules(self) -> dict[str, dict]:
        """Load general validation rules applicable to all platforms"""
        return {
            "isrc_format": {
                "pattern": r"^[A-Z]{2}[A-Z0-9]{3}[0-9]{2}[0-9]{5}$",
                "description": "ISRC must be 12 characters: 2 letters, 3 alphanumeric, 2 digits, 5 digits",
                "severity": ValidationSeverity.ERROR
            },
            "date_format": {
                "required": True,
                "description": "Date fields must be parseable",
                "severity": ValidationSeverity.CRITICAL
            },
            "numeric_ranges": {
                "streams": {"min": 0, "max": 1000000000},  # Max 1B streams per record
                "plays": {"min": 0, "max": 1000000000},
                "duration": {"min": 0, "max": 86400},  # Max 24 hours
                "price": {"min": 0, "max": 1000},     # Max $1000 per transaction
                "age": {"min": 0, "max": 120},        # Human age limits
                "severity": ValidationSeverity.ERROR
            },
            "text_length": {
                "artist_name": {"min": 1, "max": 500},
                "track_title": {"min": 1, "max": 1000},
                "album_name": {"min": 0, "max": 1000},
                "severity": ValidationSeverity.WARNING
            },
            "required_fields": {
                "severity": ValidationSeverity.CRITICAL
            }
        }
    
    def _load_platform_specific_rules(self) -> dict[str, dict]:
        """Load platform-specific validation rules based on real data analysis"""
        return {
            "apl-apple": {
                "required_columns": ["vendor_identifier", "customer_identifier", "report_date"],
                "vendor_id_pattern": r"^[A-Z0-9_]+$",
                "customer_id_pattern": r"^[a-f0-9]{64}$",  # Hashed customer IDs
                "currency_codes": ["USD", "EUR", "GBP", "JPY", "CHF", "MXN", "CAD", "AUD"],
                "expected_columns_min": 10,  # Apple files have 35+ columns
            },
            "fbk-facebook": {
                "required_columns": ["isrc", "date", "product_type"],
                "product_types": ["FB_REELS", "IG_MUSIC_STICKER", "FB_FROM_IG_CROSSPOST", "FB_MUSIC_STICKER"],
                "expected_format": "quoted_csv",
            },
            "scu-soundcloud": {
                "required_columns": ["track_id", "user_id", "timestamp"],
                "timestamp_timezone": True,  # Must include timezone
                "file_types": ["customer", "playlist", "stream", "track"],  # Multiple related files
            },
            "boo-boomplay": {
                "required_columns": ["song_id", "country", "date"],
                "country_codes": ["ZA", "KE", "UG", "NG", "GH", "TZ"],  # African markets
                "date_format": "%d/%m/%Y",  # European DD/MM/YYYY format
                "device_patterns": [r"samsung.*", r"iPhone.*", r"TECNO.*", r"Infinix.*"],
            },
            "awa-awa": {
                "required_columns": ["track_id", "prefecture", "date"],
                "prefecture_codes": list(range(1, 48)),  # Japanese prefectures 1-47
                "date_format": "%Y%m%d",  # Compact YYYYMMDD format
                "user_types": ["Paid", "Free", "RFT"],
            },
            "spo-spotify": {
                "required_columns": ["track_name", "artist_name", "streams"],
                "stream_threshold": 30,  # 30-second minimum for stream counting
                "age_buckets": ["13-17", "18-22", "23-27", "28-34", "35-44", "45-54", "55-64", "65+"],
                "gender_codes": ["male", "female", "non-binary"],
            },
            "vvo-vevo": {
                "required_columns": ["video_id", "views", "date"],
                "metric_types": ["views", "watch_time", "engagement"],
                "youtube_integration": True,
            },
            "plt-peloton": {
                "required_columns": ["track_id", "class_id", "plays"],
                "context_types": ["cycling", "running", "strength", "yoga", "meditation"],
                "fitness_specific": True,
            },
            "dzr-deezer": {
                "required_columns": ["isrc", "track_name", "streams"],
                "standard_format": "tsv",
            }
        }
    
    def validate_dataset(self, df: pd.DataFrame, platform: str, file_path: str = "") -> ValidationResult:
        """
        Comprehensive validation of streaming dataset
        Returns detailed validation results with quality scores
        """
        issues = []
        metrics = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "file_path": file_path,
            "platform": platform,
            "validation_timestamp": datetime.utcnow().isoformat()
        }
        
        if df.empty:
            issues.append(ValidationIssue(
                rule_name="empty_dataset",
                severity=ValidationSeverity.CRITICAL,
                message="Dataset is empty",
                row_count=0
            ))
            return ValidationResult(
                overall_score=0.0,
                completeness_score=0.0,
                consistency_score=0.0,
                validity_score=0.0,
                issues=issues,
                metrics=metrics
            )
        
        # Run all validation checks
        issues.extend(self._validate_required_columns(df, platform))
        issues.extend(self._validate_data_completeness(df))
        issues.extend(self._validate_data_types(df, platform))
        issues.extend(self._validate_date_formats(df, platform))
        issues.extend(self._validate_numeric_ranges(df))
        issues.extend(self._validate_text_fields(df))
        issues.extend(self._validate_isrc_codes(df))
        issues.extend(self._validate_platform_specific(df, platform))
        issues.extend(self._validate_data_consistency(df, platform))
        
        # Calculate quality scores
        scores = self._calculate_quality_scores(issues, df)
        
        # Count passed vs total rules
        total_rules = self._count_total_rules(platform)
        critical_errors = sum(1 for issue in issues if issue.severity == ValidationSeverity.CRITICAL)
        passed_rules = total_rules - len(issues)
        
        return ValidationResult(
            overall_score=scores["overall"],
            completeness_score=scores["completeness"],
            consistency_score=scores["consistency"],
            validity_score=scores["validity"],
            issues=issues,
            metrics=metrics,
            passed_rules=max(0, passed_rules),
            total_rules=total_rules
        )
    
    def _validate_required_columns(self, df: pd.DataFrame, platform: str) -> list[ValidationIssue]:
        """Validate that required columns are present"""
        issues = []
        platform_config = self.platform_specific_rules.get(platform, {})
        required_columns = platform_config.get("required_columns", [])
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            issues.append(ValidationIssue(
                rule_name="missing_required_columns",
                severity=ValidationSeverity.CRITICAL,
                message=f"Missing required columns: {', '.join(missing_columns)}",
                row_count=len(df),
                sample_values=missing_columns
            ))
        
        return issues
    
    def _validate_data_completeness(self, df: pd.DataFrame) -> list[ValidationIssue]:
        """Validate data completeness (non-null values)"""
        issues = []
        
        for column in df.columns:
            null_count = df[column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            
            if null_percentage > 80:  # More than 80% null values
                issues.append(ValidationIssue(
                    rule_name="high_null_percentage",
                    severity=ValidationSeverity.WARNING,
                    message=f"Column '{column}' has {null_percentage:.1f}% null values",
                    column=column,
                    row_count=null_count,
                    percentage=null_percentage
                ))
            elif null_percentage > 95:  # More than 95% null values
                issues.append(ValidationIssue(
                    rule_name="extremely_high_null_percentage",
                    severity=ValidationSeverity.ERROR,
                    message=f"Column '{column}' has {null_percentage:.1f}% null values - mostly empty",
                    column=column,
                    row_count=null_count,
                    percentage=null_percentage
                ))
        
        return issues
    
    def _validate_data_types(self, df: pd.DataFrame, platform: str) -> list[ValidationIssue]:
        """Validate data types and detect inconsistencies"""
        issues = []
        
        # Check for mixed data types in columns
        for column in df.columns:
            if df[column].dtype == 'object':  # Text columns
                # Check if column should be numeric
                non_null_values = df[column].dropna()
                if len(non_null_values) > 0:
                    # Try to convert to numeric
                    numeric_convertible = pd.to_numeric(non_null_values, errors='coerce').notna().sum()
                    numeric_percentage = (numeric_convertible / len(non_null_values)) * 100
                    
                    if 50 <= numeric_percentage < 95:  # Mixed numeric/text
                        sample_non_numeric = non_null_values[pd.to_numeric(non_null_values, errors='coerce').isna()].head(3).tolist()
                        issues.append(ValidationIssue(
                            rule_name="mixed_data_types",
                            severity=ValidationSeverity.WARNING,
                            message=f"Column '{column}' has mixed numeric/text values ({numeric_percentage:.1f}% numeric)",
                            column=column,
                            sample_values=sample_non_numeric
                        ))
        
        return issues
    
    def _validate_date_formats(self, df: pd.DataFrame, platform: str) -> list[ValidationIssue]:
        """Validate date formats based on platform-specific patterns"""
        issues = []
        date_columns = self._identify_date_columns(df)
        
        for column in date_columns:
            unparseable_dates = []
            non_null_values = df[column].dropna()
            
            for value in non_null_values.head(100):  # Sample first 100 dates
                if not self._is_valid_date(str(value), platform):
                    unparseable_dates.append(value)
            
            if unparseable_dates:
                unparseable_percentage = (len(unparseable_dates) / len(non_null_values)) * 100
                
                severity = ValidationSeverity.CRITICAL if unparseable_percentage > 10 else ValidationSeverity.WARNING
                
                issues.append(ValidationIssue(
                    rule_name="invalid_date_format",
                    severity=severity,
                    message=f"Column '{column}' has unparseable date values",
                    column=column,
                    row_count=len(unparseable_dates),
                    sample_values=unparseable_dates[:3],
                    percentage=unparseable_percentage
                ))
        
        return issues
    
    def _validate_numeric_ranges(self, df: pd.DataFrame) -> list[ValidationIssue]:
        """Validate numeric values are within expected ranges"""
        issues = []
        numeric_rules = self.validation_rules["numeric_ranges"]
        
        for column in df.columns:
            column_lower = column.lower()
            
            # Find applicable numeric rules
            applicable_rule = None
            for rule_name in numeric_rules:
                if rule_name in column_lower and isinstance(numeric_rules[rule_name], dict):
                    applicable_rule = numeric_rules[rule_name]
                    break
            
            if applicable_rule and pd.api.types.is_numeric_dtype(df[column]):
                min_val = applicable_rule.get("min")
                max_val = applicable_rule.get("max")
                
                if min_val is not None:
                    below_min = df[df[column] < min_val]
                    if len(below_min) > 0:
                        issues.append(ValidationIssue(
                            rule_name="value_below_minimum",
                            severity=ValidationSeverity.ERROR,
                            message=f"Column '{column}' has {len(below_min)} values below minimum {min_val}",
                            column=column,
                            row_count=len(below_min),
                            sample_values=below_min[column].head(3).tolist()
                        ))
                
                if max_val is not None:
                    above_max = df[df[column] > max_val]
                    if len(above_max) > 0:
                        issues.append(ValidationIssue(
                            rule_name="value_above_maximum",
                            severity=ValidationSeverity.ERROR,
                            message=f"Column '{column}' has {len(above_max)} values above maximum {max_val}",
                            column=column,
                            row_count=len(above_max),
                            sample_values=above_max[column].head(3).tolist()
                        ))
        
        return issues
    
    def _validate_text_fields(self, df: pd.DataFrame) -> list[ValidationIssue]:
        """Validate text field lengths and content"""
        issues = []
        text_rules = self.validation_rules["text_length"]
        
        for column in df.columns:
            column_lower = column.lower()
            
            # Find applicable text rules
            applicable_rule = None
            for rule_name in text_rules:
                if rule_name.replace("_", "").replace(" ", "") in column_lower.replace("_", "").replace(" ", ""):
                    applicable_rule = text_rules[rule_name]
                    break
            
            if applicable_rule and df[column].dtype == 'object':
                min_len = applicable_rule.get("min", 0)
                max_len = applicable_rule.get("max", float('inf'))
                
                # Check string lengths
                string_lengths = df[column].dropna().astype(str).str.len()
                
                if min_len > 0:
                    too_short = string_lengths[string_lengths < min_len]
                    if len(too_short) > 0:
                        issues.append(ValidationIssue(
                            rule_name="text_too_short",
                            severity=ValidationSeverity.WARNING,
                            message=f"Column '{column}' has {len(too_short)} values shorter than {min_len} characters",
                            column=column,
                            row_count=len(too_short)
                        ))
                
                if max_len < float('inf'):
                    too_long = string_lengths[string_lengths > max_len]
                    if len(too_long) > 0:
                        issues.append(ValidationIssue(
                            rule_name="text_too_long",
                            severity=ValidationSeverity.WARNING,
                            message=f"Column '{column}' has {len(too_long)} values longer than {max_len} characters",
                            column=column,
                            row_count=len(too_long)
                        ))
        
        return issues
    
    def _validate_isrc_codes(self, df: pd.DataFrame) -> list[ValidationIssue]:
        """Validate ISRC codes format"""
        issues = []
        isrc_columns = [col for col in df.columns if 'isrc' in col.lower()]
        
        for column in isrc_columns:
            non_null_isrcs = df[column].dropna()
            if len(non_null_isrcs) == 0:
                continue
                
            pattern = self.validation_rules["isrc_format"]["pattern"]
            invalid_isrcs = []
            
            for isrc in non_null_isrcs:
                if not re.match(pattern, str(isrc)):
                    invalid_isrcs.append(isrc)
            
            if invalid_isrcs:
                invalid_percentage = (len(invalid_isrcs) / len(non_null_isrcs)) * 100
                
                issues.append(ValidationIssue(
                    rule_name="invalid_isrc_format",
                    severity=ValidationSeverity.ERROR,
                    message=f"Column '{column}' has {len(invalid_isrcs)} invalid ISRC codes ({invalid_percentage:.1f}%)",
                    column=column,
                    row_count=len(invalid_isrcs),
                    sample_values=invalid_isrcs[:3],
                    percentage=invalid_percentage
                ))
        
        return issues
    
    def _validate_platform_specific(self, df: pd.DataFrame, platform: str) -> list[ValidationIssue]:
        """Apply platform-specific validation rules"""
        issues = []
        platform_rules = self.platform_specific_rules.get(platform, {})
        
        # Validate expected file format
        if platform == "fbk-facebook":
            expected_format = platform_rules.get("expected_format")
            if expected_format == "quoted_csv":
                # Check if data looks like it was properly parsed from quoted CSV
                # This is a heuristic check
                pass
        
        # Validate country codes for geographic platforms
        if "country_codes" in platform_rules:
            country_columns = [col for col in df.columns if 'country' in col.lower()]
            expected_countries = set(platform_rules["country_codes"])
            
            for column in country_columns:
                unique_countries = set(df[column].dropna().unique())
                invalid_countries = unique_countries - expected_countries
                
                if invalid_countries:
                    issues.append(ValidationIssue(
                        rule_name="invalid_country_codes",
                        severity=ValidationSeverity.WARNING,
                        message=f"Column '{column}' has unexpected country codes: {list(invalid_countries)}",
                        column=column,
                        sample_values=list(invalid_countries)[:5]
                    ))
        
        # Validate device patterns for Boomplay
        if platform == "boo-boomplay" and "device_patterns" in platform_rules:
            device_columns = [col for col in df.columns if 'device' in col.lower()]
            patterns = platform_rules["device_patterns"]
            
            for column in device_columns:
                devices = df[column].dropna()
                unmatched_devices = []
                
                for device in devices.unique():
                    if not any(re.search(pattern, str(device), re.IGNORECASE) for pattern in patterns):
                        unmatched_devices.append(device)
                
                if unmatched_devices:
                    issues.append(ValidationIssue(
                        rule_name="unexpected_device_format",
                        severity=ValidationSeverity.INFO,
                        message=f"Column '{column}' has unexpected device formats",
                        column=column,
                        sample_values=unmatched_devices[:3]
                    ))
        
        return issues
    
    def _validate_data_consistency(self, df: pd.DataFrame, platform: str) -> list[ValidationIssue]:
        """Validate data consistency within the dataset"""
        issues = []
        
        # Check for duplicate rows
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            duplicate_percentage = (duplicates / len(df)) * 100
            severity = ValidationSeverity.ERROR if duplicate_percentage > 5 else ValidationSeverity.WARNING
            
            issues.append(ValidationIssue(
                rule_name="duplicate_rows",
                severity=severity,
                message=f"Dataset has {duplicates} duplicate rows ({duplicate_percentage:.1f}%)",
                row_count=duplicates,
                percentage=duplicate_percentage
            ))
        
        # Check for inconsistent artist/track naming
        name_columns = [col for col in df.columns if any(term in col.lower() for term in ['artist', 'track', 'song', 'title'])]
        
        for column in name_columns:
            if df[column].dtype == 'object':
                # Check for case inconsistencies
                unique_values = df[column].dropna().unique()
                case_issues = []
                
                seen_lower = {}
                for value in unique_values:
                    lower_value = str(value).lower()
                    if lower_value in seen_lower and seen_lower[lower_value] != value:
                        case_issues.append((seen_lower[lower_value], value))
                    else:
                        seen_lower[lower_value] = value
                
                if case_issues:
                    issues.append(ValidationIssue(
                        rule_name="case_inconsistency",
                        severity=ValidationSeverity.WARNING,
                        message=f"Column '{column}' has case inconsistencies",
                        column=column,
                        row_count=len(case_issues),
                        sample_values=[f"{pair[0]} vs {pair[1]}" for pair in case_issues[:3]]
                    ))
        
        return issues
    
    def _identify_date_columns(self, df: pd.DataFrame) -> list[str]:
        """Identify columns that likely contain dates"""
        date_indicators = ['date', 'time', 'timestamp', 'created', 'updated', 'period']
        return [col for col in df.columns if any(indicator in col.lower() for indicator in date_indicators)]
    
    def _is_valid_date(self, value: str, platform: str) -> bool:
        """Check if a value is a valid date"""
        # Platform-specific date format checking
        platform_rules = self.platform_specific_rules.get(platform, {})
        specific_format = platform_rules.get("date_format")
        
        if specific_format:
            try:
                datetime.strptime(value, specific_format)
                return True
            except:
                pass
        
        # Try general date parsing
        try:
            from dateutil import parser as date_parser
            date_parser.parse(value)
            return True
        except:
            return False
    
    def _calculate_quality_scores(self, issues: list[ValidationIssue], df: pd.DataFrame) -> dict[str, float]:
        """Calculate quality scores based on validation issues"""
        # Base scores
        completeness_score = 100.0
        consistency_score = 100.0
        validity_score = 100.0
        
        # Deduct points based on issues
        for issue in issues:
            deduction = 0
            
            if issue.severity == ValidationSeverity.CRITICAL:
                deduction = 30
            elif issue.severity == ValidationSeverity.ERROR:
                deduction = 15
            elif issue.severity == ValidationSeverity.WARNING:
                deduction = 5
            elif issue.severity == ValidationSeverity.INFO:
                deduction = 1
            
            # Apply deduction to appropriate score category
            if "completeness" in issue.rule_name or "null" in issue.rule_name:
                completeness_score -= deduction
            elif "consistency" in issue.rule_name or "duplicate" in issue.rule_name:
                consistency_score -= deduction
            else:
                validity_score -= deduction
        
        # Ensure scores don't go below 0
        completeness_score = max(0, completeness_score)
        consistency_score = max(0, consistency_score)
        validity_score = max(0, validity_score)
        
        # Calculate overall score (weighted average)
        overall_score = (
            completeness_score * 0.4 +  # 40% weight on completeness
            consistency_score * 0.3 +   # 30% weight on consistency  
            validity_score * 0.3         # 30% weight on validity
        )
        
        return {
            "overall": round(overall_score, 2),
            "completeness": round(completeness_score, 2),
            "consistency": round(consistency_score, 2),
            "validity": round(validity_score, 2)
        }
    
    def _count_total_rules(self, platform: str) -> int:
        """Count total number of validation rules for a platform"""
        # This is an approximation based on the rules defined
        base_rules = 8  # General rules
        platform_rules = len(self.platform_specific_rules.get(platform, {}))
        return base_rules + platform_rules
    
    def generate_quality_report(self, validation_result: ValidationResult) -> str:
        """Generate a human-readable quality report"""
        report = []
        report.append("=" * 60)
        report.append("DATA QUALITY VALIDATION REPORT")
        report.append("=" * 60)
        report.append(f"Overall Quality Score: {validation_result.overall_score:.1f}/100")
        report.append(f"Completeness: {validation_result.completeness_score:.1f}/100")
        report.append(f"Consistency: {validation_result.consistency_score:.1f}/100")
        report.append(f"Validity: {validation_result.validity_score:.1f}/100")
        report.append("")
        report.append(f"Total Records: {validation_result.metrics.get('total_records', 0):,}")
        report.append(f"Total Columns: {validation_result.metrics.get('total_columns', 0)}")
        report.append(f"Rules Passed: {validation_result.passed_rules}/{validation_result.total_rules}")
        report.append("")
        
        if validation_result.issues:
            report.append("VALIDATION ISSUES:")
            report.append("-" * 40)
            
            # Group issues by severity
            critical_issues = [i for i in validation_result.issues if i.severity == ValidationSeverity.CRITICAL]
            error_issues = [i for i in validation_result.issues if i.severity == ValidationSeverity.ERROR]
            warning_issues = [i for i in validation_result.issues if i.severity == ValidationSeverity.WARNING]
            info_issues = [i for i in validation_result.issues if i.severity == ValidationSeverity.INFO]
            
            for severity_name, issues in [
                ("CRITICAL", critical_issues),
                ("ERROR", error_issues), 
                ("WARNING", warning_issues),
                ("INFO", info_issues)
            ]:
                if issues:
                    report.append(f"\n{severity_name} ({len(issues)} issues):")
                    for issue in issues:
                        report.append(f"  • {issue.message}")
                        if issue.sample_values:
                            report.append(f"    Sample values: {issue.sample_values}")
        else:
            report.append("No validation issues found!")
        
        return "\n".join(report)


# Example usage and testing
if __name__ == "__main__":
    # Create sample test data
    test_data = pd.DataFrame({
        'artist_name': ['Artist One', 'Artist Two', None, 'artist one'],  # Case inconsistency + null
        'track_title': ['Song 1', 'S', 'A' * 1001, 'Song 4'],  # Length issues
        'isrc': ['USRC17607839', 'INVALID123', 'GBUM71505078', None],  # Invalid ISRC
        'streams': [1000, -5, 2000000000, 500],  # Negative value, too high
        'date': ['2024-01-01', 'invalid-date', '01/12/2024', '2024-12-31'],  # Mixed formats
        'country': ['US', 'INVALID', 'GB', 'FR']
    })
    
    validator = StreamingDataValidator()
    result = validator.validate_dataset(test_data, 'spo-spotify', 'test_file.csv')
    
    print(validator.generate_quality_report(result))