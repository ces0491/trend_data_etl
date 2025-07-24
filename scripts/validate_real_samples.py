#!/usr/bin/env python3
"""
Real Sample Data Validation Script
Tests the ETL pipeline with actual sample files and validates processing results
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from datetime import datetime
import traceback

# Setup Python path
current_dir = Path(__file__).parent
project_root = current_dir.parent
src_dir = project_root / "src"

if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Import our modules
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not available, using environment variables directly")

try:
    from database.models import DatabaseManager, initialize_database
    from etl.data_processor import StreamingDataProcessor
    from etl.parsers.enhanced_parser import EnhancedETLParser
    from etl.validators.data_validator import StreamingDataValidator
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure you're running from the project root directory")
    sys.exit(1)


def create_test_sample_files():
    """Create comprehensive test sample files if they don't exist"""
    print("🔧 Creating test sample files...")
    
    sample_dir = project_root / "data" / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Apple Music sample (quote-wrapped TSV format)
    apple_data = '''\"artist_name\ttitle\treport_date\tquantity\tcustomer_currency\tvendor_identifier\"
\"Taylor Swift\tShake It Off\t12/01/24\t1250\tUSD\tPADPIDA2021030304M_191061307952_USCGH1743953\"
\"Ed Sheeran\tShape of You\t12/01/24\t890\tGBP\tPADPIDA2021030304M_191061307952_GBUM71505078\"
\"Billie Eilish\tbad guy\t2024-12-01\t2100\tUSD\tPADPIDA2021030304M_191061307952_USIR12000001\"'''
    
    (sample_dir / "apl-apple_test_20241201.txt").write_text(apple_data, encoding='utf-8')
    
    # Facebook sample (quoted CSV format)
    facebook_data = '''\"isrc\",\"date\",\"product_type\",\"plays\"
\"USRC17607839\",\"2024-12-01\",\"FB_REELS\",\"1200\"
\"GBUM71505078\",\"2024-12-01\",\"IG_MUSIC_STICKER\",\"890\"
\"USIR12000001\",\"2024-12-01\",\"FB_FROM_IG_CROSSPOST\",\"1500\"'''
    
    (sample_dir / "fbk-facebook_test_20241201.csv").write_text(facebook_data, encoding='utf-8')
    
    # Spotify sample (standard TSV)
    spotify_data = '''artist_name\ttrack_name\tstreams\tdate\tcountry
Taylor Swift\tAnti-Hero\t45000\t2024-12-01\tUS
Bad Bunny\tTití Me Preguntó\t38000\t2024-12-01\tUS
Harry Styles\tAs It Was\t29000\t2024-12-01\tGB'''
    
    (sample_dir / "spo-spotify_test_20241201.tsv").write_text(spotify_data, encoding='utf-8')
    
    # Boomplay sample (European DD/MM/YYYY dates)
    boomplay_data = '''song_id\tartist_name\ttitle\tdate\tcountry\tstreams
12345\tBurna Boy\tLast Last\t01/12/2024\tNG\t5000
67890\tWizkid\tEssence\t01/12/2024\tZA\t4500
11111\tDavido\tFEM\t01/12/2024\tKE\t3200'''
    
    (sample_dir / "boo-boomplay_test_20241201.tsv").write_text(boomplay_data, encoding='utf-8')
    
    # AWA sample (Compact YYYYMMDD dates)
    awa_data = '''track_id\tartist_name\ttitle\tdate\tprefecture\tplays
JP001\tYoasobi\tIdol\t20241201\t13.0\t3500
JP002\tOfficial HIGE DANdism\tSubtitle\t20241201\t27.0\t2800
JP003\tKing Gnu\tHakujitsu\t20241201\t23.0\t4200'''
    
    (sample_dir / "awa-awa_test_20241201.tsv").write_text(awa_data, encoding='utf-8')
    
    # SoundCloud sample (timezone-aware timestamps)
    soundcloud_data = '''track_id\tuser_id\tartist_name\ttrack_title\ttimestamp\tplays
SC001\tuser123\tIndependent Artist 1\tMidnight Vibes\t2024-12-01 17:18:10.040+00\t850
SC002\tuser456\tIndie Band X\tNeon Dreams\t2024-12-01 18:22:35.120+00\t1200
SC003\tuser789\tBedroom Producer\tLo-Fi Study Beat\t2024-12-01 19:45:12.580+00\t2100'''
    
    (sample_dir / "scu-soundcloud_test_20241201.tsv").write_text(soundcloud_data, encoding='utf-8')
    
    created_files = [
        "apl-apple_test_20241201.txt",
        "fbk-facebook_test_20241201.csv", 
        "spo-spotify_test_20241201.tsv",
        "boo-boomplay_test_20241201.tsv",
        "awa-awa_test_20241201.tsv",
        "scu-soundcloud_test_20241201.tsv"
    ]
    
    print(f"✅ Created {len(created_files)} test sample files")
    return sample_dir, created_files


def validate_individual_file(file_path: Path) -> dict:
    """Validate a single sample file through the complete pipeline"""
    print(f"\n🔍 Validating: {file_path.name}")
    
    # Step 1: Platform detection
    parser = EnhancedETLParser()
    platform = parser.detect_platform(file_path)
    
    if not platform:
        return {
            "success": False,
            "error": "Platform detection failed",
            "file": file_path.name
        }
    
    print(f"   Platform detected: {platform}")
    
    # Step 2: File parsing
    start_time = time.time()
    parse_result = parser.parse_file(file_path)
    parse_time = time.time() - start_time
    
    if not parse_result.success:
        return {
            "success": False,
            "error": f"Parsing failed: {parse_result.error_message}",
            "file": file_path.name,
            "platform": platform
        }
    
    print(f"   ✅ Parsed {parse_result.records_parsed} records in {parse_time:.2f}s")
    print(f"   Format: {parse_result.format_detected}")
    print(f"   Encoding: {parse_result.encoding_detected}")
    
    # Step 3: Data validation
    validator = StreamingDataValidator()
    validation_result = validator.validate_dataset(
        parse_result.data, platform, str(file_path)
    )
    
    print(f"   Quality Score: {validation_result.overall_score:.1f}/100")
    
    # Step 4: Display validation issues if any
    if validation_result.issues:
        critical_issues = [i for i in validation_result.issues if i.severity.value == "critical"]
        error_issues = [i for i in validation_result.issues if i.severity.value == "error"]
        
        if critical_issues:
            print(f"   ❌ {len(critical_issues)} critical issues found")
            for issue in critical_issues[:2]:  # Show first 2
                print(f"      • {issue.message}")
        
        if error_issues:
            print(f"   ⚠️  {len(error_issues)} error issues found")
            for issue in error_issues[:2]:  # Show first 2
                print(f"      • {issue.message}")
    else:
        print(f"   ✅ No validation issues found")
    
    return {
        "success": True,
        "file": file_path.name,
        "platform": platform,
        "records_parsed": parse_result.records_parsed,
        "quality_score": validation_result.overall_score,
        "parse_time": parse_time,
        "format_detected": parse_result.format_detected,
        "encoding_detected": parse_result.encoding_detected,
        "issues_count": len(validation_result.issues),
        "critical_issues": len([i for i in validation_result.issues if i.severity.value == "critical"]),
        "validation_result": validation_result
    }


def test_database_processing(sample_dir: Path) -> bool:
    """Test complete database processing pipeline"""
    print("\n🗄️  Testing database processing...")
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not configured")
        return False
    
    try:
        # Initialize database
        db_manager = initialize_database(db_url)
        print("✅ Database initialized")
        
        # Create processor
        processor = StreamingDataProcessor(db_manager)
        
        # Process a sample file
        sample_files = list(sample_dir.glob("spo-spotify*.tsv"))
        if not sample_files:
            print("❌ No Spotify sample file found for database testing")
            return False
        
        test_file = sample_files[0]
        print(f"   Processing: {test_file.name}")
        
        result = processor.process_file(test_file)
        
        if result.success:
            print(f"   ✅ Successfully processed {result.records_processed} records")
            print(f"   Quality Score: {result.quality_score:.1f}")
            print(f"   Processing Time: {result.processing_time:.2f}s")
            return True
        else:
            print(f"   ❌ Processing failed: {result.error_message}")
            return False
            
    except Exception as e:
        print(f"❌ Database processing test failed: {e}")
        traceback.print_exc()
        return False


def generate_validation_report(results: list[dict]) -> str:
    """Generate comprehensive validation report"""
    report = []
    report.append("=" * 70)
    report.append("STREAMING DATA VALIDATION REPORT")
    report.append("=" * 70)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    successful_files = [r for r in results if r.get("success", False)]
    failed_files = [r for r in results if not r.get("success", False)]
    
    # Summary
    report.append("SUMMARY:")
    report.append(f"  Total files tested: {len(results)}")
    report.append(f"  Successful: {len(successful_files)}")
    report.append(f"  Failed: {len(failed_files)}")
    
    if successful_files:
        total_records = sum(r.get("records_parsed", 0) for r in successful_files)
        avg_quality = sum(r.get("quality_score", 0) for r in successful_files) / len(successful_files)
        avg_parse_time = sum(r.get("parse_time", 0) for r in successful_files) / len(successful_files)
        
        report.append(f"  Total records parsed: {total_records:,}")
        report.append(f"  Average quality score: {avg_quality:.1f}/100")
        report.append(f"  Average parse time: {avg_parse_time:.3f}s")
    
    report.append("")
    
    # Detailed results
    if successful_files:
        report.append("SUCCESSFUL FILES:")
        report.append("-" * 40)
        for result in successful_files:
            report.append(f"✅ {result['file']}")
            report.append(f"   Platform: {result['platform']}")
            report.append(f"   Records: {result['records_parsed']:,}")
            report.append(f"   Quality: {result['quality_score']:.1f}/100")
            report.append(f"   Format: {result['format_detected']}")
            report.append(f"   Encoding: {result['encoding_detected']}")
            if result.get("issues_count", 0) > 0:
                report.append(f"   Issues: {result['issues_count']} total, {result.get('critical_issues', 0)} critical")
            report.append("")
    
    if failed_files:
        report.append("FAILED FILES:")
        report.append("-" * 40)
        for result in failed_files:
            report.append(f"❌ {result['file']}")
            report.append(f"   Error: {result.get('error', 'Unknown error')}")
            if 'platform' in result:
                report.append(f"   Platform: {result['platform']}")
            report.append("")
    
    # Quality assessment
    if successful_files:
        report.append("QUALITY ASSESSMENT:")
        report.append("-" * 40)
        
        high_quality = [r for r in successful_files if r.get("quality_score", 0) >= 90]
        medium_quality = [r for r in successful_files if 70 <= r.get("quality_score", 0) < 90]
        low_quality = [r for r in successful_files if r.get("quality_score", 0) < 70]
        
        report.append(f"  High quality (90+): {len(high_quality)} files")
        report.append(f"  Medium quality (70-89): {len(medium_quality)} files")
        report.append(f"  Low quality (<70): {len(low_quality)} files")
        report.append("")
        
        if low_quality:
            report.append("  Files needing attention:")
            for result in low_quality:
                report.append(f"    • {result['file']}: {result['quality_score']:.1f}/100")
        
        # Platform coverage
        platforms_tested = set(r.get("platform", "") for r in successful_files)
        report.append(f"  Platforms tested: {len(platforms_tested)}")
        for platform in sorted(platforms_tested):
            platform_files = [r for r in successful_files if r.get("platform") == platform]
            avg_quality = sum(r.get("quality_score", 0) for r in platform_files) / len(platform_files)
            report.append(f"    • {platform}: {len(platform_files)} files, {avg_quality:.1f} avg quality")
    
    report.append("")
    report.append("=" * 70)
    
    return "\n".join(report)


def main():
    """Main validation function"""
    print("STREAMING ANALYTICS PLATFORM - SAMPLE DATA VALIDATION")
    print("=" * 70)
    print(f"Project Root: {project_root}")
    print()
    
    # Step 1: Create test sample files if needed
    sample_dir, created_files = create_test_sample_files()
    
    # Step 2: Find all sample files
    sample_files = []
    patterns = ["*.txt", "*.csv", "*.tsv"]
    
    for pattern in patterns:
        sample_files.extend(sample_dir.glob(pattern))
    
    if not sample_files:
        print("❌ No sample files found to validate")
        return False
    
    print(f"Found {len(sample_files)} sample files to validate")
    
    # Step 3: Validate each file individually
    results = []
    for file_path in sorted(sample_files):
        try:
            result = validate_individual_file(file_path)
            results.append(result)
        except Exception as e:
            print(f"❌ Validation crashed for {file_path.name}: {e}")
            results.append({
                "success": False,
                "file": file_path.name,
                "error": f"Validation crashed: {e}"
            })
    
    # Step 4: Test database processing
    db_test_success = test_database_processing(sample_dir)
    
    # Step 5: Generate report
    report = generate_validation_report(results)
    
    # Save report to file
    reports_dir = project_root / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = reports_dir / f"validation_report_{timestamp}.txt"
    report_file.write_text(report, encoding='utf-8')
    
    # Print report
    print(report)
    print(f"\n📋 Full report saved to: {report_file}")
    
    # Overall success assessment
    successful_count = sum(1 for r in results if r.get("success", False))
    success_rate = (successful_count / len(results)) * 100 if results else 0
    
    print(f"\nVALIDATION SUMMARY:")
    print(f"  Success Rate: {success_rate:.1f}% ({successful_count}/{len(results)} files)")
    print(f"  Database Test: {'✅ PASSED' if db_test_success else '❌ FAILED'}")
    
    # Success criteria
    target_success_rate = 95
    if success_rate >= target_success_rate and db_test_success:
        print(f"\n🎉 VALIDATION SUCCESSFUL!")
        print(f"✅ Exceeded {target_success_rate}% success rate target")
        print(f"✅ Database processing working")
        print(f"✅ Ready for production data processing")
        return True
    else:
        print(f"\n⚠️  VALIDATION NEEDS ATTENTION")
        if success_rate < target_success_rate:
            print(f"❌ Success rate {success_rate:.1f}% below {target_success_rate}% target")
        if not db_test_success:
            print(f"❌ Database processing test failed")
        print(f"🔧 Please review errors and improve data quality")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Validation script crashed: {e}")
        traceback.print_exc()
        sys.exit(1)