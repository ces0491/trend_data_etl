#!/usr/bin/env python3
"""
Windows-compatible testing script for real files - COMPLETE VERSION
Run this instead of using tail and other Unix commands that don't work on Windows
"""

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

def setup_environment():
    """Setup environment and check prerequisites"""
    print("🔧 Setting up environment...")
    
    # Check if .env file exists, create if not
    if not Path('.env').exists():
        print("📝 Creating .env file...")
        with open('.env', 'w') as f:
            f.write("DATABASE_URL=sqlite:///./temp/streaming_analytics.db\n")
            f.write("API_HOST=0.0.0.0\n")
            f.write("API_PORT=8000\n")
            f.write("DEBUG=true\n")
            f.write("BATCH_SIZE=1000\n")
            f.write("QUALITY_THRESHOLD=90\n")
            f.write("MAX_FILE_SIZE_MB=500\n")
        print("✅ Created .env file")
        
        # Reload environment
        load_dotenv()
    
    # Ensure temp directory exists
    Path("temp").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("reports").mkdir(exist_ok=True)
    
    # Check DATABASE_URL
    if not os.getenv('DATABASE_URL'):
        print("❌ DATABASE_URL still not set after .env creation")
        return False
    
    print(f"✅ DATABASE_URL: {os.getenv('DATABASE_URL')}")
    return True

def test_imports():
    """Test that all required modules can be imported"""
    print("📦 Testing imports...")
    
    try:
        from database.models import DatabaseManager, QualityScore, StreamingRecord
        print("✅ Database models imported")
        
        from etl.data_processor import StreamingDataProcessor
        print("✅ Data processor imported")
        
        from etl.parsers.enhanced_parser import EnhancedETLParser
        print("✅ Enhanced parser imported")
        
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_single_file(file_path: str):
    """Test processing a single file with detailed output and error handling."""
    print(f"\n🔍 Testing file: {file_path}")
    print("-" * 60)
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    # Show file info
    file_size = os.path.getsize(file_path) / 1024  # KB
    print(f"📄 File size: {file_size:.1f} KB")
    
    # Test encoding detection first
    try:
        from etl.parsers.enhanced_parser import EnhancedETLParser
        parser = EnhancedETLParser()
        
        print("🔍 Testing encoding detection...")
        path_obj = Path(file_path)
        platform = parser.detect_platform(path_obj)
        print(f"   Detected platform: {platform}")
        
        encoding = parser.detect_encoding(path_obj, platform)
        print(f"   Detected encoding: {encoding}")
        
        # Test if file can be read with detected encoding
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                sample = f.read(1000)
            print(f"   ✅ File readable with {encoding}")
            print(f"   Sample content: {repr(sample[:100])}...")
        except Exception as e:
            print(f"   ❌ Cannot read file with {encoding}: {e}")
            
            # Try UTF-8 with error replacement
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    sample = f.read(1000)
                print(f"   ✅ File readable with UTF-8 + error replacement")
                replaced_chars = sample.count('\ufffd')
                if replaced_chars > 0:
                    print(f"   ⚠️  {replaced_chars} characters were replaced")
            except Exception as e2:
                print(f"   ❌ Even UTF-8 with replacement failed: {e2}")
                return False
        
    except Exception as e:
        print(f"❌ Parser initialization failed: {e}")
        return False
    
    # Initialize database and processor
    try:
        from database.models import DatabaseManager
        from etl.data_processor import StreamingDataProcessor
        
        db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
        processor = StreamingDataProcessor(db_manager)
        print("✅ Database and processor initialized")
    except Exception as e:
        print(f"❌ Database/processor initialization failed: {e}")
        print("💡 Make sure SQLite database is set up correctly")
        return False
    
    # Process the file
    try:
        print("⚙️ Processing file...")
        start_time = time.time()
        
        result = processor.process_file(file_path)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        if result.success:
            print(f"✅ Processing successful!")
            print(f"   Records processed: {result.records_processed:,}")
            print(f"   Quality score: {result.quality_score:.1f}/100")
            print(f"   Processing time: {processing_time:.2f}s")
            
            # Quality assessment
            if result.quality_score >= 95:
                print("   🟢 Excellent quality!")
            elif result.quality_score >= 90:
                print("   🟡 Good quality")
            elif result.quality_score >= 80:
                print("   🟠 Acceptable quality")
            else:
                print("   🔴 Low quality - needs investigation")
            
            # Show quality details if available
            try:
                from database.models import QualityScore
                with db_manager.get_session() as session:
                    latest_quality = session.query(QualityScore).order_by(
                        QualityScore.measured_at.desc()
                    ).first()
                    
                    if latest_quality and latest_quality.quality_details:
                        issues = latest_quality.quality_details.get('issues', [])
                        if issues:
                            print(f"   📋 Quality issues found:")
                            for i, issue in enumerate(issues[:5]):  # Show top 5
                                severity = issue.get('severity', 'unknown')
                                message = issue.get('message', 'Unknown issue')
                                print(f"     {i+1}. [{severity}] {message}")
            except Exception as e:
                print(f"   ⚠️  Could not retrieve quality details: {e}")
            
            return True
        else:
            print(f"❌ Processing failed: {result.error_message}")
            
            # Additional troubleshooting info
            print("\n🔧 Troubleshooting suggestions:")
            if "encoding" in result.error_message.lower():
                print("   - File encoding issue detected")
                print("   - Try opening file in text editor to check for special characters")
                print("   - Consider saving file as UTF-8 if possible")
            elif "delimiter" in result.error_message.lower():
                print("   - File delimiter issue detected")
                print("   - Check if file uses tabs, commas, or other separators")
            else:
                print("   - Check file format and structure")
                print("   - Verify file is not corrupted")
                print("   - Check logs for more detailed error information")
            
            return False
            
    except Exception as e:
        print(f"❌ Processing error: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Try to give specific advice based on error type
        if "UnicodeDecodeError" in str(type(e)):
            print("   💡 This is a character encoding issue")
            print("   💡 The file contains characters that can't be decoded")
        elif "pandas" in str(e).lower():
            print("   💡 This is a data parsing issue")
            print("   💡 The file structure may not match expected format")
        
        return False

def check_database_status():
    """Check current database status."""
    print("\n📊 Database Status Check")
    print("-" * 30)
    
    try:
        from database.models import DatabaseManager, StreamingRecord, QualityScore
        
        db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
        
        with db_manager.get_session() as session:
            # Count total records
            total_records = session.query(StreamingRecord).count()
            print(f"Total streaming records: {total_records:,}")
            
            # Count quality scores
            quality_count = session.query(QualityScore).count()
            print(f"Files with quality scores: {quality_count}")
            
            if quality_count > 0:
                # Get recent quality scores
                recent_scores = session.query(QualityScore).order_by(
                    QualityScore.measured_at.desc()
                ).limit(10).all()
                
                print(f"\n📈 Recent Quality Scores (last {len(recent_scores)}):")
                for i, score in enumerate(recent_scores, 1):
                    file_name = Path(score.file_path).name
                    platform_code = score.platform.code if score.platform else "unknown"
                    quality_icon = "🟢" if float(score.overall_score) >= 90 else "🟡" if float(score.overall_score) >= 80 else "🔴"
                    print(f"  {i:2d}. {quality_icon} {file_name[:40]:40} | {score.overall_score}/100 | {platform_code}")
                
                # Calculate average
                if recent_scores:
                    avg_score = sum(float(s.overall_score) for s in recent_scores) / len(recent_scores)
                    print(f"\nAverage quality: {avg_score:.1f}/100")
                    
                    # Show distribution
                    excellent = sum(1 for s in recent_scores if float(s.overall_score) >= 95)
                    good = sum(1 for s in recent_scores if 90 <= float(s.overall_score) < 95)
                    acceptable = sum(1 for s in recent_scores if 80 <= float(s.overall_score) < 90)
                    poor = sum(1 for s in recent_scores if float(s.overall_score) < 80)
                    
                    print(f"Quality distribution: 🟢{excellent} excellent, 🔵{good} good, 🟡{acceptable} acceptable, 🔴{poor} poor")
        
        return True
    except Exception as e:
        print(f"❌ Database check failed: {e}")
        print("💡 Make sure database is initialized with: python scripts/setup_sqlite.py")
        return False

def find_real_files():
    """Find real data files in the expected directories."""
    print("\n🔍 Looking for Real Data Files...")
    print("-" * 40)
    
    real_data_path = Path("data/real")
    
    if not real_data_path.exists():
        print("⚠️  data/real directory not found")
        print("📁 Create it and organize your files like this:")
        print("   data/real/202501/spo-spotify/your_spotify_files.csv")
        print("   data/real/202501/apl-apple/your_apple_files.txt")
        print("   data/real/202501/fbk-facebook/your_facebook_files.csv")
        return []
    
    # Find all data files
    extensions = ["*.csv", "*.tsv", "*.txt", "*.xlsx", "*.xls"]
    data_files = []
    
    for ext in extensions:
        data_files.extend(list(real_data_path.rglob(ext)))
    
    if not data_files:
        print("⚠️  No data files found in data/real/")
        print("📁 Supported file types: CSV, TSV, TXT, XLSX, XLS")
        return []
    
    # Group by platform
    platform_files = {}
    for file_path in data_files:
        path_str = str(file_path).lower()
        
        platform = "unknown"
        if "spotify" in path_str or "spo-" in path_str:
            platform = "spotify"
        elif "apple" in path_str or "apl-" in path_str:
            platform = "apple"
        elif "facebook" in path_str or "fbk-" in path_str:
            platform = "facebook"
        elif "soundcloud" in path_str or "scu-" in path_str:
            platform = "soundcloud"
        # Add other platforms as needed
        
        if platform not in platform_files:
            platform_files[platform] = []
        platform_files[platform].append(file_path)
    
    print(f"📄 Found {len(data_files)} data files across {len(platform_files)} platforms:")
    for platform, files in platform_files.items():
        print(f"   {platform}: {len(files)} files")
        # Show first few files as examples
        for file in files[:3]:
            size_kb = file.stat().st_size / 1024
            print(f"     - {file.name} ({size_kb:.1f} KB)")
        if len(files) > 3:
            print(f"     ... and {len(files) - 3} more")
    
    return data_files

def watch_logs():
    """Windows-compatible log monitoring."""
    log_file = Path("logs/processing.log")
    
    print("\n📋 Recent Processing Logs")
    print("-" * 30)
    
    if not log_file.exists():
        print("⚠️  No processing log file found yet")
        print("   Logs will appear here after processing files")
        return
    
    try:
        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
            recent_lines = lines[-30:] if len(lines) > 30 else lines
            
            if not recent_lines:
                print("   No log entries found")
                return
            
            for line in recent_lines:
                line = line.strip()
                if line:
                    # Add color coding based on log level
                    if 'ERROR' in line:
                        print(f"🔴 {line}")
                    elif 'WARNING' in line:
                        print(f"🟡 {line}")
                    elif 'INFO' in line and ('SUCCESS' in line or 'complete' in line):
                        print(f"🟢 {line}")
                    elif 'INFO' in line:
                        print(f"🔵 {line}")
                    else:
                        print(f"   {line}")
    except Exception as e:
        print(f"❌ Error reading log file: {e}")

def main():
    """Main testing function."""
    print("🧪 Real File Testing Script for Windows PowerShell")
    print("=" * 60)
    
    # Step 1: Setup environment
    if not setup_environment():
        print("❌ Environment setup failed")
        return False
    
    # Step 2: Test imports
    if not test_imports():
        print("❌ Import test failed")
        return False
    
    # Step 3: Check database status
    print("\n1️⃣ Checking Database Status...")
    db_ok = check_database_status()
    
    if not db_ok:
        print("\n🔧 Setting up database...")
        try:
            # Try to initialize database
            from database.models import initialize_database
            db = initialize_database(os.getenv('DATABASE_URL'))
            print("✅ Database initialized")
        except Exception as e:
            print(f"❌ Database setup failed: {e}")
            print("💡 Try running: python scripts/setup_sqlite.py")
            return False
    
    # Step 4: Find files
    print("\n2️⃣ Finding Real Data Files...")
    data_files = find_real_files()
    
    if not data_files:
        return False
    
    # Step 5: Test files
    print("\n3️⃣ Testing File Processing...")
    success_count = 0
    
    # Test first 5 files to avoid overwhelming output
    test_files = data_files[:5]
    
    for i, file_path in enumerate(test_files, 1):
        print(f"\n--- Testing File {i}/{len(test_files)} ---")
        success = test_single_file(str(file_path))
        if success:
            success_count += 1
        
        # Small delay between files
        if i < len(test_files):
            time.sleep(1)
    
    # Step 6: Show final results
    print("\n4️⃣ Final Results...")
    watch_logs()
    
    print("\n5️⃣ Summary...")
    check_database_status()
    
    print(f"\n📊 TESTING COMPLETE")
    print(f"Files tested: {len(test_files)}")
    print(f"Successfully processed: {success_count}")
    print(f"Success rate: {(success_count/len(test_files)*100):.1f}%")
    
    if success_count == len(test_files):
        print("🎉 All files processed successfully!")
        print("✅ Your system is ready for production use")
    elif success_count > 0:
        print("✅ Some files processed successfully")
        print("🔧 Review failed files and fix issues")
    else:
        print("❌ No files processed successfully")
        print("🔧 Check encoding and format issues")
    
    print("\n💡 Next Steps:")
    print("   - Fix any encoding issues identified")
    print("   - Process all files: python src/etl/data_processor.py data/real/")
    print("   - Start API server: python -m uvicorn src.api.main:app --reload")
    print("   - Check API docs: http://localhost:8000/docs")
    
    return success_count > 0

if __name__ == "__main__":
    success = main()
    input("\nPress Enter to exit...")  # Keep window open on Windows
    sys.exit(0 if success else 1)