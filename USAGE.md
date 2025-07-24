# Usage Guide - Trend Data ETL

Complete operational guide for processing streaming data, monitoring quality, using APIs, and maintaining the system.

## 🎯 Quick Reference

### Daily Operations

```bash

# 1. Check system health

python scripts/validate_setup.py

# 2. Process new files  

python src/etl/data_processor.py /path/to/new/files

# 3. Monitor quality

python scripts/validate_real_samples.py

# 4. Check API status

curl http://localhost:8000/health

```text

### Emergency Commands

```bash

# System not responding

pkill -f uvicorn
uvicorn src.api.main:app --reload --port 8000

# Database issues

python scripts/setup_sqlite.py  # Rebuild local DB
python scripts/init_render_db.py  # Fix production DB

# Processing stuck

python scripts/validate_setup.py  # Full system check

```text

## 📁 Data Processing Workflows

### From Raw Files to API Access

#### 1. File Collection and Organization

**Local Files (Hard Drive)**
```bash

# Organize files by platform and date

data/
├── raw/
│   ├── 202412/
│   │   ├── apl-apple/
│   │   │   ├── apple_sales_20241201.txt
│   │   │   └── apple_sales_20241202.txt
│   │   ├── spo-spotify/
│   │   │   ├── spotify_streams_20241201.tsv
│   │   │   └── spotify_streams_20241202.tsv
│   │   └── [other platforms...]
│   └── 202501/
└── processed/  # Files move here after processing

```text

**Google Drive Files**
```bash

# Option 1: Download manually

# - Download files from Google Drive folders

# - Organize in local data/ directory structure

# Option 2: Google Drive API (Future Enhancement)  

# python scripts/sync_google_drive.py --folder-id YOUR_FOLDER_ID

```text

#### 2. Single File Processing

**Process One File**
```bash

# Basic processing

python src/etl/data_processor.py data/raw/202412/spo-spotify/spotify_streams_20241201.tsv

# With options

python src/etl/data_processor.py \
  --file data/raw/202412/apl-apple/apple_sales_20241201.txt \
  --force-reprocess \
  --db-url sqlite:///./temp/streaming_analytics.db

# Expected output

# 📄 Processing: spotify_streams_20241201.tsv

# 🔍 Platform detected: spo-spotify

# ✅ Parsed 15,234 records in 2.3s

# 📊 Quality Score: 94.2/100

# 💾 Stored 15,190 records (44 failed)

# ⏱️  Total processing time: 8.7s

```text

**Check Processing Results**
```bash

# View processing logs

ls -la logs/
tail -f logs/processing.log

# Check quality report

ls -la reports/
cat reports/validation_report_20241201_143022.txt

```text

#### 3. Batch Directory Processing

**Process All Files in Directory**
```bash

# Process entire month

python src/etl/data_processor.py data/raw/202412/ --pattern="*.txt,*.tsv,*.csv"

# Process specific platform

python src/etl/data_processor.py data/raw/202412/spo-spotify/

# Monitor progress in real-time

tail -f logs/processing.log

```text

**Batch Processing Script**
```python

# scripts/batch_process.py - Create this for regular processing

#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from src.etl.data_processor import StreamingDataProcessor
from src.database.models import initialize_database

def process_monthly_data(year_month: str):
    """Process all data for a specific month"""
    data_dir = Path(f"data/raw/{year_month}")
    
    if not data_dir.exists():
        print(f"❌ Directory not found: {data_dir}")
        return
    
    db_manager = initialize_database(os.getenv('DATABASE_URL'))
    processor = StreamingDataProcessor(db_manager)
    
    results = processor.process_directory(data_dir)
    
    # Print summary
    successful = sum(1 for r in results if r.success)
    total_records = sum(r.records_processed for r in results if r.success)
    
    print(f"\n📊 BATCH PROCESSING SUMMARY")
    print(f"Files processed: {successful}/{len(results)}")
    print(f"Total records: {total_records:,}")
    
    if successful > 0:
        avg_quality = sum(r.quality_score for r in results if r.success) / successful
        print(f"Average quality: {avg_quality:.1f}/100")

if __name__ == "__main__":
    year_month = sys.argv[1] if len(sys.argv) > 1 else "202412"
    process_monthly_data(year_month)

```text

```bash

# Use the batch script

python scripts/batch_process.py 202412

```text

#### 4. Quality Monitoring During Processing

**Real-time Quality Monitoring**
```bash

# Monitor processing in one terminal

python src/etl/data_processor.py data/raw/202412/ &

# Monitor quality in another terminal

watch -n 5 'python -c "
from src.database.models import DatabaseManager, QualityScore
import os
db = DatabaseManager(os.getenv(\"DATABASE_URL\"))
with db.get_session() as session:
    recent = session.query(QualityScore).order_by(QualityScore.measured_at.desc()).limit(5).all()
    for q in recent:
        print(f\"{q.platform.code}: {q.overall_score:.1f}/100 - {q.file_path}\")
"'

```text

**Quality Alerts**
```python

# Add to your processing script

def check_quality_alerts(quality_score: float, platform: str, file_path: str):
    """Alert on quality issues"""
    if quality_score < 70:
        print(f"🚨 CRITICAL: {platform} quality {quality_score:.1f}/100 - {file_path}")
        # Add notification logic here (email, Slack, etc.)
    elif quality_score < 85:
        print(f"⚠️  WARNING: {platform} quality {quality_score:.1f}/100 - {file_path}")

```text

### 5. Data Verification and Validation

**After Processing - Verify Data**
```bash

# Check database contents

python -c "
from src.database.models import DatabaseManager, StreamingRecord, Platform
import os
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.get_session() as session:
    total = session.query(StreamingRecord).count()
    platforms = session.query(Platform.code, session.query(StreamingRecord).filter(StreamingRecord.platform_id == Platform.id).count().label('count')).all()
    print(f'Total records: {total:,}')
    for p in platforms:
        if p[1] > 0:
            print(f'  {p[0]}: {p[1]:,} records')
"

# Generate comprehensive quality report

python scripts/validate_real_samples.py > reports/daily_quality_$(date +%Y%m%d).txt

```text

## 🌐 API Usage Guide

### Getting Started with the API

#### 1. Start the API Server

```bash

# Development server with auto-reload

uvicorn src.api.main:app --reload --port 8000

# Production server  

uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# With custom configuration

API_HOST=0.0.0.0 API_PORT=8080 uvicorn src.api.main:app --reload

```text

#### 2. API Documentation

- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc  
- **OpenAPI JSON**: http://localhost:8000/openapi.json

### Core API Endpoints

#### Health and Status

```bash

# System health check

curl http://localhost:8000/health

# Detailed health information  

curl http://localhost:8000/health/database
curl http://localhost:8000/health/platforms

# Kubernetes-style probes

curl http://localhost:8000/health/ready   # Readiness probe
curl http://localhost:8000/health/live    # Liveness probe

```text

#### Platform Management

```bash

# List all platforms

curl http://localhost:8000/platforms

# Get specific platform  

curl http://localhost:8000/platforms/spo-spotify

# Platform statistics

curl http://localhost:8000/platforms/spo-spotify/statistics

# Platform health

curl http://localhost:8000/platforms/spo-spotify/health

```text

#### Artist Data Access

```bash

# Search artists

curl "http://localhost:8000/artists?search=taylor&limit=10"

# Get specific artist

curl http://localhost:8000/artists/123

# Artist's tracks

curl http://localhost:8000/artists/123/tracks

# Artist statistics

curl http://localhost:8000/artists/123/statistics

# Recent activity

curl "http://localhost:8000/artists/123/recent-activity?days=30"

```text

#### Track Information

```bash

# Search tracks

curl "http://localhost:8000/tracks?search=shake&artist_search=swift"

# Get track by ID

curl http://localhost:8000/tracks/456

# Get track by ISRC

curl http://localhost:8000/tracks/by-isrc/USRC17607839

# Track statistics and trends

curl http://localhost:8000/tracks/456/statistics
curl "http://localhost:8000/tracks/456/trends?days=90&aggregation=weekly"

```text

#### Streaming Records (Core Data)

```bash

# Basic streaming data query

curl "http://localhost:8000/streaming-records?limit=100"

# Advanced filtering

curl "http://localhost:8000/streaming-records?platform=spo-spotify&artist_name=taylor&date_from=2024-01-01&date_to=2024-12-31&min_quality_score=90"

# Paginated results

curl "http://localhost:8000/streaming-records/paginated?page=1&page_size=50"

# Time-series analytics

curl -X POST "http://localhost:8000/streaming-records/time-series" \
  -H "Content-Type: application/json" \
  -d '{
    "platforms": ["spo-spotify", "apl-apple"],
    "metric_types": ["streams"],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "aggregation": "monthly"
  }'

# Summary statistics

curl "http://localhost:8000/streaming-records/summary?date_from=2024-12-01&date_to=2024-12-31"

```text

#### Data Quality Monitoring

```bash

# Quality summary

curl "http://localhost:8000/data-quality/summary?days=30"

# Detailed quality information

curl "http://localhost:8000/data-quality/details?platform=spo-spotify&min_score=80"

# Quality trends

curl "http://localhost:8000/data-quality/trends?days=90&aggregation=weekly"

# Platform-specific quality

curl http://localhost:8000/data-quality/platform/spo-spotify

# Processing logs

curl "http://localhost:8000/data-quality/processing-logs?days=7"

# Quality issues

curl "http://localhost:8000/data-quality/issues?severity=critical"

# Comprehensive quality report

curl "http://localhost:8000/data-quality/report?days=30"

```text

### API Usage Examples

#### Python Client Example

```python

# api_client.py - Create this for programmatic access

import requests
import json
from datetime import date, timedelta

class StreamingAnalyticsClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
    
    def get_artist_stats(self, artist_name: str):
        """Get statistics for an artist"""
        # Search for artist
        response = requests.get(f"{self.base_url}/artists", params={"search": artist_name})
        artists = response.json()
        
        if not artists:
            return None
        
        artist_id = artists[0]["id"]
        
        # Get detailed statistics  
        response = requests.get(f"{self.base_url}/artists/{artist_id}/statistics")
        return response.json()
    
    def get_quality_summary(self, days=30):
        """Get data quality summary"""
        response = requests.get(f"{self.base_url}/data-quality/summary", params={"days": days})
        return response.json()
    
    def get_streaming_trends(self, platforms=None, days=90):
        """Get streaming trends over time"""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        payload = {
            "date_from": start_date.isoformat(),
            "date_to": end_date.isoformat(),
            "aggregation": "weekly"
        }
        
        if platforms:
            payload["platforms"] = platforms
        
        response = requests.post(
            f"{self.base_url}/streaming-records/time-series",
            json=payload
        )
        return response.json()

# Usage example

client = StreamingAnalyticsClient()

# Get Taylor Swift statistics

taylor_stats = client.get_artist_stats("taylor swift")
print(f"Taylor Swift total streams: {taylor_stats['overall_statistics']['total_streams']:,}")

# Check data quality

quality = client.get_quality_summary(days=7)
print(f"Average quality score: {quality['average_quality_score']}/100")

# Get Spotify trends

spotify_trends = client.get_streaming_trends(platforms=["spo-spotify"], days=30)
print(f"Spotify trend data points: {len(spotify_trends['data_points'])}")

```text

#### Excel/Dashboard Integration

```python

# excel_export.py - Export data for Excel analysis

import pandas as pd
import requests

def export_artist_data_to_excel(artist_name: str, output_file: str):
    """Export artist data to Excel for analysis"""
    base_url = "http://localhost:8000"
    
    # Get artist info
    response = requests.get(f"{base_url}/artists", params={"search": artist_name})
    artists = response.json()
    
    if not artists:
        print(f"Artist '{artist_name}' not found")
        return
    
    artist_id = artists[0]["id"]
    
    # Get streaming records
    response = requests.get(
        f"{base_url}/streaming-records",
        params={
            "artist_name": artist_name,
            "limit": 10000
        }
    )
    records = response.json()
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Create Excel file with multiple sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Streaming Records', index=False)
        
        # Platform summary
        platform_summary = df.groupby('platform_code')['metric_value'].agg(['sum', 'count', 'mean']).reset_index()
        platform_summary.to_excel(writer, sheet_name='Platform Summary', index=False)
        
        # Geographic breakdown
        geo_summary = df.groupby('geography')['metric_value'].sum().reset_index().sort_values('metric_value', ascending=False)
        geo_summary.to_excel(writer, sheet_name='Geographic Breakdown', index=False)
    
    print(f"Data exported to {output_file}")

# Usage

export_artist_data_to_excel("taylor swift", "taylor_swift_analysis.xlsx")

```text

## 📊 Quality Monitoring and Maintenance

### Daily Quality Monitoring

#### Morning Health Check

```bash

#!/bin/bash

# daily_health_check.sh - Run this every morning

echo "🌅 Daily Health Check - $(date)"
echo "================================"

# 1. System health

echo "📊 System Health:"
python scripts/validate_setup.py | grep -E "✅|❌|⚠️"

# 2. API status

echo -e "\n🌐 API Status:"
curl -s http://localhost:8000/health | jq '.status'

# 3. Recent quality scores

echo -e "\n📈 Recent Quality Scores:"
python -c "
from src.database.models import DatabaseManager, QualityScore
import os
from datetime import datetime, timedelta
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.get_session() as session:
    yesterday = datetime.utcnow() - timedelta(days=1)
    recent = session.query(QualityScore).filter(QualityScore.measured_at >= yesterday).all()
    if recent:
        avg_score = sum(float(q.overall_score) for q in recent) / len(recent)
        print(f'Files processed yesterday: {len(recent)}')
        print(f'Average quality score: {avg_score:.1f}/100')
        low_quality = [q for q in recent if float(q.overall_score) < 80]
        if low_quality:
            print(f'⚠️  {len(low_quality)} files below 80% quality')
    else:
        print('No files processed yesterday')
"

# 4. Database size

echo -e "\n💾 Database Status:"
if [ -f "temp/streaming_analytics.db" ]; then
    echo "SQLite size: $(du -h temp/streaming_analytics.db | cut -f1)"
fi

echo -e "\n✅ Health check complete"

```text

#### Weekly Quality Report

```python

# scripts/weekly_quality_report.py

#!/usr/bin/env python3
"""Generate weekly quality report"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path

sys.path.append(str(Path(__file__).parent.parent / "src"))

from database.models import DatabaseManager, QualityScore, Platform
from dotenv import load_dotenv

load_dotenv()

def generate_weekly_report():
    """Generate comprehensive weekly quality report"""
    db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
    
    # Get last 7 days of data
    week_ago = datetime.utcnow() - timedelta(days=7)
    
    with db_manager.get_session() as session:
        quality_records = session.query(QualityScore).join(Platform).filter(
            QualityScore.measured_at >= week_ago
        ).all()
        
        if not quality_records:
            print("No quality data from the last week")
            return
        
        # Calculate statistics
        total_files = len(quality_records)
        scores = [float(q.overall_score) for q in quality_records]
        avg_score = sum(scores) / len(scores)
        
        # Platform breakdown
        platform_stats = {}
        for record in quality_records:
            platform = record.platform.code
            if platform not in platform_stats:
                platform_stats[platform] = []
            platform_stats[platform].append(float(record.overall_score))
        
        # Generate report
        report_date = datetime.utcnow().strftime("%Y-%m-%d")
        report_file = Path(f"reports/weekly_quality_report_{report_date}.txt")
        
        with open(report_file, 'w') as f:
            f.write(f"WEEKLY QUALITY REPORT - {report_date}\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"📊 SUMMARY\n")
            f.write(f"Files processed: {total_files}\n")
            f.write(f"Average quality score: {avg_score:.1f}/100\n")
            f.write(f"Files above 90%: {sum(1 for s in scores if s >= 90)}\n")
            f.write(f"Files below 70%: {sum(1 for s in scores if s < 70)}\n\n")
            
            f.write(f"🎯 PLATFORM BREAKDOWN\n")
            for platform, platform_scores in platform_stats.items():
                platform_avg = sum(platform_scores) / len(platform_scores)
                f.write(f"{platform}: {len(platform_scores)} files, {platform_avg:.1f}/100 average\n")
            
            f.write(f"\n📈 RECOMMENDATIONS\n")
            if avg_score < 85:
                f.write("- Overall quality below target - review ETL pipeline\n")
            
            low_platforms = [p for p, scores in platform_stats.items() 
                           if sum(scores)/len(scores) < 80]
            if low_platforms:
                f.write(f"- Focus on platforms: {', '.join(low_platforms)}\n")
        
        print(f"Weekly report generated: {report_file}")

if __name__ == "__main__":
    generate_weekly_report()

```text

### System Maintenance

#### Database Maintenance

```bash

# SQLite maintenance

python -c "
from src.database.models import DatabaseManager
import os
db = DatabaseManager(os.getenv('DATABASE_URL'))

# VACUUM to reclaim space and optimize

with db.engine.connect() as conn:
    conn.execute('VACUUM;')
    conn.execute('ANALYZE;')
print('Database optimized')
"

# PostgreSQL maintenance (production)

python -c "
from src.database.models import DatabaseManager
import os
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.engine.connect() as conn:
    conn.execute('VACUUM ANALYZE;')
print('Production database optimized')
"

```text

#### Log Rotation and Cleanup

```bash

# scripts/cleanup_logs.sh

#!/bin/bash

# Clean up old logs and reports

echo "🧹 Cleaning up old files..."

# Keep only last 30 days of logs

find logs/ -name "*.log" -mtime +30 -delete
echo "Cleaned up old log files"

# Keep only last 90 days of reports  

find reports/ -name "*.txt" -mtime +90 -delete
echo "Cleaned up old reports"

# Clean up temporary processing files

find temp/ -name "*.tmp" -delete
echo "Cleaned up temporary files"

echo "✅ Cleanup complete"

```text

#### Performance Monitoring

```python

# scripts/performance_monitor.py

#!/usr/bin/env python3
"""Monitor system performance"""

import os
import sys
import time
import psutil
from pathlib import Path

# Add src to path

sys.path.append(str(Path(__file__).parent.parent / "src"))

def monitor_system():
    """Monitor system resources"""
    # CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Memory usage
    memory = psutil.virtual_memory()
    memory_percent = memory.percent
    
    # Disk usage
    disk = psutil.disk_usage('.')
    disk_percent = (disk.used / disk.total) * 100
    
    # Database size
    db_size = 0
    if os.path.exists('temp/streaming_analytics.db'):
        db_size = os.path.getsize('temp/streaming_analytics.db') / 1024 / 1024  # MB
    
    print(f"📊 System Performance - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"CPU: {cpu_percent:.1f}%")
    print(f"Memory: {memory_percent:.1f}%")
    print(f"Disk: {disk_percent:.1f}%")
    print(f"Database: {db_size:.1f} MB")
    
    # Alert on high usage
    if cpu_percent > 80:
        print("⚠️  HIGH CPU USAGE")
    if memory_percent > 85:
        print("⚠️  HIGH MEMORY USAGE")
    if disk_percent > 90:
        print("⚠️  HIGH DISK USAGE")

if __name__ == "__main__":
    monitor_system()

```text

## 🔧 Troubleshooting Guide

### Common Issues and Solutions

#### Processing Issues

**Problem: Low Quality Scores**
```bash

# 1. Check specific validation issues

python scripts/validate_real_samples.py

# 2. Review quality details for failing files

python -c "
from src.database.models import DatabaseManager, QualityScore
import os, json
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.get_session() as session:
    low_quality = session.query(QualityScore).filter(QualityScore.overall_score < 70).all()
    for q in low_quality:
        print(f'File: {q.file_path}')
        print(f'Score: {q.overall_score}/100')
        if q.quality_details:
            issues = q.quality_details.get('issues', [])
            for issue in issues[:3]:  # Show top 3 issues
                print(f'  - {issue.get(\"message\", \"Unknown issue\")}')
        print()
"

# 3. Fix common issues by adjusting validation rules

# Edit src/etl/validators/data_validator.py

```text

**Problem: Files Not Processing**
```bash

# 1. Check file permissions

ls -la data/raw/

# 2. Verify file format detection

python -c "
from src.etl.parsers.enhanced_parser import EnhancedETLParser
parser = EnhancedETLParser()
result = parser.detect_platform('path/to/problem/file.txt')
print(f'Detected platform: {result}')
"

# 3. Test manual parsing

python -c "
from src.etl.parsers.enhanced_parser import EnhancedETLParser
parser = EnhancedETLParser()
result = parser.parse_file('path/to/problem/file.txt')
print(f'Success: {result.success}')
if not result.success:
    print(f'Error: {result.error_message}')
"

```text

#### API Issues  

**Problem: API Not Responding**
```bash

# 1. Check if server is running

ps aux | grep uvicorn

# 2. Check port availability

netstat -tulpn | grep :8000

# 3. Restart API server

pkill -f uvicorn
uvicorn src.api.main:app --reload --port 8000

# 4. Check logs

tail -f logs/api.log

```text

**Problem: Database Connection Errors**
```bash

# 1. Verify DATABASE_URL

echo $DATABASE_URL

# 2. Test connection directly

python -c "
from src.database.models import DatabaseManager
import os
try:
    db = DatabaseManager(os.getenv('DATABASE_URL'))
    with db.get_session() as session:
        print('✅ Database connection successful')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"

# 3. For SQLite - check file permissions

ls -la temp/streaming_analytics.db

# 4. For PostgreSQL - check connection string format

# postgresql://username:password@host:port/database

```text

#### Performance Issues

**Problem: Slow Processing**
```bash

# 1. Check file sizes

find data/raw -name "*.txt" -o -name "*.csv" -o -name "*.tsv" | xargs ls -lh

# 2. Monitor processing with profiling

python -m cProfile -o profile_output.prof src/etl/data_processor.py data/raw/large_file.txt

# 3. Increase batch size for large files

export BATCH_SIZE=5000
python src/etl/data_processor.py large_file.txt

# 4. Process files in parallel

ls data/raw/202412/*.txt | xargs -P 4 -I {} python src/etl/data_processor.py {}

```text

**Problem: API Slow Response**
```bash

# 1. Check database query performance

python -c "
import time
from src.database.models import DatabaseManager, StreamingRecord
import os
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.get_session() as session:
    start = time.time()
    count = session.query(StreamingRecord).count()
    end = time.time()
    print(f'Query time: {end-start:.2f}s for {count:,} records')
"

# 2. Add database indexes if needed

# Edit src/database/models.py to add indexes

# 3. Use pagination for large results

curl "http://localhost:8000/streaming-records/paginated?page_size=50"

```text

### Emergency Recovery

#### Corrupted Database

```bash

# SQLite recovery

cp temp/streaming_analytics.db temp/streaming_analytics.db.backup
python scripts/setup_sqlite.py  # Rebuild database

# Then reprocess critical files

```text

#### Lost Configuration

```bash

# Restore configuration

cp .env.template .env

# Edit .env with your settings

# Verify setup

python scripts/validate_setup.py

```text

#### Data Loss Prevention

```bash

# Regular backups

cp temp/streaming_analytics.db backups/streaming_analytics_$(date +%Y%m%d).db

# Export critical data

python -c "
from src.database.models import DatabaseManager, StreamingRecord
import pandas as pd
import os
db = DatabaseManager(os.getenv('DATABASE_URL'))
with db.get_session() as session:
    # Export recent data
    df = pd.read_sql('SELECT * FROM streaming_records WHERE date >= date(\"now\", \"-30 days\")', db.engine)
    df.to_csv('backups/recent_data.csv', index=False)
    print(f'Exported {len(df):,} recent records')
"

```text

## 🤖 Working with Claude

### Sharing Context Effectively

When working with Claude on issues:

1. **Current Status**

```text
System Status:

- Database: [SQLite/PostgreSQL] 
- API: [Running/Down/Issues]
- Recent Processing: [Success rate, quality scores]
- Current Issue: [Specific problem description]

```text

2. **Include Relevant Data**
```bash

# Generate status report for Claude

python scripts/validate_setup.py > status_for_claude.txt
python scripts/validate_real_samples.py > quality_report_for_claude.txt

# Include error logs

tail -50 logs/processing.log > recent_errors.txt

```text

3. **Specific Requests**
- "Help debug quality scores below 80% for Apple Music files"
- "API returning 500 errors for /streaming-records endpoint"  
- "Processing stuck on large Spotify files - optimize performance"
- "Add new validation rule for ISRC format checking"

### Development Workflow with Claude

#### Feature Development

```bash

# 1. Start with current status

python scripts/validate_setup.py

# 2. Create feature branch  

git checkout -b feature/new-enhancement

# 3. Work with Claude on implementation

# Share relevant code files and requirements

# 4. Test changes

python scripts/validate_setup.py
python scripts/validate_real_samples.py

# 5. Commit when tests pass

git add .
git commit -m "Add feature - validated"

```text

#### Bug Fixing

```bash

# 1. Reproduce issue

python scripts/reproduce_issue.py

# 2. Share logs and context with Claude

tail -100 logs/processing.log > error_context.txt

# 3. Apply suggested fixes

# Test immediately after changes

# 4. Validate fix

python scripts/validate_setup.py

```text

## 📈 Scaling and Growth

### Preparing for High Volume

#### Database Optimization

```python

# Monitor database growth

def monitor_database_size():
    from src.database.models import DatabaseManager, StreamingRecord
    import os
    
    db = DatabaseManager(os.getenv('DATABASE_URL'))
    with db.get_session() as session:
        count = session.query(StreamingRecord).count()
        print(f"Total records: {count:,}")
        
        # Growth rate
        from datetime import datetime, timedelta
        last_week = datetime.utcnow() - timedelta(days=7)
        recent_count = session.query(StreamingRecord).filter(
            StreamingRecord.created_at >= last_week
        ).count()
        print(f"Records added this week: {recent_count:,}")
        print(f"Weekly growth rate: {(recent_count/count)*100:.1f}%")

```text

#### Performance Tuning

```bash

# Profile processing performance

python -m cProfile -s cumulative src/etl/data_processor.py large_file.txt

# Optimize database queries

# Add indexes for frequent query patterns

# Consider partitioning for time-series data

```text

### Migration to Production

#### PostgreSQL Migration

```bash

# 1. Export SQLite data

python scripts/export_sqlite_data.py

# 2. Setup PostgreSQL  

python scripts/init_render_db.py

# 3. Import data to PostgreSQL

python scripts/import_to_postgresql.py

# 4. Validate migration

python scripts/validate_production.py

```text

## 🎯 Success Metrics

### Daily Metrics to Track

- **Processing Success Rate**: Target >95%
- **Quality Scores**: Target >90% average
- **API Response Times**: Target <500ms
- **System Uptime**: Target >99.9%

### Weekly Reviews

- **Data Volume Growth**: Track weekly increases
- **Platform Coverage**: Ensure all 9 platforms processing
- **Quality Trends**: Monitor improvement over time
- **Error Patterns**: Identify recurring issues

### Monthly Analysis  

- **Business Insights**: What trends are emerging?
- **System Performance**: Scaling needs assessment
- **Feature Requests**: Based on usage patterns
- **Maintenance**: Database optimization needs

**You're now ready to operate a production streaming analytics platform!** 🎵📊

For setup instructions, see `SETUP.md`. For project overview, see `README.md`.
