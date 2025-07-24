# Setup Guide - Trend Data ETL

Complete installation and configuration guide for local development and production deployment.

## 🎯 Quick Start (5 Minutes)

For experienced developers who want to get running immediately:

```bash

# 1. Clone and setup

git clone <repository-url>
cd streaming-analytics-platform
python -m pip install -r requirements.txt

# 2. Setup local database

python scripts/setup_sqlite.py

# 3. Validate installation

python scripts/validate_setup.py

# 4. Start API server

uvicorn src.api.main:app --reload --port 8000

# 5. Visit API docs

open http://localhost:8000/docs

```text

## 📋 Prerequisites

### System Requirements

- **Python 3.8+** (3.11 recommended for Windows PowerShell/VS Code)
- **Git** for version control
- **Internet connection** for package installation
- **4GB+ RAM** recommended for processing large files
- **10GB+ disk space** for data storage

### Development Environment

- **VS Code** (recommended) with Python extension
- **Windows PowerShell** or equivalent terminal
- **Chrome/Firefox** for API documentation viewing

### Optional for Production

- **PostgreSQL 12+** for production database
- **Docker** for containerized deployment
- **Render/Heroku account** for cloud deployment

## 🛠️ Detailed Installation

### Step 1: Python Environment Setup

#### Option A: System Python (Simplest)

```bash

# Verify Python version

python --version  # Should be 3.8+

# Install required packages

python -m pip install -r requirements.txt

```text

#### Option B: Virtual Environment (Recommended)

```bash

# Create virtual environment

python -m venv streaming_analytics_env

# Activate (Windows PowerShell)

.\streaming_analytics_env\Scripts\Activate.ps1

# Activate (Mac/Linux)

source streaming_analytics_env/bin/activate

# Install packages in virtual environment

python -m pip install -r requirements.txt

```text

#### Option C: Conda Environment

```bash

# Create conda environment

conda create -n streaming_analytics python=3.11

# Activate environment

conda activate streaming_analytics

# Install packages

python -m pip install -r requirements.txt

```text

### Step 2: Project Dependencies

The platform requires these key packages:

```text
fastapi>=0.104.1      # Web API framework
uvicorn>=0.24.0       # ASGI server
sqlalchemy>=2.0.23    # Database ORM
pandas>=2.1.3         # Data processing
python-dotenv>=1.0.0  # Environment management
chardet>=5.2.0        # Encoding detection
python-dateutil>=2.8.2 # Date parsing
openpyxl>=3.1.2       # Excel support
psycopg2-binary>=2.9.9 # PostgreSQL (production)
pydantic>=2.5.0       # Data validation

```text

**Installation Issues?**
```bash

# If psycopg2 fails on Windows

pip install psycopg2-binary --force-reinstall --no-cache-dir

# If pandas fails

pip install pandas --upgrade

# If all else fails, install individually

pip install fastapi uvicorn sqlalchemy pandas python-dotenv

```text

### Step 3: Environment Configuration

#### Create Environment File

```bash

# Copy template

cp .env.template .env

# Edit with your settings

# Windows: notepad .env

# Mac/Linux: nano .env

```text

#### Environment Variables Explained

```bash

# .env file contents

# Database Configuration

DATABASE_URL=sqlite:///./temp/streaming_analytics.db  # Local development

# DATABASE_URL=postgresql://user:password@host:5432/database  # Production

# API Configuration  

API_HOST=0.0.0.0      # Listen on all interfaces
API_PORT=8000         # API server port
DEBUG=true            # Enable debug mode for development

# Processing Configuration

BATCH_SIZE=1000       # Records to process at once
QUALITY_THRESHOLD=90  # Minimum acceptable quality score (0-100)
MAX_FILE_SIZE_MB=500  # Maximum file size to process

# Optional: External Service Configuration

# GOOGLE_DRIVE_CREDENTIALS_PATH=path/to/credentials.json

# SLACK_WEBHOOK_URL=https://hooks.slack.com/

# EMAIL_NOTIFICATIONS=user@example.com

```text

### Step 4: Database Setup

#### Local Development (SQLite)

```bash

# Automated setup with sample data

python scripts/setup_sqlite.py

# This script will

# ✅ Create temp/ directory

# ✅ Generate streaming_analytics.db 

# ✅ Create all tables with proper indexes

# ✅ Insert 9 platform configurations

# ✅ Create sample test data

# ✅ Generate comprehensive validation files

```text

**Manual Database Setup (if needed):**
```bash

# Create directories

mkdir -p temp data/sample logs reports

# Run database initialization

python -c "
from src.database.models import initialize_database
import os
from dotenv import load_dotenv
load_dotenv()
db = initialize_database(os.getenv('DATABASE_URL'))
print('Database initialized successfully')
"

```text

#### Production Database (PostgreSQL)

See [Production Deployment](#production-deployment) section below.

### Step 5: Validation and Testing

#### Comprehensive System Check

```bash

# Validate complete installation

python scripts/validate_setup.py

# Expected output

# ✅ Python Version: 3.11.x

# ✅ Required Packages: All installed

# ✅ Project Structure: Complete

# ✅ Custom Modules: All importable

# ✅ Environment Config: Configured

# ✅ Database Connection: Working

# ✅ Sample Data Processing: Successful

```text

#### Test with Real Sample Data

```bash

# Process and validate sample files

python scripts/validate_real_samples.py

# Expected output

# ✅ Apple Music sample: 95.2/100 quality

# ✅ Facebook sample: 92.1/100 quality  

# ✅ Spotify sample: 96.8/100 quality

# ... (all platforms tested)

# 🎉 VALIDATION SUCCESSFUL

```text

#### Quick Functional Test

```bash

# Run end-to-end demo

python scripts/quick_start_demo.py

# This will

# ✅ Create sample data files

# ✅ Process through ETL pipeline

# ✅ Store in database

# ✅ Query via API endpoints

# ✅ Generate quality reports

```text

### Step 6: Start the API Server

#### Development Server

```bash

# Start with auto-reload (recommended for development)

uvicorn src.api.main:app --reload --port 8000

# Expected output

# INFO: Will watch for changes in these directories: ['/path/to/project']

# INFO: Uvicorn running on http://0.0.0.0:8000

# INFO: Started reloader process

# INFO: Started server process

# INFO: Waiting for application startup

# 🚀 Starting Streaming Analytics Platform API

# ✅ Database initialized successfully

# INFO: Application startup complete

```text

#### Production Server

```bash

# Production-ready server (for deployment)

uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

```text

#### Verify API is Working

```bash

# Test basic endpoint

curl http://localhost:8000/health

# Expected response

# {

#   "status": "healthy",

#   "timestamp": "2024-12-01T12:00:00.000Z",

#   "database_status": "healthy", 

#   "platforms_configured": 9,

#   "api_version": "1.0.0"

# }

```text

## 🌐 Production Deployment

### Render.com Deployment (Recommended)

#### Prerequisites

- Render.com account
- GitHub repository with your code
- PostgreSQL database plan on Render

#### Step 1: Database Setup

```bash

# Create PostgreSQL database on Render

# Note the connection string: postgresql://username:password@host/database

# Initialize production database

DATABASE_URL=postgresql://username:password@host/database python scripts/init_render_db.py

# Expected output

# ✅ Connected to PostgreSQL

# ✅ TimescaleDB extension enabled

# ✅ Database schema initialized  

# ✅ Production optimizations applied

# 🚀 PRODUCTION READY

```text

#### Step 2: Application Deployment

1. **Create Render Web Service**
   - Connect GitHub repository
   - Use `render.yaml` configuration
   - Set environment variables

2. **Environment Variables on Render**
```bash

DATABASE_URL=postgresql://username:password@host/database
DEBUG=false
API_HOST=0.0.0.0
API_PORT=10000
QUALITY_THRESHOLD=90

```text

3. **Deploy**
   - Push to GitHub triggers automatic deployment
   - Monitor deployment logs
   - Test endpoints once deployed

#### Step 3: Production Validation

```bash

# Test production endpoints

curl https://your-app.onrender.com/health
curl https://your-app.onrender.com/platforms

# Run production quality check

python scripts/validate_production.py --url https://your-app.onrender.com

```text

### Alternative Deployment Options

#### Heroku

```bash

# Install Heroku CLI and login

heroku create your-app-name

# Add PostgreSQL addon

heroku addons:create heroku-postgresql:mini

# Set environment variables

heroku config:set DEBUG=false
heroku config:set QUALITY_THRESHOLD=90

# Deploy

git push heroku main

# Initialize database

heroku run python scripts/init_production_db.py

```text

#### Docker Deployment

```dockerfile

# Dockerfile (create this file)

FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

```text

```bash

# Build and run

docker build -t streaming-analytics .
docker run -p 8000:8000 -e DATABASE_URL=sqlite:///./temp/streaming_analytics.db streaming-analytics

```text

## 🔧 Troubleshooting

### Common Installation Issues

#### Import Errors

```bash

# Problem: ModuleNotFoundError

# Solution: Ensure you're running from project root

cd /path/to/streaming-analytics-platform
python scripts/validate_setup.py

# Problem: API models import failing

# Solution: Check that src/api/models.py exists and is separate from database models

ls -la src/api/models.py

```text

#### Database Issues

```bash

# Problem: Database connection failed

# Solution: Check DATABASE_URL in .env file

cat .env | grep DATABASE_URL

# Problem: SQLite permission denied

# Solution: Ensure temp/ directory is writable

mkdir -p temp
chmod 755 temp

# Problem: PostgreSQL connection issues

# Solution: Verify connection string format

# postgresql://username:password@host:port/database

```text

#### API Server Issues

```bash

# Problem: Port already in use

# Solution: Use different port or kill existing process

uvicorn src.api.main:app --reload --port 8001

# Problem: FastAPI import errors

# Solution: Reinstall FastAPI

pip install fastapi uvicorn --upgrade

```text

### Performance Issues

#### Slow File Processing

```bash

# Check file sizes

ls -lh data/sample/

# Monitor processing with verbose logging

DEBUG=true python scripts/validate_real_samples.py

# Increase batch size for large files

export BATCH_SIZE=5000

```text

#### Database Performance

```bash

# For SQLite - check database size

ls -lh temp/streaming_analytics.db

# For PostgreSQL - check connections

# Monitor from database logs or admin panel

```text

### Data Quality Issues

#### Low Quality Scores

```bash

# Generate detailed quality report

python scripts/validate_real_samples.py

# Check specific platform issues

python -c "
from src.etl.validators.data_validator import StreamingDataValidator

# Add platform-specific debugging

"

# Review validation rules for your data

# Edit src/etl/validators/data_validator.py if needed

```text

## 🤖 Claude Development Workflow

### Sharing Context with Claude

When starting a new Claude session for development:

1. **Share Project Status**

```text
I'm working on the streaming analytics platform. Current status:

- Installation: [Complete/In Progress/Issues]
- Database: [SQLite/PostgreSQL]  
- API Status: [Working/Issues]
- Last successful test: [Date/Results]

Need help with: [Specific issue or enhancement]

```text

2. **Include Relevant Files**
- Always share `trend_data_project_guidelines.txt` for full context
- Include error logs or validation results
- Share specific code files you're working on

3. **Use Validation Commands**
```bash

# Quick health check to share with Claude

python scripts/validate_setup.py > status_report.txt

# Detailed analysis for complex issues  

python scripts/validate_real_samples.py > detailed_report.txt

```text

### Development Best Practices

#### Making Changes

```bash

# Always validate before making changes

python scripts/validate_setup.py

# Make changes to specific modules

# Test immediately after changes

python scripts/quick_validation.py

# Validate complete system after changes

python scripts/validate_setup.py

```text

#### Testing New Features

```bash

# Create test branch

git checkout -b feature/new-enhancement

# Develop with frequent validation

python scripts/validate_setup.py

# Test with sample data

python scripts/validate_real_samples.py

# Commit when validation passes

git add .
git commit -m "Add new feature - all tests passing"

```text

#### Rollback if Issues

```bash

# Quick rollback to working state  

git checkout main
python scripts/validate_setup.py  # Should pass

# Then debug the issue in a new branch

git checkout -b debug/fix-issue

```text

## 📊 Next Steps After Setup

### Development Workflow

1. **Process Sample Data**: `python scripts/validate_real_samples.py`
2. **Explore API**: Visit `http://localhost:8000/docs`
3. **Add Your Data**: Place files in `data/` and process
4. **Monitor Quality**: Check quality reports in `reports/`
5. **Iterate and Improve**: Use validation scripts for feedback

### Production Readiness

1. **Deploy to Render**: Follow production deployment steps
2. **Process Real Data**: Upload production files
3. **Monitor Performance**: Watch quality trends and API metrics  
4. **Plan Phase 2**: Advanced analytics and reporting features

### Integration Ready

- **API Endpoints**: All 8 endpoints ready for integration
- **Data Access**: Comprehensive filtering and pagination
- **Quality Monitoring**: Built-in validation and reporting
- **Scalable Architecture**: Ready for high-volume data

**You're ready to start processing streaming data!** 🎵📊

For operational workflows and maintenance, see `USAGE.md`.
