# Trend Data ETL

Data Platform Foundation + Data Access API that aggregates streaming data from 9 major platforms, transforms messy real-world data into clean insights, and provides powerful APIs for cross-platform analysis.

## 🎯 Project Overview

### What This Platform Does

- **Ingests streaming data** from Spotify, Apple Music, Facebook/Meta, SoundCloud, Boomplay, AWA, Vevo, Peloton, and Deezer
- **Handles real-world data chaos** - quote-wrapped TSV, multiple date formats, encoding issues, platform-specific quirks
- **Validates and scores data quality** with comprehensive validation rules (targeting 90%+ quality scores)
- **Provides unified API access** to processed streaming data with advanced filtering and analytics
- **Supports both local development** (SQLite) and production deployment (PostgreSQL + TimescaleDB)

### Key Business Value

- **Unified cross-platform analytics** - Compare performance across all streaming services
- **Data quality assurance** - Automated validation with detailed quality reporting
- **API-first architecture** - Ready for integration with dashboards, reports, and applications
- **Production-ready scaling** - Optimized for time-series data with millions of records
- **Spotify Wrapped-style insights** - Foundation for generating personalized streaming reports

## 🏗️ Architecture Overview

### Two-Phase Architecture

```text
Phase 1: Data Platform Foundation (✅ COMPLETE)
├── ETL Pipeline: File parsing, validation, standardization
├── Database: Time-series optimized storage (SQLite/PostgreSQL)
├── Data Access API: RESTful endpoints for data retrieval
└── Quality Monitoring: Comprehensive validation and reporting

Phase 2: Analytics & Reporting Engine (🔄 FUTURE)
├── Advanced Analytics: Cross-platform metrics, predictions
├── Business Logic API: High-level insights, benchmarking  
├── Report Generation: Spotify Wrapped-style narratives
└── External Partner APIs: Data licensing capabilities

```text

### Current Status: Phase 1 Complete

- **✅ 93.8%+ parsing success rate** (targeting 95%+)
- **✅ 8/8 API endpoints implemented** with comprehensive functionality
- **✅ Real-world data handling** for all 9 platform formats
- **✅ Quality validation framework** with detailed scoring
- **✅ Production deployment ready** with PostgreSQL + TimescaleDB

## 🌟 Key Features

### Data Processing Excellence

- **Platform-specific parsers** handle Apple quote-wrapped TSV, Facebook quoted CSV, and 7 other unique formats
- **Intelligent date parsing** supports MM/dd/yy, DD/MM/YYYY, YYYYMMDD, ISO timestamps with timezones
- **Encoding detection** with fallback strategies for UTF-8, CP1252, Latin1
- **Quality scoring** (0-100) with completeness, consistency, and validity metrics
- **Deduplication** prevents reprocessing with file hash tracking

### Comprehensive API Coverage

- **`/platforms`** - Platform management and configuration
- **`/artists`** - Artist search, statistics, and recent activity
- **`/tracks`** - Track management, ISRC lookup, and trends
- **`/streaming-records`** - Core data access with advanced filtering
- **`/data-quality`** - Quality monitoring, trends, and reporting
- **`/health`** - System status and readiness checks

### Advanced Analytics Ready

- **Time-series optimization** for streaming data patterns
- **Cross-platform aggregation** with normalized schemas
- **Geographic and demographic** breakdowns
- **Device and subscription type** analysis
- **Quality trend monitoring** with automated alerts

## 🗄️ Database Architecture

### Development Environment

- **SQLite** (`temp/streaming_analytics.db`)
- **9 platforms** pre-configured with processing rules
- **Sample data** included for testing and validation
- **Full schema** with indexes and views for performance

### Production Environment  

- **PostgreSQL + TimescaleDB** for optimal time-series performance
- **Hypertables** for efficient time-based partitioning
- **Continuous aggregates** for real-time analytics
- **Connection pooling** and performance optimizations

### Schema Highlights

```sql

-- 7 Core Tables + 3 Analytical Views
platforms          -- 9 streaming services configuration
artists            -- Normalized artist data with deduplication  
tracks             -- Track metadata with ISRC linking
streaming_records   -- Main time-series data (hypertable)
data_processing_logs -- ETL audit trail
quality_scores     -- File-level quality metrics
file_processing_queue -- Batch processing management

-- Plus optimized indexes and views for common queries

```text

## 📊 Real-World Data Handling

### Platform-Specific Challenges Solved

- **Apple Music**: Quote-wrapped TSV with complex vendor IDs and multi-currency data
- **Facebook/Meta**: Quoted CSV with social interaction tracking
- **SoundCloud**: Timezone-aware timestamps with playlist categorization  
- **Boomplay**: European DD/MM/YYYY dates for African market data
- **AWA**: Compact YYYYMMDD dates with Japanese prefecture codes
- **Spotify**: Weekly reporting cycles with age/gender demographics
- **Others**: Each with unique quirks and format requirements

### Data Quality Standards

- **95%+ parsing success rate** across all platforms
- **90%+ data quality scores** for production files  
- **Comprehensive validation** with 40+ validation rules
- **Issue categorization** (Critical, Error, Warning, Info)
- **Automated recommendations** for quality improvement

## 🚀 Performance & Scale

### Target Performance Metrics

- **10,000+ records/minute** processing speed
- **<500ms API response times** for standard queries
- **99.9% uptime** for data access API
- **Horizontal scalability** for growing data volumes

### Quality Assurance

- **Automated daily processing** of new files
- **Real-time quality monitoring** with alerts
- **Complete data lineage** tracking for audit
- **Error recovery** and retry mechanisms

## 🔧 Development Workflow

### Claude-Assisted Development

This project is designed for collaborative development with Claude:

- **Comprehensive documentation** for context sharing
- **Modular architecture** for focused improvements
- **Extensive logging** for debugging assistance
- **Validation scripts** for quick health checks
- **Sample data** for testing and validation

### Quick Start Commands

```bash

# Complete setup and validation

python scripts/setup_sqlite.py
python scripts/validate_setup.py  
python scripts/validate_real_samples.py

# Start development server

uvicorn src.api.main:app --reload --port 8000

# Process sample data

python scripts/quick_start_demo.py

# API documentation

open http://localhost:8000/docs

```text

## 📁 Project Structure

```text
streaming-analytics-platform/
├── src/                          # Source code
│   ├── api/                      # FastAPI application
│   │   ├── routes/              # API endpoint modules
│   │   ├── models.py            # Pydantic response models
│   │   ├── dependencies.py     # FastAPI dependencies
│   │   └── main.py             # Application entry point
│   ├── database/                # Database layer
│   │   └── models.py           # SQLAlchemy models & DB manager
│   └── etl/                     # Extract, Transform, Load
│       ├── parsers/            # Platform-specific parsers
│       ├── validators/         # Data validation framework
│       └── data_processor.py  # Main processing pipeline
├── scripts/                     # Utility and setup scripts
│   ├── setup_sqlite.py        # Local database setup
│   ├── validate_setup.py      # Comprehensive validation
│   ├── validate_real_samples.py # Sample data testing
│   ├── init_render_db.py      # Production deployment
│   └── quick_start_demo.py    # End-to-end demonstration
├── data/                       # Data storage
│   └── sample/                # Test data files
├── temp/                       # SQLite database location
├── logs/                       # Application logs
├── reports/                    # Generated quality reports
├── requirements.txt            # Python dependencies
├── render.yaml                # Production deployment config
├── .env.template              # Environment variables template
└── README.md                  # This document

```text

## 🎯 Success Metrics & Status

### Current Achievement

- **✅ Phase 1 Foundation Complete** - ETL pipeline and data access API working
- **✅ 93.8% parsing success rate** - Very close to 95% target
- **✅ 8/8 API endpoints implemented** - Complete functionality coverage
- **✅ Quality validation active** - Comprehensive scoring and monitoring
- **✅ Production deployment ready** - PostgreSQL configuration prepared

### Next Milestones

- **🔄 Achieve 95% parsing success** - Fine-tune edge cases
- **🔄 Production deployment** - Deploy to Render with PostgreSQL
- **🔄 Phase 2 planning** - Advanced analytics and reporting features
- **🔄 Integration testing** - Connect with dashboard applications

## 🤝 Contributing & Development

### Working with Claude

This project includes comprehensive documentation and validation tools designed for Claude-assisted development:

1. **Context Sharing**: Complete project knowledge in `trend_data_project_guidelines.txt`
2. **Validation Tools**: Scripts to quickly assess system health
3. **Modular Design**: Easy to focus on specific components
4. **Sample Data**: Real-world test cases for validation

### Development Commands

```bash

# Health check - run this first in any new Claude session

python scripts/validate_setup.py

# Process and validate sample data

python scripts/validate_real_samples.py

# Start API for testing

uvicorn src.api.main:app --reload

# Quick end-to-end demo

python scripts/quick_start_demo.py

```text

## 📞 Support & Documentation

- **Setup Guide**: See `SETUP.md` for complete installation instructions
- **Usage Guide**: See `USAGE.md` for operational workflows and maintenance
- **API Documentation**: Available at `/docs` when running the server
- **Sample Data**: Included in `data/sample/` for testing
- **Validation Reports**: Generated in `reports/` directory

## 📄 License & Credits

Built for streaming analytics and cross-platform insights. Designed for collaborative development with Claude AI assistance.

---

**Ready to transform streaming data chaos into business insights!** 🎵📊
