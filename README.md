### README.md
```markdown
# Trend Data ETL Platform

A comprehensive streaming analytics platform that aggregates disparate streaming data from multiple digital platforms (Spotify, Apple Music, Facebook, SoundCloud, etc.) into a unified, queryable format.

## 🎯 Project Overview

**Phase 1 Complete:** Data Platform Foundation + Data Access API
- Transforms messy, disparate streaming data into clean, unified format
- Handles complex real-world data issues (quote-wrapped formats, mixed dates, etc.)
- Provides robust API access with quality scoring
- Targets 95%+ parsing success rate and 90%+ data quality scores

**Phase 2 Planned:** Advanced Analytics & Reporting Engine
- Cross-platform metrics and trend detection
- Spotify Wrapped-style insights for all artists
- Business intelligence integration

## 🏗️ Architecture
Raw Files → ETL Pipeline → TimescaleDB → Data Access API → Analytics Layer

## 📊 Platform Support

**Currently Supports 9 Streaming Platforms:**
- Spotify (spo) - Weekly subdirectories
- Apple Music/iTunes (apl) - Complex vendor IDs, multi-currency
- Facebook/Meta (fbk) - Social interaction tracking
- SoundCloud (scu) - Multiple file types per date
- Deezer (dzr) - Standard streaming metrics
- Boomplay (boo) - African market focus
- AWA (awa) - Japanese market specialization
- Vevo (vvo) - Video-specific metrics
- Peloton (plt) - Fitness context tracking

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker Desktop
- PostgreSQL knowledge (helpful)

### Installation

1. **Clone and setup:**
```bash
git clone https://github.com/yourusername/trend_data_etl.git
cd trend_data_etl
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt