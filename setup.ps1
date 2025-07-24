# setup.ps1 - PowerShell Development Commands (Updated for SQLite + Render workflow)
# Trend Data ETL Platform - No Docker Required

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

function Show-Help {
    Write-Host "Trend Data ETL Platform - PowerShell Commands" -ForegroundColor Green
    Write-Host ""
    Write-Host "Usage: .\setup.ps1 <command>" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Development Commands:" -ForegroundColor Cyan
    Write-Host "  init           - Install dependencies and setup local environment"
    Write-Host "  validate       - Validate setup and configuration" 
    Write-Host "  db-sqlite      - Setup SQLite database for testing"
    Write-Host "  db-postgres    - Setup connection to Render PostgreSQL"
    Write-Host "  test-samples   - Test with sample data files"
    Write-Host "  serve          - Start API server locally"
    Write-Host "  demo           - Run quick start demo"
    Write-Host ""
    Write-Host "Testing & Quality:" -ForegroundColor Cyan
    Write-Host "  test           - Run unit tests"
    Write-Host "  lint           - Run code linting"
    Write-Host "  format         - Format code with black"
    Write-Host ""
    Write-Host "Deployment Commands:" -ForegroundColor Cyan
    Write-Host "  render-prep    - Prepare for Render deployment"
    Write-Host "  render-init    - Initialize production database on Render"
    Write-Host "  render-test    - Test production deployment"
    Write-Host ""
    Write-Host "Utilities:" -ForegroundColor Cyan
    Write-Host "  clean          - Clean temporary files and databases"
    Write-Host "  reset          - Reset local environment"
    Write-Host ""
}

function Initialize-Environment {
    param(
        [switch]$ProductionOnly
    )
    
    Write-Host "Setting up development environment..." -ForegroundColor Yellow

    # Check Python version
    $pythonVersion = python --version 2>$null
    if (-not $pythonVersion) {
        Write-Host "❌ Python not found. Please install Python 3.9+" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✅ Found $pythonVersion" -ForegroundColor Green

    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "venv")) {
        Write-Host "Creating virtual environment..." -ForegroundColor Yellow
        python -m venv venv
    }

    # Activate virtual environment
    Write-Host "Activating virtual environment..." -ForegroundColor Yellow
    & .\venv\Scripts\Activate.ps1

    # Install dependencies
    Write-Host "Installing production dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt

    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to install production dependencies" -ForegroundColor Red
        exit 1
    }

    # Install dev dependencies unless production-only flag is set
    if (-not $ProductionOnly) {
        if (Test-Path "requirements-dev.txt") {
            Write-Host "Installing development dependencies..." -ForegroundColor Yellow
            Write-Host "  📦 Enhanced debugging (ipython, jupyter)" -ForegroundColor Gray
            Write-Host "  🧪 Testing tools (pytest extensions)" -ForegroundColor Gray
            Write-Host "  🔍 Code quality (pre-commit, bandit, safety)" -ForegroundColor Gray
            Write-Host "  📊 Database tools (sqlite-utils, pgcli)" -ForegroundColor Gray
            Write-Host "  📚 Documentation (sphinx, mkdocs)" -ForegroundColor Gray
            
            pip install -r requirements-dev.txt
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ Development dependencies installed successfully" -ForegroundColor Green
            } else {
                Write-Host "⚠️  Some development dependencies failed to install" -ForegroundColor Yellow
                Write-Host "   This won't affect core functionality" -ForegroundColor Gray
            }
        } else {
            Write-Host "⚠️  requirements-dev.txt not found, skipping dev dependencies" -ForegroundColor Yellow
        }
    } else {
        Write-Host "🏭 Production-only mode: skipping development dependencies" -ForegroundColor Cyan
    }

    Write-Host "✅ Dependencies installed successfully" -ForegroundColor Green

    # Setup environment file
    if (-not (Test-Path ".env")) {
        Write-Host "Creating .env file from template..." -ForegroundColor Yellow
        
        # Create basic .env file
        @"
# Database Configuration
DATABASE_URL=sqlite:///temp/trend_data_test.db

# Data Quality Settings  
QUALITY_THRESHOLD=90
DATABASE_DEBUG=false

# For Render deployment (update with your actual values)
# DATABASE_URL=postgresql://username:password@dpg-xxxxx-a.oregon-postgres.render.com/database_name

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
"@ | Out-File -FilePath ".env" -Encoding UTF8

        Write-Host "✅ Created .env file (edit as needed)" -ForegroundColor Green
    } else {
        Write-Host "✅ .env file already exists" -ForegroundColor Yellow
    }

    Write-Host ""
    Write-Host "🚀 Setup complete!" -ForegroundColor Green
    
    if (-not $ProductionOnly) {
        Write-Host "📋 Development tools now available:" -ForegroundColor Cyan
        Write-Host "   • ipython - Enhanced Python shell" -ForegroundColor Gray
        Write-Host "   • jupyter notebook - Interactive development" -ForegroundColor Gray
        Write-Host "   • pytest --cov - Testing with coverage" -ForegroundColor Gray
        Write-Host "   • sqlite-utils - Database inspection" -ForegroundColor Gray
        Write-Host "   • pre-commit - Git hooks for code quality" -ForegroundColor Gray
        Write-Host ""
    }
    
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. .\setup.ps1 db-sqlite     # Setup local database"
    Write-Host "  2. .\setup.ps1 demo          # Run quick demo"
    Write-Host "  3. .\setup.ps1 test-samples  # Test with your sample data"
}

function Initialize-SQLiteDatabase {
    Write-Host "Setting up SQLite database for testing..." -ForegroundColor Yellow
    
    # Create temp directory
    $tempDir = "temp"
    if (-not (Test-Path $tempDir)) {
        New-Item -ItemType Directory -Path $tempDir | Out-Null
    }

    # Run SQLite setup
    python scripts/setup_sqlite.py
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ SQLite database setup complete" -ForegroundColor Green
        Write-Host "   Database: temp/trend_data_test.db" -ForegroundColor Gray
        Write-Host "   Ready for testing!" -ForegroundColor Gray
    } else {
        Write-Host "❌ SQLite setup failed" -ForegroundColor Red
    }
}

function Initialize-PostgreSQLConnection {
    Write-Host "Setting up PostgreSQL connection..." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To connect to Render PostgreSQL:" -ForegroundColor Cyan
    Write-Host "1. Create PostgreSQL database on Render" -ForegroundColor White
    Write-Host "2. Get connection string from Render dashboard" -ForegroundColor White
    Write-Host "3. Update DATABASE_URL in .env file" -ForegroundColor White
    Write-Host "4. Run: .\setup.ps1 render-init" -ForegroundColor White
    Write-Host ""
    Write-Host "Example DATABASE_URL format:" -ForegroundColor Yellow
    Write-Host "postgresql://username:password@dpg-xxxxx-a.oregon-postgres.render.com/database_name" -ForegroundColor Gray
}

function Test-SetupValidation {
    Write-Host "Running setup validation..." -ForegroundColor Yellow
    python scripts/validate_setup.py
}

function Test-SampleData {
    Write-Host "Testing with sample data files..." -ForegroundColor Yellow
    
    # Check if sample directory exists
    if (-not (Test-Path "data/sample")) {
        Write-Host "Creating sample data directory..." -ForegroundColor Yellow
        New-Item -ItemType Directory -Path "data/sample" -Force | Out-Null
    }

    # Run sample validation
    python scripts/validate_real_samples.py
}

function Start-APIServer {
    Write-Host "Starting API server..." -ForegroundColor Yellow
    Write-Host "API will be available at: http://localhost:8000" -ForegroundColor Green
    Write-Host "API documentation: http://localhost:8000/docs" -ForegroundColor Green
    Write-Host ""
    Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
    Write-Host ""
    
    uvicorn scripts.quick_start_demo:app --reload --host 0.0.0.0 --port 8000
}

function Start-Demo {
    Write-Host "Running quick start demo..." -ForegroundColor Yellow
    python scripts/quick_start_demo.py
}

function Invoke-Tests {
    Write-Host "Running tests..." -ForegroundColor Yellow
    if (Test-Path "tests") {
        pytest tests/ -v --cov=src --cov-report=html
    } else {
        Write-Host "⚠️  No tests directory found" -ForegroundColor Yellow
        Write-Host "Creating basic test structure..." -ForegroundColor Yellow
        New-Item -ItemType Directory -Path "tests" -Force | Out-Null
        
        # Create basic test file
        @"
# tests/test_basic.py
"""Basic tests to ensure system is working"""

def test_import_main_modules():
    \"\"\"Test that main modules can be imported\"\"\"
    from src.etl.parsers.enhanced_parser import EnhancedETLParser
    from src.etl.validators.data_validator import StreamingDataValidator
    from src.database.models import DatabaseManager
    
    assert EnhancedETLParser is not None
    assert StreamingDataValidator is not None
    assert DatabaseManager is not None

def test_sqlite_connection():
    \"\"\"Test SQLite database connection\"\"\"
    import os
    import tempfile
    from src.database.models import DatabaseManager
    
    # Create temporary database
    db_path = os.path.join(tempfile.gettempdir(), "test_trend_data.db")
    db_url = f"sqlite:///{db_path}"
    
    # Test connection
    db_manager = DatabaseManager(db_url)
    assert db_manager is not None
    
    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)
"@ | Out-File -FilePath "tests/test_basic.py" -Encoding UTF8

        Write-Host "✅ Created basic test structure" -ForegroundColor Green
        pytest tests/ -v
    }
}

function Invoke-Linting {
    Write-Host "Running code linting..." -ForegroundColor Yellow
    
    # Check if flake8 is installed
    $flake8Check = pip show flake8 2>$null
    if (-not $flake8Check) {
        Write-Host "Installing flake8..." -ForegroundColor Yellow
        pip install flake8
    }
    
    flake8 src/ --max-line-length=120 --ignore=E203,W503
    
    # Check if mypy is installed
    $mypyCheck = pip show mypy 2>$null
    if (-not $mypyCheck) {
        Write-Host "Installing mypy..." -ForegroundColor Yellow
        pip install mypy
    }
    
    mypy src/ --ignore-missing-imports
}

function Format-Code {
    Write-Host "Formatting code with black..." -ForegroundColor Yellow
    
    # Check if black is installed
    $blackCheck = pip show black 2>$null
    if (-not $blackCheck) {
        Write-Host "Installing black..." -ForegroundColor Yellow
        pip install black
    }
    
    black src/ scripts/ --line-length=120
    Write-Host "✅ Code formatted" -ForegroundColor Green
}

function Initialize-RenderDeployment {
    Write-Host "Preparing for Render deployment..." -ForegroundColor Yellow
    
    # Create render.yaml if it doesn't exist
    if (-not (Test-Path "render.yaml")) {
        Write-Host "Creating render.yaml configuration..." -ForegroundColor Yellow
        
        @"
services:
  - type: web
    name: trend-data-etl-api
    runtime: python
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn scripts.quick_start_demo:app --host 0.0.0.0 --port `$PORT
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: trend-data-etl-db
          property: connectionString
      - key: QUALITY_THRESHOLD
        value: "90"
      - key: DATABASE_DEBUG
        value: "false"

databases:
  - name: trend-data-etl-db
    plan: starter
    databaseName: trend_data_etl
    user: etl_user
"@ | Out-File -FilePath "render.yaml" -Encoding UTF8
        
        Write-Host "✅ Created render.yaml configuration" -ForegroundColor Green
    }
    
    # Verify requirements.txt has all needed packages
    Write-Host "Verifying requirements.txt..." -ForegroundColor Yellow
    
    # Check key packages
    $requiredPackages = @(
        "fastapi",
        "uvicorn", 
        "sqlalchemy",
        "psycopg2-binary",
        "pandas",
        "python-dotenv",
        "chardet",
        "python-dateutil",
        "pydantic"
    )
    
    $requirements = Get-Content "requirements.txt" -ErrorAction SilentlyContinue
    $missingPackages = @()
    
    foreach ($package in $requiredPackages) {
        $found = $requirements | Where-Object { $_ -like "$package*" }
        if (-not $found) {
            $missingPackages += $package
        }
    }
    
    if ($missingPackages.Count -gt 0) {
        Write-Host "⚠️  Missing packages in requirements.txt:" -ForegroundColor Yellow
        $missingPackages | ForEach-Object { Write-Host "   - $_" -ForegroundColor Gray }
        Write-Host "Run: pip freeze > requirements.txt" -ForegroundColor Yellow
    } else {
        Write-Host "✅ All required packages found in requirements.txt" -ForegroundColor Green
    }
    
    Write-Host ""
    Write-Host "🚀 Render deployment preparation complete!" -ForegroundColor Green
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Create PostgreSQL database on Render" -ForegroundColor White
    Write-Host "2. Create web service on Render (connect to your GitHub repo)" -ForegroundColor White
    Write-Host "3. Run: .\setup.ps1 render-init (after deployment)" -ForegroundColor White
}

function Initialize-RenderDatabase {
    Write-Host "Initializing production database on Render..." -ForegroundColor Yellow
    Write-Host "Make sure DATABASE_URL is set to your Render PostgreSQL connection string" -ForegroundColor Yellow
    
    python scripts/init_render_db.py
}

function Test-RenderDeployment {
    Write-Host "Testing Render deployment..." -ForegroundColor Yellow
    Write-Host "Enter your Render app URL (e.g., https://your-app.onrender.com):" -ForegroundColor Yellow
    $renderUrl = Read-Host
    
    if ($renderUrl) {
        Write-Host "Testing endpoints..." -ForegroundColor Yellow
        
        try {
            Invoke-RestMethod -Uri "$renderUrl/health" -Method Get | Out-Null
            Write-Host "✅ Health check passed" -ForegroundColor Green
            
            $platformsResponse = Invoke-RestMethod -Uri "$renderUrl/platforms" -Method Get
            Write-Host "✅ Platforms endpoint working" -ForegroundColor Green
            Write-Host "   Found $($platformsResponse.Count) platforms" -ForegroundColor Gray
            
            Write-Host ""
            Write-Host "🚀 Render deployment is working!" -ForegroundColor Green
            Write-Host "API Documentation: $renderUrl/docs" -ForegroundColor Cyan
            
        } catch {
            Write-Host "❌ Deployment test failed: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

function Clear-Environment {
    Write-Host "Cleaning temporary files and databases..." -ForegroundColor Yellow
    
    # Remove temporary database
    if (Test-Path "temp/trend_data_test.db") {
        Remove-Item "temp/trend_data_test.db" -Force
        Write-Host "✅ Removed temporary SQLite database" -ForegroundColor Green
    }
    
    # Clean Python cache
    Get-ChildItem -Path . -Recurse -Name "__pycache__" | Remove-Item -Recurse -Force
    Get-ChildItem -Path . -Recurse -Name "*.pyc" | Remove-Item -Force
    
    # Clean test artifacts
    if (Test-Path "htmlcov") {
        Remove-Item "htmlcov" -Recurse -Force
    }
    if (Test-Path ".coverage") {
        Remove-Item ".coverage" -Force
    }
    
    Write-Host "✅ Cleanup complete" -ForegroundColor Green
}

function Reset-Environment {
    Write-Host "Resetting local environment..." -ForegroundColor Yellow
    
    Clear-Environment
    
    # Remove virtual environment
    if (Test-Path "venv") {
        Remove-Item "venv" -Recurse -Force
        Write-Host "✅ Removed virtual environment" -ForegroundColor Green
    }
    
    Write-Host "Environment reset. Run '.\setup.ps1 init' to reinitialize." -ForegroundColor Yellow
}

# Main command dispatcher
switch ($Command.ToLower()) {
    "help"           { Show-Help }
    "init"           { Initialize-Environment }
    "validate"       { Test-SetupValidation }
    "db-sqlite"      { Initialize-SQLiteDatabase }
    "db-postgres"    { Initialize-PostgreSQLConnection }
    "test-samples"   { Test-SampleData }
    "serve"          { Start-APIServer }
    "demo"           { Start-Demo }
    "test"           { Invoke-Tests }
    "lint"           { Invoke-Linting }
    "format"         { Format-Code }
    "render-prep"    { Initialize-RenderDeployment }
    "render-init"    { Initialize-RenderDatabase }
    "render-test"    { Test-RenderDeployment }
    "clean"          { Clear-Environment }
    "reset"          { Reset-Environment }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host ""
        Show-Help
        exit 1
    }
}