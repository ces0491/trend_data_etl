# setup.ps1 - PowerShell equivalent of Makefile commands 
# Trend Data ETL Platform - Development Commands

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

function Show-Help {
    Write-Host "Trend Data ETL Platform - PowerShell Commands" -ForegroundColor Green
    Write-Host ""
    Write-Host "Usage: .\setup.ps1 <command>" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Available commands:" -ForegroundColor Cyan
    Write-Host "  initialize     - Install dependencies and setup environment"
    Write-Host "  db-up          - Start database containers"
    Write-Host "  db-down        - Stop database containers"
    Write-Host "  db-reset       - Reset database and apply migrations"
    Write-Host "  test           - Run tests"
    Write-Host "  lint           - Run code linting"
    Write-Host "  format         - Format code"
    Write-Host "  serve          - Start API server"
    Write-Host "  dev            - Start database and API server"
    Write-Host "  check-data     - Run data quality check"
    Write-Host "  process-data   - Process sample data"
    Write-Host ""
}

function Install-Dependencies {
    Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
    pip install -r requirements.txt
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Dependencies installed successfully" -ForegroundColor Green
    } else {
        Write-Host "Failed to install dependencies" -ForegroundColor Red
        exit 1
    }
}

function Initialize-Environment {
    Write-Host "Setting up environment..." -ForegroundColor Yellow

    Install-Dependencies

    if (-not (Test-Path ".env")) {
        Copy-Item ".env.template" ".env"
        Write-Host "Environment template copied to .env" -ForegroundColor Green
        Write-Host "Please edit .env file with your configuration" -ForegroundColor Yellow
    } else {
        Write-Host ".env file already exists, skipping copy" -ForegroundColor Yellow
    }

    Write-Host "Setup complete!" -ForegroundColor Green
}

function Start-Database {
    Write-Host "Starting database containers..." -ForegroundColor Yellow
    docker-compose up -d timescaledb redis
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Database containers started" -ForegroundColor Green
    } else {
        Write-Host "Failed to start database containers" -ForegroundColor Red
        Write-Host "Make sure Docker is installed and running" -ForegroundColor Yellow
        exit 1
    }
}

function Stop-Database {
    Write-Host "Stopping database containers..." -ForegroundColor Yellow
    docker-compose down
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Database containers stopped" -ForegroundColor Green
    } else {
        Write-Host "Failed to stop database containers" -ForegroundColor Red
    }
}

function Reset-Database {
    Write-Host "Resetting database..." -ForegroundColor Yellow
    docker-compose down -v
    docker-compose up -d timescaledb redis

    Write-Host "Waiting for database to start..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10

    # Note: Alembic not set up yet, so skipping migrations for now
    # alembic upgrade head

    Write-Host "Database reset complete" -ForegroundColor Green
}

function Invoke-Test {
    Write-Host "Running tests..." -ForegroundColor Yellow
    pytest tests/ -v --cov=src --cov-report=html
}

function Invoke-LintCheck {
    Write-Host "Running code linting..." -ForegroundColor Yellow
    flake8 src/ tests/
    mypy src/
}

function Format-Code {
    Write-Host "Formatting code..." -ForegroundColor Yellow
    black src/ tests/
    Write-Host "Code formatted" -ForegroundColor Green
}

function Start-API {
    Write-Host "Starting API server..." -ForegroundColor Yellow
    uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
}

function Start-Development {
    Write-Host "Starting development environment..." -ForegroundColor Yellow
    Start-Database
    Start-Sleep -Seconds 5
    Start-API
}

function Test-DataQuality {
    Write-Host "Running data quality check..." -ForegroundColor Yellow
    python scripts/data_quality_check.py
}

function Invoke-SampleProcessing {
    Write-Host "Processing sample data..." -ForegroundColor Yellow
    python scripts/process_sample_data.py
}

# Main command dispatcher
switch ($Command.ToLower()) {
    "help"           { Show-Help }
    "initialize"     { Initialize-Environment }
    "db-up"          { Start-Database }
    "db-down"        { Stop-Database }
    "db-reset"       { Reset-Database }
    "test"           { Invoke-Test }
    "lint"           { Invoke-LintCheck }
    "format"         { Format-Code }
    "serve"          { Start-API }
    "dev"            { Start-Development }
    "check-data"     { Test-DataQuality }
    "process-data"   { Invoke-SampleProcessing }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host ""
        Show-Help
        exit 1
    }
}
