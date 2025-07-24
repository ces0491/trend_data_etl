#!/usr/bin/env python3
"""
from __future__ import annotations

Complete Setup Validation Script
Validates that all components are properly configured and can be imported
"""

import importlib
import sys
import os
from pathlib import Path
import tempfile
import traceback

# Add src to path with multiple fallback strategies
def setup_python_path():
    """Setup Python path to find our modules"""
    # Strategy 1: Standard project structure
    project_root = Path(__file__).parent.parent
    src_dir = project_root / "src"
    
    # Strategy 2: If we're in scripts/, go up one level
    if "scripts" in str(Path(__file__).parent):
        project_root = Path(__file__).parent.parent
        src_dir = project_root / "src"
    
    # Strategy 3: Current directory fallback
    if not src_dir.exists():
        current_dir = Path.cwd()
        src_dir = current_dir / "src"
        project_root = current_dir
    
    # Add to path if exists
    if src_dir.exists():
        src_path = str(src_dir.absolute())
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        return True, project_root, src_dir
    
    return False, project_root, src_dir

# Setup paths
PATH_SETUP_SUCCESS, PROJECT_ROOT, SRC_DIR = setup_python_path()

class ValidationResult:
    """Container for validation results"""
    def __init__(self, passed: bool, message: str, details: list[str] | None = None):
        self.passed = passed
        self.message = message
        self.details = details or []
        
    def __bool__(self):
        return self.passed

def validate_python_version() -> ValidationResult:
    """Validate Python version"""
    print("🔍 Checking Python version...")
    
    min_version = (3, 8)
    current_version = sys.version_info[:2]
    
    if current_version < min_version:
        return ValidationResult(
            False, 
            f"Python {min_version[0]}.{min_version[1]}+ required, found {current_version[0]}.{current_version[1]}",
            [f"Please upgrade to Python {min_version[0]}.{min_version[1]} or higher"]
        )
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    return ValidationResult(True, f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")


def validate_required_packages() -> ValidationResult:
    """Validate that required Python packages are installed"""
    print("\n🔍 Validating required packages...")
    
    # Core packages that must be available
    # Format: (import_name, package_name, description)
    required_packages = [
        ("fastapi", "fastapi", "FastAPI web framework"),
        ("uvicorn", "uvicorn", "ASGI server"),
        ("sqlalchemy", "sqlalchemy", "Database ORM"),
        ("pandas", "pandas", "Data processing"),
        ("dotenv", "python-dotenv", "Environment configuration"),  # Fixed: import 'dotenv', not 'python_dotenv'
        ("chardet", "chardet", "Character encoding detection"),
        ("dateutil", "python-dateutil", "Date parsing"),
        ("openpyxl", "openpyxl", "Excel file support"),
        ("psycopg2", "psycopg2-binary", "PostgreSQL driver (install as psycopg2-binary)")
    ]
    
    # Optional packages that enhance functionality
    optional_packages = [
        ("xlrd", "xlrd", "Legacy Excel support"),
        ("requests", "requests", "HTTP client"),
    ]
    
    failed_required = []
    failed_optional = []
    
    # Test required packages
    for import_name, package_name, description in required_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name} ({description})")
        except ImportError:
            failed_required.append((package_name, description))
            print(f"❌ {package_name}: {description}")
    
    # Test optional packages
    for import_name, package_name, description in optional_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name} ({description}) - optional")
        except ImportError:
            failed_optional.append((package_name, description))
            print(f"⚠️  {package_name}: {description} - optional, missing")
    
    if failed_required:
        # Provide specific installation instructions for failed packages
        install_suggestions = []
        for pkg, desc in failed_required:
            install_suggestions.append(f"pip install {pkg}")
        
        return ValidationResult(
            False,
            f"Missing {len(failed_required)} required packages",
            install_suggestions
        )
    
    message = "All required packages installed"
    if failed_optional:
        message += f" ({len(failed_optional)} optional packages missing)"
    
    return ValidationResult(True, message)

def validate_project_structure() -> ValidationResult:
    """Validate project directory structure"""
    print("\n🔍 Validating project structure...")
    
    required_dirs = [
        "src",
        "src/database", 
        "src/etl",
        "src/etl/parsers",
        "src/etl/validators",
        "scripts",
    ]
    
    required_files = [
        "requirements.txt",
        "render.yaml",
        ".env.template",
    ]
    
    # Files that should exist for full functionality
    important_files = [
        "src/__init__.py",
        "src/database/__init__.py", 
        "src/database/models.py",
        "src/etl/__init__.py",
        "src/etl/parsers/__init__.py",
        "src/etl/validators/__init__.py",
        "src/etl/data_processor.py",
    ]
    
    missing_dirs = []
    missing_required = []
    missing_important = []
    
    # Check directories
    for directory in required_dirs:
        dir_path = PROJECT_ROOT / directory
        if not dir_path.exists():
            missing_dirs.append(directory)
            print(f"❌ Missing directory: {directory}")
        else:
            print(f"✅ {directory}/")
    
    # Check required files
    for file_path in required_files:
        full_path = PROJECT_ROOT / file_path
        if not full_path.exists():
            missing_required.append(file_path)
            print(f"❌ Missing required file: {file_path}")
        else:
            print(f"✅ {file_path}")
    
    # Check important files
    for file_path in important_files:
        full_path = PROJECT_ROOT / file_path
        if not full_path.exists():
            missing_important.append(file_path)
            print(f"⚠️  Missing important file: {file_path}")
        else:
            print(f"✅ {file_path}")
    
    # Determine result
    critical_missing = missing_dirs + missing_required
    if critical_missing:
        return ValidationResult(
            False,
            f"Missing {len(critical_missing)} critical items",
            [f"Create: {item}" for item in critical_missing]
        )
    
    message = "Project structure is valid"
    if missing_important:
        message += f" ({len(missing_important)} important files missing)"
    
    return ValidationResult(True, message, missing_important)

def validate_custom_modules() -> ValidationResult:
    """Validate that our custom modules can be imported"""
    print("\n🔍 Validating custom modules...")
    
    if not PATH_SETUP_SUCCESS:
        return ValidationResult(
            False,
            "Cannot find src directory",
            ["Ensure you're running from project root", "Check that src/ directory exists"]
        )
    
    # Modules to test with fallback handling
    modules_to_test = [
        ("database", None, "Database package"),
        ("database.models", "DatabaseManager", "Database models"),
        ("etl", None, "ETL package"),
        ("etl.data_processor", "StreamingDataProcessor", "Data processor"),
        ("etl.parsers", None, "Parsers package"),
        ("etl.validators", None, "Validators package"),
    ]
    
    failed_imports = []
    warnings = []
    
    for module_name, class_name, description in modules_to_test:
        try:
            module = importlib.import_module(module_name)
            
            if class_name:
                # Test specific class
                if hasattr(module, class_name):
                    print(f"✅ {module_name}.{class_name} ({description})")
                else:
                    warnings.append(f"{module_name}.{class_name} class not found")
                    print(f"⚠️  {module_name} exists but {class_name} class missing")
            else:
                # Just test module import
                print(f"✅ {module_name} ({description})")
                
        except ImportError as e:
            failed_imports.append((module_name, str(e)))
            print(f"❌ {module_name}: {e}")
        except Exception as e:
            failed_imports.append((module_name, f"Unexpected error: {e}"))
            print(f"❌ {module_name}: Unexpected error: {e}")
    
    if failed_imports:
        return ValidationResult(
            False,
            f"Failed to import {len(failed_imports)} modules",
            [f"{mod}: {err}" for mod, err in failed_imports]
        )
    
    message = "All custom modules importable"
    if warnings:
        message += f" ({len(warnings)} warnings)"
    
    return ValidationResult(True, message, warnings)

def validate_environment_config() -> ValidationResult:
    """Validate environment configuration"""
    print("\n🔍 Validating environment configuration...")
    
    env_file = PROJECT_ROOT / ".env"
    env_template = PROJECT_ROOT / ".env.template"
    
    # Check if .env exists
    if not env_file.exists():
        if env_template.exists():
            print("⚠️  .env file missing but template found")
            return ValidationResult(
                False,
                ".env file missing",
                ["Copy .env.template to .env", "Configure environment variables"]
            )
        else:
            print("❌ No .env or .env.template file found")
            return ValidationResult(
                False,
                "No environment configuration found",
                ["Create .env file with DATABASE_URL and other config"]
            )
    
    # Load environment variables
    try:
        # Try to import and load dotenv
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            # Fallback: read .env manually
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        
        # Required environment variables
        required_vars = ['DATABASE_URL']
        optional_vars = ['DEBUG', 'API_HOST', 'API_PORT', 'BATCH_SIZE']
        
        missing_required = []
        set_optional = []
        
        for var in required_vars:
            value = os.getenv(var)
            if not value:
                missing_required.append(var)
                print(f"❌ {var}: Not set")
            else:
                # Don't print full DATABASE_URL for security
                if var == 'DATABASE_URL':
                    print(f"✅ {var}: {'*' * min(20, len(value))}...")
                else:
                    print(f"✅ {var}: {value}")
        
        for var in optional_vars:
            value = os.getenv(var)
            if value:
                print(f"✅ {var}: {value}")
                set_optional.append(var)
            else:
                print(f"⚠️  {var}: Using default")
        
        if missing_required:
            return ValidationResult(
                False,
                f"Missing required environment variables: {', '.join(missing_required)}",
                [f"Set {var} in .env file" for var in missing_required]
            )
        
        return ValidationResult(
            True,
            f"Environment configured ({len(set_optional)} optional vars set)"
        )
        
    except Exception as e:
        return ValidationResult(
            False,
            f"Environment configuration error: {e}",
            ["Check .env file format", "Ensure no syntax errors"]
        )

def validate_database_connection() -> ValidationResult:
    """Validate database connection"""
    print("\n🔍 Validating database connection...")
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        return ValidationResult(
            False,
            "DATABASE_URL not configured",
            ["Set DATABASE_URL in .env file"]
        )
    
    # Show partial URL for security
    url_preview = db_url[:20] + "..." if len(db_url) > 20 else db_url
    print(f"   Testing connection to: {url_preview}")
    
    try:
        # Try to import our database models
        try:
            from database.models import DatabaseManager
        except ImportError:
            # Fallback: test with SQLAlchemy directly
            try:
                from sqlalchemy import create_engine, text
                engine = create_engine(db_url)
                
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    test_result = result.fetchone()
                    
                if test_result and test_result[0] == 1:
                    print("✅ Database connection successful (using SQLAlchemy directly)")
                    return ValidationResult(True, "Database connection working")
                else:
                    return ValidationResult(False, "Database test query failed")
                    
            except ImportError:
                return ValidationResult(
                    False,
                    "Cannot test database: SQLAlchemy not available",
                    ["Install SQLAlchemy: pip install sqlalchemy"]
                )
        
        # Test with our DatabaseManager
        db_manager = DatabaseManager(db_url)
        
        with db_manager.get_session() as session:
            # Test basic query
            from sqlalchemy import text
            result = session.execute(text("SELECT 1 as test"))
            test_result = result.fetchone()
            
            if test_result and test_result[0] == 1:
                print("✅ Database connection successful")
                
                # Additional database info if possible
                try:
                    version_result = session.execute(text("SELECT version()"))
                    version = version_result.fetchone()
                    if version:
                        db_version = version[0][:50] + "..." if len(version[0]) > 50 else version[0]
                        print(f"   Database: {db_version}")
                except:
                    pass  # Version query not critical
                
                return ValidationResult(True, "Database connection and queries working")
            else:
                return ValidationResult(False, "Database test query returned unexpected result")
        
    except Exception as e:
        error_msg = str(e)
        if "could not connect" in error_msg.lower():
            suggestions = [
                "Check DATABASE_URL format",
                "Ensure database server is running",
                "Verify network connectivity"
            ]
        elif "authentication" in error_msg.lower():
            suggestions = [
                "Check database username/password",
                "Verify database user permissions"
            ]
        else:
            suggestions = ["Check database configuration", f"Error details: {error_msg}"]
        
        return ValidationResult(
            False,
            f"Database connection failed: {error_msg}",
            suggestions
        )

def validate_sample_data_processing() -> ValidationResult:
    """Validate that sample data can be processed"""
    print("\n🔍 Validating sample data processing...")
    
    try:
        # Create temporary test data with recognizable platform filename
        temp_dir = Path(tempfile.gettempdir()) / "streaming_test"
        temp_dir.mkdir(exist_ok=True)
        
        # Create a test file with Spotify platform pattern
        test_file = temp_dir / "spo-spotify_validation_test.tsv"
        
        test_data = "artist_name\ttrack_name\tstreams\tdate\n"
        test_data += "Test Artist\tTest Song\t1000\t2024-01-01\n"
        test_data += "Another Artist\tAnother Song\t2000\t2024-01-02\n"
        
        test_file.write_text(test_data, encoding='utf-8')
        
        try:
            # Try to use our enhanced parser
            try:
                from etl.parsers.enhanced_parser import EnhancedETLParser
                parser = EnhancedETLParser()
                result = parser.parse_file(test_file)
                
                if hasattr(result, 'success') and result.success:
                    records_count = getattr(result, 'records_parsed', 0)
                    print(f"✅ Sample data parsing successful ({records_count} records)")
                    return ValidationResult(True, f"Data processing working ({records_count} records parsed)")
                else:
                    error_msg = getattr(result, 'error_message', 'Unknown error')
                    return ValidationResult(False, f"Parser returned failure: {error_msg}")
                    
            except ImportError:
                # Fallback: test with pandas directly
                import pandas as pd
                df = pd.read_csv(test_file, sep='\t')
                
                if len(df) == 2 and 'artist_name' in df.columns:
                    print("✅ Sample data parsing successful (using pandas fallback)")
                    return ValidationResult(True, f"Basic data processing working ({len(df)} records)")
                else:
                    return ValidationResult(False, "Data parsing produced unexpected results")
                    
        finally:
            # Clean up temporary file
            try:
                test_file.unlink()
                temp_dir.rmdir()
            except:
                pass  # Cleanup failure is not critical
                
    except Exception as e:
        return ValidationResult(
            False,
            f"Sample data processing failed: {e}",
            ["Check data processing modules", "Verify pandas installation"]
        )

def create_missing_init_files() -> None:
    """Create missing __init__.py files"""
    print("\n🔧 Creating missing __init__.py files...")
    
    init_dirs = [
        "src",
        "src/database",
        "src/etl",
        "src/etl/parsers", 
        "src/etl/validators",
        "src/api",
        "src/config",
    ]
    
    created_files = []
    for dir_path in init_dirs:
        full_dir = PROJECT_ROOT / dir_path
        if full_dir.exists():
            init_file = full_dir / "__init__.py"
            if not init_file.exists():
                try:
                    init_file.touch()
                    created_files.append(dir_path)
                    print(f"✅ Created {dir_path}/__init__.py")
                except Exception as e:
                    print(f"❌ Failed to create {dir_path}/__init__.py: {e}")
    
    if created_files:
        print(f"✅ Created {len(created_files)} __init__.py files")
    else:
        print("✅ All __init__.py files already exist")

def generate_setup_suggestions(results: dict[str, ValidationResult]) -> list[str]:
    """Generate setup suggestions based on validation results"""
    suggestions = []
    
    failed_checks = [name for name, result in results.items() if not result.passed]
    
    if not results.get("Python Version", ValidationResult(True, "")).passed:
        suggestions.append("🐍 Upgrade Python to 3.8+")
    
    if not results.get("Required Packages", ValidationResult(True, "")).passed:
        suggestions.append("📦 Install missing packages: pip install -r requirements.txt")
    
    if not results.get("Project Structure", ValidationResult(True, "")).passed:
        suggestions.append("📁 Create missing directories and files")
        suggestions.append("🔧 Run initialization script if available")
    
    if not results.get("Custom Modules", ValidationResult(True, "")).passed:
        suggestions.append("🔍 Check Python path configuration")
        suggestions.append("📝 Create missing __init__.py files")
        suggestions.append("🔧 Verify src/ directory structure")
    
    if not results.get("Environment Config", ValidationResult(True, "")).passed:
        suggestions.append("⚙️  Copy .env.template to .env and configure")
        suggestions.append("🔑 Set DATABASE_URL and other required variables")
    
    if not results.get("Database Connection", ValidationResult(True, "")).passed:
        suggestions.append("🗄️  Check database configuration")
        suggestions.append("🔌 Ensure database server is accessible")
    
    if not results.get("Sample Data Processing", ValidationResult(True, "")).passed:
        suggestions.append("📊 Check data processing modules")
        suggestions.append("🔍 Verify parser implementations")
    
    # General suggestions
    if failed_checks:
        suggestions.extend([
            "",
            "🚀 Quick fixes:",
            "   • Run: pip install -r requirements.txt",
            "   • Copy: .env.template → .env", 
            "   • Configure DATABASE_URL in .env",
            "   • Ensure you're in the project root directory"
        ])
    
    return suggestions

def main() -> bool:
    """Main validation function"""
    print("STREAMING ANALYTICS PLATFORM - SETUP VALIDATION")
    print("=" * 65)
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Source Directory: {SRC_DIR} ({'✅ Found' if SRC_DIR.exists() else '❌ Missing'})")
    print()
    
    # Auto-create missing __init__.py files
    create_missing_init_files()
    
    # Define all validation checks
    validation_checks = [
        ("Python Version", validate_python_version),
        ("Required Packages", validate_required_packages),
        ("Project Structure", validate_project_structure),
        ("Custom Modules", validate_custom_modules),
        ("Environment Config", validate_environment_config),
        ("Database Connection", validate_database_connection),
        ("Sample Data Processing", validate_sample_data_processing),
    ]
    
    # Run all validation checks
    results = {}
    all_passed = True
    
    for check_name, check_function in validation_checks:
        try:
            result = check_function()
            results[check_name] = result
            if not result.passed:
                all_passed = False
        except Exception as e:
            print(f"\n❌ {check_name} validation crashed: {e}")
            traceback.print_exc()
            results[check_name] = ValidationResult(False, f"Validation crashed: {e}")
            all_passed = False
    
    # Print summary
    print("\n" + "=" * 65)
    print("VALIDATION SUMMARY")
    print("=" * 65)
    
    for check_name, result in results.items():
        status = "✅ PASSED" if result.passed else "❌ FAILED"
        print(f"{status} - {check_name}")
        
        if not result.passed and result.details:
            for detail in result.details[:3]:  # Show first 3 details
                print(f"         • {detail}")
            if len(result.details) > 3:
                print(f"         • ... and {len(result.details) - 3} more issues")
        elif result.message:
            if not result.passed:
                print(f"         {result.message}")
    
    print()
    
    # Results and next steps
    if all_passed:
        print("🎉 ALL VALIDATION CHECKS PASSED!")
        print("✅ Your setup is ready for development")
        print()
        print("🚀 Next steps:")
        print("   1. python scripts/quick_start_demo.py    # Run demo")
        print("   2. python scripts/init_production_db.py  # Setup production DB")
        print("   3. uvicorn src.api.main:app --reload     # Start API server")
        print("   4. Visit http://localhost:8000/docs      # API documentation")
        
        return True
    else:
        failed_count = sum(1 for result in results.values() if not result.passed)
        print(f"⚠️  {failed_count} VALIDATION CHECKS FAILED")
        print()
        
        suggestions = generate_setup_suggestions(results)
        if suggestions:
            print("🔧 SUGGESTED FIXES:")
            print("-" * 30)
            for suggestion in suggestions:
                print(suggestion)
        
        print()
        print("💡 For detailed help, check the project documentation")
        print("🐛 If issues persist, verify your project structure matches the requirements")
        
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
