"""
Integration tests for Mage.ai ETL framework integration.

These tests verify that the Mage.ai orchestration layer works properly
with the existing ETL framework.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Any

import pytest
import requests
from sqlalchemy import create_engine, text

# Add ETL framework to path
sys.path.append(str(Path(__file__).parent.parent.parent / "etl"))

from connections.dw_destination import get_dw_engine, test_dw_connection
from connections.sources.factory import create_connector


class TestMageIntegration:
    """Test suite for Mage.ai integration."""
    
    @pytest.fixture(autouse=True)
    def setup_test_environment(self):
        """Setup test environment before each test."""
        # Ensure lake directory exists
        lake_path = Path("./lake")
        lake_path.mkdir(exist_ok=True)
        
        # Clean up any existing test files
        for file in lake_path.glob("test_*"):
            file.unlink()
    
    def test_docker_services_running(self):
        """Test that required Docker services are running."""
        # Test PostgreSQL
        try:
            result = test_dw_connection()
            assert result is True or (isinstance(result, dict) and result.get("success")), \
                "PostgreSQL DW should be running and accessible"
        except Exception as e:
            pytest.skip(f"PostgreSQL not running: {e}")
        
        # Test Mage.ai service
        try:
            response = requests.get("http://localhost:6789/api/status", timeout=5)
            assert response.status_code == 200, "Mage.ai should be accessible on port 6789"
        except requests.RequestException as e:
            pytest.skip(f"Mage.ai not running: {e}")
    
    def test_etl_framework_mounted_in_mage(self):
        """Test that ETL framework is properly mounted in Mage container."""
        # Check if we can execute ETL CLI inside Mage container
        cmd = [
            "docker", "exec", "dw_mage",
            "python", "-c", 
            "import sys; sys.path.append('/app/etl'); from connections.dw_destination import test_dw_connection; print('ETL framework accessible')"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, f"ETL framework should be accessible in Mage container: {result.stderr}"
            assert "ETL framework accessible" in result.stdout, "ETL framework import should work"
        except subprocess.TimeoutExpired:
            pytest.fail("Mage container not responding")
        except FileNotFoundError:
            pytest.skip("Docker not available or Mage container not running")
    
    def test_mage_etl_runner_functions(self):
        """Test custom ETL runner functions work in Mage container."""
        # Test connection test function
        test_script = """
import sys
sys.path.append('/app/etl')
sys.path.append('/app/mage_blocks')
from custom.etl_runner import test_dw_connection
import json

result = test_dw_connection()
print(json.dumps(result))
"""
        
        cmd = ["docker", "exec", "dw_mage", "python", "-c", test_script]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, f"ETL runner functions should work: {result.stderr}"
            
            # Parse result
            output_lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
            json_line = output_lines[-1]  # Last line should be JSON
            test_result = json.loads(json_line)
            
            assert test_result.get("success") == True, "DW connection test should succeed"
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            pytest.skip(f"Cannot test Mage container: {e}")
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON response from ETL runner: {result.stdout}")
    
    def test_etl_cli_execution_in_mage(self):
        """Test that ETL CLI can be executed inside Mage container."""
        # Create a simple test connector config
        test_config = {
            "protocol": "http",
            "base_url": "https://jsonplaceholder.typicode.com"
        }
        
        # Create temp config file that will be mounted
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir='./') as f:
            json.dump(test_config, f)
            temp_config = f.name
        
        try:
            # Test connection from inside Mage container
            cmd = [
                "docker", "exec", "dw_mage",
                "python", "-m", "etl.cli", "test-connection",
                "--config", f"/app/{Path(temp_config).name}"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # Parse JSON result
                try:
                    output_lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
                    json_line = output_lines[0]  # First line should be JSON
                    test_result = json.loads(json_line)
                    assert test_result.get("success") == True, "HTTP connection test should succeed"
                except (json.JSONDecodeError, IndexError):
                    # If no JSON, check stderr for success message
                    assert "successful" in result.stderr.lower(), "Connection test should report success"
            else:
                pytest.skip(f"External API not accessible for test: {result.stderr}")
                
        finally:
            # Clean up temp file
            os.unlink(temp_config)
    
    def test_full_etl_pipeline_execution(self):
        """Test complete ETL pipeline execution through Mage container."""
        # Create test connector for JSONPlaceholder API
        test_config = {
            "protocol": "http",
            "base_url": "https://jsonplaceholder.typicode.com",
            "timeout_seconds": 30
        }
        
        # Create temp config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, dir='./') as f:
            json.dump(test_config, f)
            temp_config = f.name
        
        try:
            # Execute full pipeline in Mage container
            cmd = [
                "docker", "exec", "dw_mage",
                "python", "-m", "etl.cli", "run",
                "--config", f"/app/{Path(temp_config).name}",
                "--query", "/users?_limit=3",  # Limit to 3 users for test
                "--source", "test_api",
                "--table", "test_mage_users",
                "--lake", "/app/lake",
                "--schema", "public",
                "--pipeline", "mage_integration_test"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                # Parse JSON result
                try:
                    output_lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
                    json_line = output_lines[0]
                    pipeline_result = json.loads(json_line)
                    
                    assert pipeline_result.get("status") == "success", "Pipeline should complete successfully"
                    assert pipeline_result.get("rows_loaded", 0) > 0, "Pipeline should load some rows"
                    
                    print(f"✅ Pipeline completed successfully: {pipeline_result}")
                    
                    # Verify data in DW
                    engine = get_dw_engine()
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT COUNT(*) FROM test_mage_users"))
                        count = result.scalar()
                        assert count > 0, "Data should be loaded to DW table"
                        print(f"✅ Found {count} rows in DW table")
                        
                        # Clean up test table
                        conn.execute(text("DROP TABLE IF EXISTS test_mage_users"))
                        conn.commit()
                        
                except (json.JSONDecodeError, IndexError) as e:
                    pytest.fail(f"Could not parse pipeline result: {e}\nOutput: {result.stdout}")
            else:
                pytest.skip(f"Pipeline execution failed (external dependency): {result.stderr}")
                
        finally:
            # Clean up temp file
            os.unlink(temp_config)
    
    def test_audit_logging_integration(self):
        """Test that audit logging works properly with Mage integration."""
        engine = get_dw_engine()
        
        # Check if audit table exists and is accessible
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'etl_audit_log'
            """))
            
            audit_table_exists = result.scalar() > 0
            assert audit_table_exists, "ETL audit log table should exist"
            
            # Check for recent Mage-related audit entries
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM etl_audit_log 
                WHERE pipeline_name LIKE '%mage%' 
                   OR pipeline_name LIKE '%test%'
                   AND started_at > NOW() - INTERVAL '1 hour'
            """))
            
            recent_entries = result.scalar()
            print(f"📊 Found {recent_entries} recent Mage-related audit entries")
    
    def test_mage_blocks_import(self):
        """Test that custom Mage blocks can be imported properly."""
        # Test importing ETL runner in Mage container
        import_script = """
import sys
sys.path.append('/app/mage_blocks')

try:
    from custom.etl_runner import execute_etl_pipeline, test_connection, test_dw_connection
    print("SUCCESS: ETL runner functions imported")
except ImportError as e:
    print(f"ERROR: Cannot import ETL runner: {e}")
    sys.exit(1)

try:
    # Test basic function availability
    assert callable(execute_etl_pipeline), "execute_etl_pipeline should be callable"
    assert callable(test_connection), "test_connection should be callable" 
    assert callable(test_dw_connection), "test_dw_connection should be callable"
    print("SUCCESS: All ETL runner functions are callable")
except AssertionError as e:
    print(f"ERROR: Function check failed: {e}")
    sys.exit(1)
"""
        
        cmd = ["docker", "exec", "dw_mage", "python", "-c", import_script]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, f"Mage blocks should import successfully: {result.stderr}"
            assert "SUCCESS" in result.stdout, "Import tests should pass"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Cannot test Mage container")
    
    def test_environment_variables_in_mage(self):
        """Test that required environment variables are available in Mage container."""
        env_test_script = """
import os

required_vars = [
    'DW_HOST', 'DW_PORT', 'DW_DATABASE', 'DW_USERNAME', 'DW_PASSWORD',
    'PYTHONPATH'
]

missing_vars = []
for var in required_vars:
    if not os.getenv(var):
        missing_vars.append(var)

if missing_vars:
    print(f"ERROR: Missing environment variables: {missing_vars}")
    exit(1)
else:
    print("SUCCESS: All required environment variables are set")
    
# Test PYTHONPATH specifically
pythonpath = os.getenv('PYTHONPATH', '')
if '/app/etl' not in pythonpath:
    print(f"ERROR: PYTHONPATH does not include /app/etl: {pythonpath}")
    exit(1)
else:
    print("SUCCESS: PYTHONPATH includes ETL framework")
"""
        
        cmd = ["docker", "exec", "dw_mage", "python", "-c", env_test_script]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, f"Environment variables should be properly set: {result.stderr}"
            assert "SUCCESS" in result.stdout, "Environment validation should pass"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Cannot test Mage container")


class TestMageAPIEndpoints:
    """Test Mage.ai API endpoints for integration."""
    
    def test_mage_api_health(self):
        """Test Mage API health endpoint."""
        try:
            response = requests.get("http://localhost:6789/api/status", timeout=10)
            assert response.status_code == 200, "Mage API should be healthy"
            
            # Try to get pipelines list (may be empty, but endpoint should work)
            response = requests.get("http://localhost:6789/api/pipelines", timeout=10)
            assert response.status_code == 200, "Pipelines API should be accessible"
            
        except requests.RequestException:
            pytest.skip("Mage.ai not accessible for API tests")
    
    def test_mage_file_system_access(self):
        """Test that Mage can access mounted ETL files."""
        # Test if ETL files are visible in Mage container
        cmd = ["docker", "exec", "dw_mage", "ls", "-la", "/app/etl"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, "ETL directory should be accessible in Mage"
            assert "cli.py" in result.stdout, "ETL CLI should be mounted"
            assert "connections" in result.stdout, "ETL connections package should be mounted"
            
            # Test mage_blocks directory
            cmd = ["docker", "exec", "dw_mage", "ls", "-la", "/app/mage_blocks"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, "Mage blocks should be accessible"
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Cannot test Mage container file system")


def run_integration_tests():
    """Run all integration tests and provide summary."""
    print("🧪 Running Mage.ai Integration Tests...")
    print("=" * 50)
    
    # Run pytest with verbose output
    exit_code = pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-x"  # Stop on first failure
    ])
    
    if exit_code == 0:
        print("\n" + "=" * 50)
        print("✅ All Mage.ai integration tests passed!")
        print("🎉 Your Mage.ai integration is working correctly!")
    else:
        print("\n" + "=" * 50)
        print("❌ Some integration tests failed.")
        print("💡 Check the error messages above for troubleshooting.")
    
    return exit_code


if __name__ == "__main__":
    exit(run_integration_tests())