"""Test connection and dependencies for Spain Power Grid Analysis"""
import sys
import importlib
import requests
from datetime import datetime

def test_imports():
    """Test if all required libraries are installed"""
    print("Testing Python package imports...")
    required_packages = ['requests', 'pandas', 'json', 'datetime', 'openpyxl']
    
    for package in required_packages:
        try:
            if package == 'json' or package == 'datetime':
                __import__(package)
            else:
                importlib.import_module(package)
            print(f"✓ {package} - OK")
        except ImportError:
            print(f"✗ {package} - MISSING (run: pip install -r requirements.txt)")
            return False
    return True

def test_internet():
    """Test internet connectivity"""
    print("\nTesting internet connection...")
    try:
        response = requests.get("http://www.google.com", timeout=5)
        print("✓ Internet connection - OK")
        return True
    except:
        print("✗ Internet connection - FAILED")
        return False

def test_osm_api():
    """Test OpenStreetMap Overpass API access"""
    print("\nTesting OpenStreetMap API...")
    try:
        # Simple test query for a small area
        test_query = '[out:json][timeout:10];node["power"="plant"](40.4,-3.71,40.41,-3.70);out count;'
        response = requests.post(
            "http://overpass-api.de/api/interpreter",
            data=test_query,
            timeout=15
        )
        if response.status_code == 200:
            print("✓ OpenStreetMap API - OK")
            return True
        else:
            print(f"✗ OpenStreetMap API - HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ OpenStreetMap API - FAILED: {str(e)}")
        return False

def test_file_creation():
    """Test if we can create files in outputs folder"""
    print("\nTesting file creation permissions...")
    try:
        test_file = "../outputs/test_file.txt"
        with open(test_file, 'w') as f:
            f.write("Test successful")
        import os
        os.remove(test_file)
        print("✓ File creation - OK")
        return True
    except Exception as e:
        print(f"✗ File creation - FAILED: {str(e)}")
        return False

def main():
    print("=" * 60)
    print("Spain Power Grid Analysis - System Test")
    print("=" * 60)
    print(f"Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    tests = [
        test_imports(),
        test_internet(),
        test_osm_api(),
        test_file_creation()
    ]
    
    print("\n" + "=" * 60)
    if all(tests):
        print("✓ ALL TESTS PASSED - System ready for analysis!")
    else:
        print("✗ Some tests failed - Please fix issues before running analysis")
    print("=" * 60)

if __name__ == "__main__":
    main()