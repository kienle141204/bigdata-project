#!/usr/bin/env python3
"""
Script để import Airflow Variables từ JSON file
Usage: python import_variables.py [variables-file.json]
"""

import json
import sys
import os

# Add Airflow to path
sys.path.insert(0, '/opt/airflow')

from airflow.models import Variable

def import_variables(json_file='airflow-variables.json'):
    """Import variables từ JSON file vào Airflow"""
    
    if not os.path.exists(json_file):
        print(f"❌ Error: File {json_file} not found!")
        print(f"Usage: python {sys.argv[0]} [variables-file.json]")
        sys.exit(1)
    
    # Đọc JSON file
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON format: {e}")
        sys.exit(1)
    
    # Hỗ trợ cả 2 format:
    # Format 1: {"key1": "value1", "key2": "value2"} - Airflow UI format
    # Format 2: {"variables": [{"key": "...", "value": "...", ...}]} - Custom format
    
    variables = []
    if isinstance(data, dict):
        if 'variables' in data:
            # Format với array
            variables = data.get('variables', [])
        else:
            # Format đơn giản key-value pairs
            variables = [{"key": k, "value": v} for k, v in data.items()]
    
    if not variables:
        print("⚠️  Warning: No variables found in JSON file")
        sys.exit(0)
    
    print(f"📋 Found {len(variables)} variables to import\n")
    
    success_count = 0
    fail_count = 0
    
    # Import từng variable
    for var in variables:
        if isinstance(var, dict):
            key = var.get('key')
            value = var.get('value')
            description = var.get('description', '')
        else:
            # Nếu var là string (key), lấy value từ data
            key = var
            value = data.get(var)
            description = ''
        
        if not key or value is None:
            print(f"⚠️  Skipping invalid variable: {var}")
            fail_count += 1
            continue
        
        try:
            # Set variable
            Variable.set(key, str(value), description=description)
            print(f"✅ {key} = {value}")
            success_count += 1
        except Exception as e:
            print(f"❌ Failed to set {key}: {e}")
            fail_count += 1
    
    print(f"\n📊 Summary:")
    print(f"   Success: {success_count}")
    print(f"   Failed: {fail_count}")
    
    if success_count > 0:
        print(f"\n✅ Variables imported successfully!")
        print(f"💡 View in Airflow UI: Admin -> Variables")

if __name__ == '__main__':
    json_file = sys.argv[1] if len(sys.argv) > 1 else 'airflow-variables.json'
    import_variables(json_file)
