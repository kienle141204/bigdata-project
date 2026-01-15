#!/usr/bin/env python3
"""
Script tự động import Airflow Variables từ JSON file
Được gọi tự động khi Airflow khởi động
"""

import json
import sys
import os

# Add Airflow to path
sys.path.insert(0, '/opt/airflow')

try:
    from airflow.models import Variable
except ImportError:
    print("ERROR: Airflow not available. Make sure this runs inside Airflow container.")
    sys.exit(1)

def import_variables(json_file='/tmp/airflow-variables.json'):
    """Import variables từ JSON file vào Airflow"""
    
    if not os.path.exists(json_file):
        print(f"INFO: File {json_file} not found. Skipping import.")
        return 0
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON format: {e}")
        return 1
    except Exception as e:
        print(f"ERROR: Failed to read file: {e}")
        return 1
    
    # Hỗ trợ cả 2 format JSON
    variables = {}
    if isinstance(data, dict):
        if 'variables' in data:
            # Format với array: {"variables": [{"key": "...", "value": "..."}]}
            for var in data.get('variables', []):
                if isinstance(var, dict) and 'key' in var:
                    variables[var['key']] = var.get('value', '')
        else:
            # Format đơn giản: {"key1": "value1", "key2": "value2"}
            variables = data
    
    if not variables:
        print("WARNING: No variables found in JSON file")
        return 0
    
    print(f"INFO: Found {len(variables)} variables to import")
    
    success_count = 0
    fail_count = 0
    
    # Import từng variable
    for key, value in variables.items():
        if not key:
            continue
        
        try:
            # Set variable (convert value to string)
            Variable.set(key, str(value))
            print(f"OK: {key}")
            success_count += 1
        except Exception as e:
            print(f"ERROR: {key} - {e}")
            fail_count += 1
    
    print(f"SUCCESS: Imported {success_count} variables")
    if fail_count > 0:
        print(f"WARNING: {fail_count} variables failed to import")
    
    return 0 if fail_count == 0 else 1

if __name__ == '__main__':
    json_file = sys.argv[1] if len(sys.argv) > 1 else '/tmp/airflow-variables.json'
    sys.exit(import_variables(json_file))
