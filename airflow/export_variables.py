#!/usr/bin/env python3
"""
Script để export Airflow Variables ra JSON file
Usage: python export_variables.py [output-file.json]
"""

import json
import sys
import os

# Add Airflow to path
sys.path.insert(0, '/opt/airflow')

from airflow.models import Variable

def export_variables(output_file='airflow-variables-exported.json'):
    """Export tất cả Airflow Variables ra JSON file"""
    
    try:
        # Lấy tất cả variables
        all_vars = Variable.get_all()
        
        variables = []
        for key in all_vars:
            try:
                value = Variable.get(key, deserialize_json=False)
                # Lấy description nếu có
                description = ''
                try:
                    # Airflow không có API để lấy description, để trống
                    pass
                except:
                    pass
                
                variables.append({
                    'key': key,
                    'value': value,
                    'description': description
                })
            except Exception as e:
                print(f"⚠️  Warning: Could not export {key}: {e}")
        
        # Tạo JSON structure
        data = {
            'variables': variables
        }
        
        # Ghi vào file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Exported {len(variables)} variables to {output_file}")
        
        return variables
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    output_file = sys.argv[1] if len(sys.argv) > 1 else 'airflow-variables-exported.json'
    export_variables(output_file)
