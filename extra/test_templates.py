#!/usr/bin/env python3
"""
Test template rendering
"""

from flask import Flask, render_template
import os

app = Flask(__name__)

def test_templates():
    """Test if all templates can be rendered"""
    templates = [
        'main_dashboard.html',
        'parquet_dashboard.html', 
        'duckdb_dashboard.html',
        'etl_dashboard.html',
        'snowflake_dashboard.html'
    ]
    
    print("Testing template rendering...")
    print("=" * 50)
    
    with app.app_context():
        for template in templates:
            try:
                # Check if template file exists
                template_path = os.path.join('templates', template)
                if not os.path.exists(template_path):
                    print(f"❌ {template}: File not found")
                    continue
                
                # Try to render template
                content = render_template(template)
                print(f"✅ {template}: OK ({len(content)} chars)")
                
            except Exception as e:
                print(f"❌ {template}: ERROR - {e}")
    
    print("=" * 50)

if __name__ == "__main__":
    test_templates()