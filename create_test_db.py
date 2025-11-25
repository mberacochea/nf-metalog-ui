"""Create a sample test database for testing the TUI."""
import sqlite3
from datetime import datetime, timedelta
import json

# Create test database
conn = sqlite3.connect('test_metalog.db')

# Create table
conn.execute("""
    CREATE TABLE IF NOT EXISTS metalog (
        run_name TEXT,
        id TEXT,
        ingested TEXT,
        process TEXT,
        task_id TEXT,
        status TEXT,
        metadata TEXT
    )
""")

# Generate sample data
base_time = datetime.now()

# Run 1: Recent run with mixed statuses
for i in range(5):
    for j in range(3):
        conn.execute("""
            INSERT INTO metalog VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            'run_2025_01_01',
            f'workflow_{i}',
            (base_time - timedelta(minutes=i*10 + j)).isoformat(),
            f'process_{j}',
            f'task_{i}_{j}',
            ['COMPLETED', 'FAILED', 'RUNNING'][j % 3],
            json.dumps({'cpu': j * 2, 'memory': f'{j}GB', 'duration': f'{i*10}s'})
        ))

# Run 2: Older run, all completed
for i in range(3):
    for j in range(2):
        conn.execute("""
            INSERT INTO metalog VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            'run_2024_12_25',
            f'workflow_{i}',
            (base_time - timedelta(days=7, minutes=i*10 + j)).isoformat(),
            f'process_{j}',
            f'task_{i}_{j}',
            'COMPLETED',
            json.dumps({'cpu': 4, 'memory': '8GB', 'duration': '120s'})
        ))

# Run 3: Very recent with failures
for i in range(2):
    for j in range(4):
        conn.execute("""
            INSERT INTO metalog VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            'run_2025_01_15',
            f'sample_{i}',
            (base_time - timedelta(hours=1, minutes=i*5 + j)).isoformat(),
            f'analysis_{j}',
            f'task_analysis_{i}_{j}',
            'FAILED' if j == 2 else 'COMPLETED',
            json.dumps({'error': 'Out of memory' if j == 2 else None, 'retry_count': j})
        ))

conn.commit()
conn.close()
print("Test database created: test_metalog.db")
print("Run: uv run metalog-ui test_metalog.db")
