#!/usr/bin/env python3
"""
Quick script to inspect parquet file structure
Helps you understand what data will be imported
"""
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
    import pandas as pd
except ImportError:
    print("Error: pyarrow and pandas required")
    print("Install with: pip install pyarrow pandas")
    sys.exit(1)

def inspect_parquet(file_path: str):
    """Inspect a parquet file and show its structure"""
    print(f"\n{'='*60}")
    print(f"Inspecting: {file_path}")
    print(f"{'='*60}\n")
    
    # Read the file
    table = pq.read_table(file_path)
    df = table.to_pandas()
    
    # Basic info
    print(f"Total rows: {len(df):,}")
    print(f"Total columns: {len(df.columns)}\n")
    
    # Column information
    print("Columns and data types:")
    print("-" * 60)
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].count()
        null_count = len(df) - non_null
        sample = df[col].dropna().iloc[0] if non_null > 0 else "N/A"
        
        print(f"{col:20} | {str(dtype):15} | Non-null: {non_null:6,} | Sample: {sample}")
    
    # Sample data
    print(f"\n{'='*60}")
    print("First 3 rows:")
    print(f"{'='*60}")
    print(df.head(3).to_string())
    
    # Statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print(f"\n{'='*60}")
        print("Numeric column statistics:")
        print(f"{'='*60}")
        print(df[numeric_cols].describe())
    
    # Value counts for categorical columns
    categorical_cols = ['county', 'abuse_type', 'severity', 'status', 'child_sex']
    available_categorical = [col for col in categorical_cols if col in df.columns]
    
    if available_categorical:
        print(f"\n{'='*60}")
        print("Categorical value counts:")
        print(f"{'='*60}")
        for col in available_categorical[:3]:  # Show first 3
            print(f"\n{col}:")
            print(df[col].value_counts().head(10))


if __name__ == "__main__":
    data_dir = Path(__file__).parent / "data"
    
    # List available files
    parquet_files = sorted(data_dir.glob("*.parquet"))
    
    if not parquet_files:
        print("No parquet files found in data/ directory")
        sys.exit(1)
    
    print("Available parquet files:")
    for i, f in enumerate(parquet_files, 1):
        size_kb = f.stat().st_size / 1024
        print(f"  {i}. {f.name} ({size_kb:.1f} KB)")
    
    # Get user choice
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        file_path = data_dir / file_name
    else:
        choice = input("\nEnter file number to inspect (or 0 to exit): ").strip()
        
        if choice == "0":
            sys.exit(0)
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(parquet_files):
                file_path = parquet_files[idx]
            else:
                print("Invalid choice")
                sys.exit(1)
        except ValueError:
            print("Invalid input")
            sys.exit(1)
    
    # Inspect the file
    if file_path.exists():
        inspect_parquet(str(file_path))
    else:
        print(f"File not found: {file_path}")
        sys.exit(1)
