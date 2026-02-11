#!/usr/bin/env python3
"""
Standalone script to load parquet/CSV data into MongoDB
Can be run independently without starting the API server
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from motor.motor_asyncio import AsyncIOMotorClient
from app.services.data_loader_service import DataLoaderService
from app.config import settings
from app.core.logging import logger


async def main():
    """Main function to load data"""
    print("=" * 60)
    print("Save The Children - Data Loader")
    print("=" * 60)
    
    # Connect to MongoDB
    print(f"\nConnecting to MongoDB: {settings.DB_NAME}...")
    client = AsyncIOMotorClient(settings.DB_URI)
    db = client[settings.DB_NAME]
    
    try:
        # Test connection
        await client.admin.command('ping')
        print("✓ Connected to MongoDB")
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        return
    
    # Create data loader service
    loader = DataLoaderService(db)
    
    # List available files
    print(f"\nData directory: {loader.data_dir}")
    parquet_files = sorted(loader.data_dir.glob("*.parquet"))
    csv_files = sorted(loader.data_dir.glob("*.csv"))
    
    print(f"\nAvailable files:")
    print(f"  Parquet files: {len(parquet_files)}")
    for f in parquet_files:
        print(f"    - {f.name}")
    print(f"  CSV files: {len(csv_files)}")
    for f in csv_files:
        print(f"    - {f.name}")
    
    # Check current database state
    current_count = await db.cases.count_documents({})
    print(f"\nCurrent cases in database: {current_count}")
    
    # Ask user what to do
    print("\nOptions:")
    print("  1. Load all parquet files")
    print("  2. Load specific parquet file")
    print("  3. Load all CSV files")
    print("  4. Load specific CSV file")
    print("  5. View import statistics")
    print("  6. Clear all cases (WARNING: destructive)")
    print("  0. Exit")
    
    choice = input("\nEnter choice (0-6): ").strip()
    
    if choice == "1":
        # Load all parquet files
        confirm = input("\nLoad all parquet files? This may take time. (y/n): ").strip().lower()
        if confirm == 'y':
            skip_duplicates = input("Skip duplicate case_ids? (y/n, default=y): ").strip().lower()
            skip_duplicates = skip_duplicates != 'n'
            
            print("\nLoading parquet files...")
            results = await loader.load_all_parquet_files(
                pattern="*.parquet",
                batch_size=1000,
                skip_duplicates=skip_duplicates
            )
            
            print("\n" + "=" * 60)
            print("Import Summary")
            print("=" * 60)
            print(f"Files processed: {results['files_processed']}")
            print(f"Total records: {results['total_records']}")
            print(f"Inserted: {results['total_inserted']}")
            print(f"Skipped: {results['total_skipped']}")
            print(f"Errors: {results['total_errors']}")
            print("\nDetails per file:")
            for file_stats in results['files']:
                print(f"\n  {Path(file_stats['file']).name}:")
                print(f"    Records: {file_stats['total_records']}")
                print(f"    Inserted: {file_stats['inserted']}")
                print(f"    Skipped: {file_stats['skipped']}")
                print(f"    Errors: {file_stats['errors']}")
    
    elif choice == "2":
        # Load specific parquet file
        print("\nAvailable parquet files:")
        for i, f in enumerate(parquet_files, 1):
            print(f"  {i}. {f.name}")
        
        file_idx = input("\nEnter file number: ").strip()
        try:
            file_idx = int(file_idx) - 1
            if 0 <= file_idx < len(parquet_files):
                file_path = parquet_files[file_idx]
                
                skip_duplicates = input("Skip duplicate case_ids? (y/n, default=y): ").strip().lower()
                skip_duplicates = skip_duplicates != 'n'
                
                print(f"\nLoading {file_path.name}...")
                results = await loader.load_parquet_file(
                    str(file_path),
                    batch_size=1000,
                    skip_duplicates=skip_duplicates
                )
                
                print("\n" + "=" * 60)
                print("Import Summary")
                print("=" * 60)
                print(f"File: {results['file']}")
                print(f"Total records: {results['total_records']}")
                print(f"Inserted: {results['inserted']}")
                print(f"Skipped: {results['skipped']}")
                print(f"Errors: {results['errors']}")
            else:
                print("Invalid file number")
        except ValueError:
            print("Invalid input")
    
    elif choice == "3":
        # Load all CSV files
        if not csv_files:
            print("\nNo CSV files found")
        else:
            confirm = input(f"\nLoad all {len(csv_files)} CSV files? (y/n): ").strip().lower()
            if confirm == 'y':
                skip_duplicates = input("Skip duplicate case_ids? (y/n, default=y): ").strip().lower()
                skip_duplicates = skip_duplicates != 'n'
                
                print("\nLoading CSV files...")
                total_inserted = 0
                total_skipped = 0
                total_errors = 0
                
                for csv_file in csv_files:
                    print(f"\nLoading {csv_file.name}...")
                    results = await loader.load_csv_file(
                        str(csv_file),
                        batch_size=1000,
                        skip_duplicates=skip_duplicates
                    )
                    total_inserted += results['inserted']
                    total_skipped += results['skipped']
                    total_errors += results['errors']
                
                print("\n" + "=" * 60)
                print("Import Summary")
                print("=" * 60)
                print(f"Files processed: {len(csv_files)}")
                print(f"Total inserted: {total_inserted}")
                print(f"Total skipped: {total_skipped}")
                print(f"Total errors: {total_errors}")
    
    elif choice == "4":
        # Load specific CSV file
        if not csv_files:
            print("\nNo CSV files found")
        else:
            print("\nAvailable CSV files:")
            for i, f in enumerate(csv_files, 1):
                print(f"  {i}. {f.name}")
            
            file_idx = input("\nEnter file number: ").strip()
            try:
                file_idx = int(file_idx) - 1
                if 0 <= file_idx < len(csv_files):
                    file_path = csv_files[file_idx]
                    
                    skip_duplicates = input("Skip duplicate case_ids? (y/n, default=y): ").strip().lower()
                    skip_duplicates = skip_duplicates != 'n'
                    
                    print(f"\nLoading {file_path.name}...")
                    results = await loader.load_csv_file(
                        str(file_path),
                        batch_size=1000,
                        skip_duplicates=skip_duplicates
                    )
                    
                    print("\n" + "=" * 60)
                    print("Import Summary")
                    print("=" * 60)
                    print(f"File: {results['file']}")
                    print(f"Total records: {results['total_records']}")
                    print(f"Inserted: {results['inserted']}")
                    print(f"Skipped: {results['skipped']}")
                    print(f"Errors: {results['errors']}")
                else:
                    print("Invalid file number")
            except ValueError:
                print("Invalid input")
    
    elif choice == "5":
        # View statistics
        print("\nFetching import statistics...")
        stats = await loader.get_import_statistics()
        
        print("\n" + "=" * 60)
        print("Import Statistics")
        print("=" * 60)
        print(f"Total cases in database: {stats['total_cases']}")
        
        print("\nBy source:")
        for item in stats['by_source']:
            source = item['_id'] or 'unknown'
            print(f"  {source}: {item['count']}")
        
        print("\nBy year (created_at):")
        for item in stats['by_year']:
            year = item['_id'] or 'unknown'
            print(f"  {year}: {item['count']}")
    
    elif choice == "6":
        # Clear collection
        print("\n" + "!" * 60)
        print("WARNING: This will DELETE ALL cases from the database!")
        print("!" * 60)
        confirm1 = input("\nType 'DELETE' to confirm: ").strip()
        if confirm1 == "DELETE":
            confirm2 = input("Are you absolutely sure? (yes/no): ").strip().lower()
            if confirm2 == "yes":
                print("\nClearing cases collection...")
                results = await loader.clear_collection(confirm=True)
                print(f"\n✓ Deleted {results['deleted_count']} cases")
            else:
                print("\nCancelled")
        else:
            print("\nCancelled")
    
    elif choice == "0":
        print("\nExiting...")
    
    else:
        print("\nInvalid choice")
    
    # Close connection
    client.close()
    print("\n✓ Disconnected from MongoDB")
    print("\nDone!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"\n✗ Error: {e}")
        sys.exit(1)
