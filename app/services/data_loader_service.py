from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timezone
from typing import Optional, List, Dict
from app.core.logging import logger
from pathlib import Path
import asyncio


class DataLoaderService:
    """Service to load case data from parquet files into MongoDB"""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self.data_dir = Path(__file__).parent.parent.parent / "data"
        
    async def load_parquet_file(
        self, 
        file_path: str, 
        batch_size: int = 1000,
        skip_duplicates: bool = True
    ) -> Dict:
        """
        Load a single parquet file into MongoDB cases collection
        
        Args:
            file_path: Path to the parquet file
            batch_size: Number of records to insert at once
            skip_duplicates: Whether to skip records with duplicate case_ids
            
        Returns:
            Dict with statistics about the import
        """
        try:
            import pyarrow.parquet as pq
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pyarrow and pandas are required. Install with: pip install pyarrow pandas"
            )
        
        logger.info(f"Loading parquet file: {file_path}")
        
        # Read parquet file
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        total_records = len(df)
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0
        
        # Process in batches
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_docs = []
            
            for _, row in batch.iterrows():
                try:
                    # Convert row to document
                    doc = self._convert_row_to_document(row)
                    
                    if skip_duplicates and doc.get('case_id'):
                        # Check if case_id already exists
                        existing = await self.cases_collection.find_one(
                            {"case_id": doc['case_id']}
                        )
                        if existing:
                            skipped_count += 1
                            continue
                    
                    batch_docs.append(doc)
                    
                except Exception as e:
                    logger.error(f"Error converting row to document: {e}")
                    error_count += 1
                    continue
            
            # Insert batch
            if batch_docs:
                try:
                    result = await self.cases_collection.insert_many(
                        batch_docs, 
                        ordered=False
                    )
                    inserted_count += len(result.inserted_ids)
                except Exception as e:
                    logger.error(f"Error inserting batch: {e}")
                    error_count += len(batch_docs)
            
            logger.info(
                f"Progress: {min(i + batch_size, total_records)}/{total_records} records processed"
            )
        
        stats = {
            "file": str(file_path),
            "total_records": total_records,
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "errors": error_count,
            "timestamp": datetime.now(timezone.utc)
        }
        
        logger.info(f"Load complete: {stats}")
        return stats
    
    async def load_all_parquet_files(
        self, 
        pattern: str = "*.parquet",
        batch_size: int = 1000,
        skip_duplicates: bool = True
    ) -> List[Dict]:
        """
        Load all parquet files matching pattern from data directory
        
        Args:
            pattern: File pattern to match (e.g., "*.parquet" or "2024.parquet")
            batch_size: Number of records to insert at once
            skip_duplicates: Whether to skip records with duplicate case_ids
            
        Returns:
            List of statistics for each file loaded
        """
        logger.info(f"Loading all parquet files from: {self.data_dir}")
        
        parquet_files = sorted(self.data_dir.glob(pattern))
        
        if not parquet_files:
            logger.warning(f"No parquet files found matching pattern: {pattern}")
            return []
        
        results = []
        for file_path in parquet_files:
            stats = await self.load_parquet_file(
                str(file_path),
                batch_size=batch_size,
                skip_duplicates=skip_duplicates
            )
            results.append(stats)
        
        # Summary
        total_stats = {
            "files_processed": len(results),
            "total_records": sum(r['total_records'] for r in results),
            "total_inserted": sum(r['inserted'] for r in results),
            "total_skipped": sum(r['skipped'] for r in results),
            "total_errors": sum(r['errors'] for r in results),
            "files": results
        }
        
        logger.info(f"All files loaded: {total_stats}")
        return total_stats
    
    async def load_csv_file(
        self,
        file_path: str,
        batch_size: int = 1000,
        skip_duplicates: bool = True
    ) -> Dict:
        """
        Load a CSV file into MongoDB cases collection
        
        Args:
            file_path: Path to the CSV file
            batch_size: Number of records to insert at once
            skip_duplicates: Whether to skip records with duplicate case_ids
            
        Returns:
            Dict with statistics about the import
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required. Install with: pip install pandas"
            )
        
        logger.info(f"Loading CSV file: {file_path}")
        
        # Read CSV file
        df = pd.read_csv(file_path)
        
        total_records = len(df)
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0
        
        # Process in batches
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_docs = []
            
            for _, row in batch.iterrows():
                try:
                    # Convert row to document
                    doc = self._convert_row_to_document(row)
                    
                    if skip_duplicates and doc.get('case_id'):
                        # Check if case_id already exists
                        existing = await self.cases_collection.find_one(
                            {"case_id": doc['case_id']}
                        )
                        if existing:
                            skipped_count += 1
                            continue
                    
                    batch_docs.append(doc)
                    
                except Exception as e:
                    logger.error(f"Error converting row to document: {e}")
                    error_count += 1
                    continue
            
            # Insert batch
            if batch_docs:
                try:
                    result = await self.cases_collection.insert_many(
                        batch_docs,
                        ordered=False
                    )
                    inserted_count += len(result.inserted_ids)
                except Exception as e:
                    logger.error(f"Error inserting batch: {e}")
                    error_count += len(batch_docs)
            
            logger.info(
                f"Progress: {min(i + batch_size, total_records)}/{total_records} records processed"
            )
        
        stats = {
            "file": str(file_path),
            "total_records": total_records,
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "errors": error_count,
            "timestamp": datetime.now(timezone.utc)
        }
        
        logger.info(f"Load complete: {stats}")
        return stats
    
    def _convert_row_to_document(self, row) -> Dict:
        """
        Convert a DataFrame row to a MongoDB document
        Customize this method based on your parquet file structure
        """
        import pandas as pd
        
        doc = {}
        
        # Convert all fields from the row
        for column in row.index:
            value = row[column]
            
            # Handle NaN/None values
            if pd.isna(value):
                continue
            
            # Handle datetime columns
            if pd.api.types.is_datetime64_any_dtype(type(value)):
                doc[column] = pd.to_datetime(value).to_pydatetime()
            # Handle numeric types
            elif isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    doc[column] = int(value)
                else:
                    doc[column] = value
            # Handle strings
            else:
                doc[column] = str(value)
        
        # Add metadata fields
        doc['source'] = 'parquet_import'
        doc['created_at'] = datetime.now(timezone.utc)
        doc['updated_at'] = datetime.now(timezone.utc)
        
        # Set default status if not present
        if 'status' not in doc:
            doc['status'] = 'open'
        
        return doc
    
    async def clear_collection(self, confirm: bool = False) -> Dict:
        """
        Clear all data from cases collection
        
        Args:
            confirm: Must be True to actually delete data
            
        Returns:
            Dict with deletion stats
        """
        if not confirm:
            raise ValueError(
                "Must confirm deletion by setting confirm=True"
            )
        
        logger.warning("Clearing cases collection...")
        result = await self.cases_collection.delete_many({})
        
        stats = {
            "deleted_count": result.deleted_count,
            "timestamp": datetime.now(timezone.utc)
        }
        
        logger.info(f"Collection cleared: {stats}")
        return stats
    
    async def get_import_statistics(self) -> Dict:
        """Get statistics about imported data"""
        pipeline = [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "by_source": [
                        {"$group": {"_id": "$source", "count": {"$sum": 1}}}
                    ],
                    "by_year": [
                        {
                            "$group": {
                                "_id": {"$year": "$created_at"},
                                "count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"_id": 1}}
                    ]
                }
            }
        ]
        
        results = await self.cases_collection.aggregate(pipeline).to_list(1)
        
        if not results:
            return {
                "total_cases": 0,
                "by_source": [],
                "by_year": []
            }
        
        data = results[0]
        return {
            "total_cases": data["total"][0]["count"] if data["total"] else 0,
            "by_source": data["by_source"],
            "by_year": data["by_year"]
        }
