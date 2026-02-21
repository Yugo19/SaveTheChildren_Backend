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
                    # Convert row to document (parquet source)
                    doc = self._convert_row_to_document(row, source='parquet_import')
                    
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
                    # Convert row to document (CSV source)
                    doc = self._convert_row_to_document(row, source='csv_import')
                    
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
    
    def _convert_row_to_document(self, row, source: str = 'csv_import') -> Dict:
        """
        Convert a DataFrame row to a MongoDB document
        Handles date normalization and field name mapping for API compatibility
        Ensures case_id is always stored as string for consistency
        
        Args:
            row: DataFrame row to convert
            source: Source of the data (e.g., 'csv_import', 'parquet_import')
        """
        import pandas as pd
        from datetime import datetime as dt
        import re
        
        # Field name mapping from CSV columns to API field names
        field_mapping = {
            'Case Date': 'case_date',
            'Sex': 'child_sex',
            'Age Range': 'age_range',
            'Case Category': 'abuse_type',
            'No. of Cases': 'no_of_cases',
            'County': 'county',
            'Sub County': 'subcounty',
            'Intervention': 'intervention',
            'Year': 'year',
            'Month': 'month',
            'MonthName': 'month_name',
            '#': 'case_id'
        }
        
        doc = {}
        
        # Date fields that need normalization
        date_fields = ['case_date', 'Case Date', 'Date', 'incident_date', 'report_date', 'date_reported']
        
        # Convert all fields from the row
        for column in row.index:
            value = row[column]
            
            # Handle NaN/None values
            if pd.isna(value):
                continue
            
            # Map column name to API field name
            target_field = field_mapping.get(column, column.lower().replace(' ', '_'))
            
            # Handle datetime columns
            if pd.api.types.is_datetime64_any_dtype(type(value)):
                # Convert pandas datetime to Python datetime
                py_datetime = pd.to_datetime(value).to_pydatetime()
                # Store as normalized string for consistent filtering
                doc[target_field] = py_datetime.strftime('%Y-%m-%d %H:%M:%S')
            # Handle date fields that come as strings (from CSV)
            elif column in date_fields or column.lower() in [f.lower() for f in date_fields]:
                try:
                    # Try to parse various date formats
                    date_str = str(value).strip()
                    
                    # Try common formats
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', 
                               '%Y-%m-%d %H:%M:%S', '%d-%m-%Y', '%m-%d-%Y']:
                        try:
                            parsed_date = dt.strptime(date_str, fmt)
                            # Normalize to consistent format
                            doc[target_field] = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                            break
                        except ValueError:
                            continue
                    else:
                        # If parsing failed, store as-is but log warning
                        logger.warning(f"Could not parse date field {column}: {value}")
                        doc[target_field] = str(value)
                except Exception as e:
                    logger.warning(f"Error processing date field {column}: {e}")
                    doc[target_field] = str(value)
            # Handle numeric types
            elif isinstance(value, (int, float)):
                # Special handling for case_id - always convert to string
                if target_field == 'case_id':
                    doc[target_field] = str(int(value))
                elif isinstance(value, float) and value.is_integer():
                    doc[target_field] = int(value)
                else:
                    doc[target_field] = value
            # Handle strings
            else:
                # Ensure case_id is always string type
                if target_field == 'case_id':
                    doc[target_field] = str(value).strip()
                else:
                    doc[target_field] = str(value)
        
        # Convert age_range to child_age (integer)
        if 'age_range' in doc:
            age_range_str = doc['age_range']
            doc['child_age'] = self._parse_age_range(age_range_str)
            # Keep age_range for reference but child_age is the primary field
        
        # Add metadata fields
        doc['source'] = source
        doc['created_at'] = datetime.now(timezone.utc)
        doc['updated_at'] = datetime.now(timezone.utc)
        
        # Set default status if not present
        if 'status' not in doc:
            doc['status'] = 'open'
        
        # Final validation: Ensure case_id is string if it exists
        if 'case_id' in doc and not isinstance(doc['case_id'], str):
            doc['case_id'] = str(doc['case_id'])
        
        return doc
    
    def _parse_age_range(self, age_range_str: str) -> Optional[int]:
        """
        Parse age range string to approximate integer age
        Examples: '0 - 5 yrs' -> 3, '6 - 11 yrs' -> 9, '16 - 18 yrs' -> 17, '18+ yrs' -> 18
        """
        import re
        
        # Extract numbers from string like "16 - 18 yrs" or "18+ yrs"
        numbers = re.findall(r'\d+', str(age_range_str))
        
        if not numbers:
            return None
        
        # Convert to integers
        ages = [int(n) for n in numbers]
        
        if len(ages) == 1:
            # Single age or "18+" -> use that age (cap at 18)
            return min(ages[0], 18)
        else:
            # Range like "16 - 18" -> use midpoint
            return (ages[0] + ages[1]) // 2
    
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
