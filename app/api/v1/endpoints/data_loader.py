from fastapi import APIRouter, Depends, Query, HTTPException, status, UploadFile, File, Form
from typing import Optional
from app.db.client import get_database
from app.core.security import get_current_user, TokenData
from app.services.data_loader_service import DataLoaderService
from app.core.logging import logger
from pydantic import BaseModel
import tempfile
import shutil
from pathlib import Path


router = APIRouter(prefix="/data-loader", tags=["Data Loader"])


class LoadParquetRequest(BaseModel):
    file_pattern: str = "*.parquet"
    batch_size: int = 1000
    skip_duplicates: bool = True


class LoadSingleFileRequest(BaseModel):
    file_name: str
    batch_size: int = 1000
    skip_duplicates: bool = True


@router.post("/upload/parquet")
async def upload_parquet_file(
    file: UploadFile = File(..., description="Parquet file to upload and import"),
    batch_size: int = Form(1000),
    skip_duplicates: bool = Form(True),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Upload and import a parquet file into cases collection
    Requires admin role
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can import data"
        )
    
    # Validate file extension
    if not file.filename.endswith('.parquet'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a .parquet file"
        )
    
    # Create temporary file to store upload
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            shutil.copyfileobj(file.file, temp_file)
            temp_file_path = temp_file.name
        
        # Load the file
        loader = DataLoaderService(db)
        results = await loader.load_parquet_file(
            temp_file_path,
            batch_size=batch_size,
            skip_duplicates=skip_duplicates
        )
        
        logger.info(f"Parquet file '{file.filename}' imported by {current_user.user_id}")
        return {
            "status": "success",
            "message": f"Parquet file '{file.filename}' imported successfully",
            "results": results
        }
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing required dependencies: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error uploading parquet file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading file: {str(e)}"
        )
    finally:
        # Clean up temporary file
        if temp_file and Path(temp_file_path).exists():
            try:
                Path(temp_file_path).unlink()
            except Exception as e:
                logger.warning(f"Failed to delete temporary file: {e}")


@router.post("/upload/csv")
async def upload_csv_file(
    file: UploadFile = File(..., description="CSV file to upload and import"),
    batch_size: int = Form(1000),
    skip_duplicates: bool = Form(True),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Upload and import a CSV file into cases collection
    Requires admin role
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can import data"
        )
    
    # Validate file extension
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a .csv file"
        )
    
    # Create temporary file to store upload
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            shutil.copyfileobj(file.file, temp_file)
            temp_file_path = temp_file.name
        
        # Load the file
        loader = DataLoaderService(db)
        results = await loader.load_csv_file(
            temp_file_path,
            batch_size=batch_size,
            skip_duplicates=skip_duplicates
        )
        
        logger.info(f"CSV file '{file.filename}' imported by {current_user.user_id}")
        return {
            "status": "success",
            "message": f"CSV file '{file.filename}' imported successfully",
            "results": results
        }
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing required dependencies: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error uploading CSV file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading file: {str(e)}"
        )
    finally:
        # Clean up temporary file
        if temp_file and Path(temp_file_path).exists():
            try:
                Path(temp_file_path).unlink()
            except Exception as e:
                logger.warning(f"Failed to delete temporary file: {e}")


@router.post("/load/parquet")
async def load_parquet_files(
    request: LoadParquetRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Load all parquet files from data directory into cases collection
    Requires admin role
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can import data"
        )
    
    try:
        loader = DataLoaderService(db)
        results = await loader.load_all_parquet_files(
            pattern=request.file_pattern,
            batch_size=request.batch_size,
            skip_duplicates=request.skip_duplicates
        )
        
        logger.info(f"Data import completed by {current_user.user_id}")
        return {
            "status": "success",
            "message": "Data import completed",
            "results": results
        }
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing required dependencies: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error loading parquet files: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading data: {str(e)}"
        )


@router.post("/load/csv")
async def load_csv_file(
    request: LoadSingleFileRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Load a specific CSV file from data directory into cases collection
    Requires admin role
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can import data"
        )
    
    try:
        loader = DataLoaderService(db)
        file_path = loader.data_dir / request.file_name
        
        if not file_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found: {request.file_name}"
            )
        
        results = await loader.load_csv_file(
            str(file_path),
            batch_size=request.batch_size,
            skip_duplicates=request.skip_duplicates
        )
        
        logger.info(f"CSV import completed by {current_user.user_id}")
        return {
            "status": "success",
            "message": "CSV import completed",
            "results": results
        }
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing required dependencies: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading data: {str(e)}"
        )


@router.post("/load/single-parquet")
async def load_single_parquet_file(
    request: LoadSingleFileRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Load a specific parquet file from data directory into cases collection
    Requires admin role
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can import data"
        )
    
    try:
        loader = DataLoaderService(db)
        file_path = loader.data_dir / request.file_name
        
        if not file_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found: {request.file_name}"
            )
        
        results = await loader.load_parquet_file(
            str(file_path),
            batch_size=request.batch_size,
            skip_duplicates=request.skip_duplicates
        )
        
        logger.info(f"Parquet import completed by {current_user.user_id}")
        return {
            "status": "success",
            "message": "Parquet import completed",
            "results": results
        }
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing required dependencies: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error loading parquet file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error loading data: {str(e)}"
        )


@router.get("/statistics")
async def get_import_statistics(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get statistics about imported data"""
    try:
        loader = DataLoaderService(db)
        stats = await loader.get_import_statistics()
        
        logger.info(f"Import statistics retrieved by {current_user.user_id}")
        return stats
    except Exception as e:
        logger.error(f"Error getting import statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting statistics: {str(e)}"
        )


@router.get("/available-files")
async def list_available_files(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """List available parquet and CSV files in data directory"""
    try:
        loader = DataLoaderService(db)
        
        parquet_files = sorted([f.name for f in loader.data_dir.glob("*.parquet")])
        csv_files = sorted([f.name for f in loader.data_dir.glob("*.csv")])
        
        return {
            "parquet_files": parquet_files,
            "csv_files": csv_files,
            "data_directory": str(loader.data_dir)
        }
    except Exception as e:
        logger.error(f"Error listing available files: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing files: {str(e)}"
        )


@router.delete("/clear-collection")
async def clear_cases_collection(
    confirm: bool = Query(..., description="Must be true to confirm deletion"),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """
    Clear all cases from the database
    ⚠️ WARNING: This will delete ALL case data!
    Requires admin role and explicit confirmation
    """
    # Check if user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can clear data"
        )
    
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must confirm deletion by setting confirm=true"
        )
    
    try:
        loader = DataLoaderService(db)
        results = await loader.clear_collection(confirm=True)
        
        logger.warning(f"Cases collection cleared by {current_user.user_id}")
        return {
            "status": "success",
            "message": "Cases collection cleared",
            "results": results
        }
    except Exception as e:
        logger.error(f"Error clearing collection: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing collection: {str(e)}"
        )
