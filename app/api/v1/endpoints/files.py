from fastapi import APIRouter, Depends, Query, UploadFile, File, HTTPException, status
from typing import Optional
from pydantic import BaseModel
from app.db.client import get_database
from app.services.file_service import FileService
from app.core.security import admin_required, TokenData, get_current_user
from app.core.logging import logger

router = APIRouter(prefix="/files", tags=["Files"])


class FileResponse(BaseModel):
    file_id: str
    file_name: str
    file_type: str
    size_bytes: int
    upload_date: str


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    file_type: str = Query(...),
    description: Optional[str] = None,
    current_user: TokenData = Depends(admin_required),
    db=Depends(get_database)
):
    """Upload file, chunk it, and index in PostgreSQL vector database for RAG (Admin only)"""
    try:
        # Validate file type
        allowed_types = ["csv", "xlsx", "pdf", "json", "txt", "doc", "docx"]
        if file_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Allowed: {', '.join(allowed_types)}"
            )

        # Read file content
        content = await file.read()

        if len(content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is empty"
            )

        if len(content) > 100 * 1024 * 1024:  # 100MB limit
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="File too large (max 100MB)"
            )

        # Upload file
        file_service = FileService(db)
        result = await file_service.upload_file(
            content,
            file.filename,
            file_type,
            current_user.user_id,
            description
        )

        logger.info(f"File uploaded and indexed by {current_user.user_id}: {file.filename}")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error uploading file"
        )


@router.get("")
async def list_files(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    file_type: Optional[str] = None,
    uploaded_by: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """List uploaded files"""
    file_service = FileService(db)
    result = await file_service.list_files(
        page=page,
        limit=limit,
        file_type=file_type,
        uploaded_by=uploaded_by
    )

    logger.info(f"Files listed by {current_user.user_id}")
    return result


@router.get("/{file_id}")
async def get_file_info(
    file_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get file information"""
    file_service = FileService(db)
    file_doc = await file_service.get_file(file_id)

    return {
        "file_id": file_doc["file_id"],
        "file_name": file_doc["file_name"],
        "file_type": file_doc["file_type"],
        "size_bytes": file_doc["size_bytes"],
        "chunk_count": file_doc.get("chunk_count", 0),
        "upload_date": file_doc["upload_date"],
        "description": file_doc.get("description", ""),
        "indexed_in_vector_db": file_doc.get("indexed_in_vector_db", False)
    }


@router.get("/{file_id}/download")
async def download_file(
    file_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Download file"""
    from fastapi.responses import StreamingResponse
    import io

    file_service = FileService(db)
    file_doc = await file_service.get_file(file_id)

    # Download content
    content = await file_service.get_file_content(file_id)

    logger.info(f"File downloaded by {current_user.user_id}: {file_doc['file_name']}")

    # Return as stream
    stream = io.BytesIO(content)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={file_doc['file_name']}"}
    )


@router.delete("/{file_id}")
async def delete_file(
    file_id: str,
    current_user: TokenData = Depends(admin_required),
    db=Depends(get_database)
):
    """Delete file (Admin only)"""
    file_service = FileService(db)
    await file_service.delete_file(file_id)
    logger.info(f"File deleted by {current_user.user_id}: {file_id}")

    return {"message": "File deleted successfully"}


@router.post("/search")
async def search_documents(
    query: str = Query(..., description="Search query"),
    top_k: int = Query(5, ge=1, le=20, description="Number of results"),
    file_type: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Search documents using semantic similarity (RAG)"""
    try:
        file_service = FileService(db)
        results = await file_service.search_documents(
            query=query,
            top_k=top_k,
            file_type=file_type
        )
        
        logger.info(f"Document search by {current_user.user_id}: '{query}' - {len(results)} results")
        
        return {
            "query": query,
            "results_count": len(results),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error searching documents: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error searching documents"
        )
