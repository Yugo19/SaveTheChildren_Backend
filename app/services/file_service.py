from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timezone
from typing import List, Dict
from app.integrations.postgres_vector_service import PostgresVectorService
from app.integrations.embedding_service import EmbeddingService
from app.integrations.document_chunker import DocumentChunker
from app.config import settings
from app.core.logging import logger
import uuid
import base64


class FileService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.files_collection = db.files
        self.embedding_service = EmbeddingService(
            preferred_provider=settings.EMBEDDING_PROVIDER
        )
        self.vector_service = PostgresVectorService(
            dimension=self.embedding_service.dimension if self.embedding_service.available else 384
        )
        self.chunker = DocumentChunker(chunk_size=1000, chunk_overlap=200)
        
        # Initialize PostgreSQL vector database
        import asyncio
        asyncio.create_task(self.vector_service.initialize())
        
        logger.info(f"Using embeddings: {self.embedding_service.provider} (dim: {self.embedding_service.dimension})")
        logger.info("Using PostgreSQL for local vector storage")

    async def upload_file(
        self,
        file_content: bytes,
        file_name: str,
        file_type: str,
        user_id: str,
        description: str = None
    ) -> dict:
        """Upload file, chunk it, generate embeddings, and store in PostgreSQL vector database"""
        try:
            file_id = str(uuid.uuid4())
            
            # Extract text from file content
            text_content = await self._extract_text(file_content, file_type)
            
            if not text_content:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Could not extract text from file"
                )
            
            # Chunk the document
            chunks = self.chunker.chunk_text(text_content)
            
            # Generate embeddings for chunks
            chunk_texts = [chunk['text'] for chunk in chunks]
            embeddings = await self.embedding_service.embed_texts(chunk_texts)
            
            # Store in PostgreSQL vector database with metadata
            metadata = {
                "file_name": file_name,
                "file_type": file_type,
                "uploaded_by": user_id,
                "upload_date": datetime.now(timezone.utc).isoformat(),
                "description": description or ""
            }
            
            await self.vector_service.upsert_document_chunks(
                file_id,
                chunks,
                embeddings,
                metadata
            )
            
            # Store file content as base64 in MongoDB (for download capability)
            file_content_b64 = base64.b64encode(file_content).decode('utf-8')
            
            # Store metadata in MongoDB
            file_doc = {
                "file_id": file_id,
                "file_name": file_name,
                "file_type": file_type,
                "size_bytes": len(file_content),
                "chunk_count": len(chunks),
                "upload_date": datetime.now(timezone.utc),
                "uploaded_by": ObjectId(user_id),
                "description": description or "",
                "file_content": file_content_b64,  # Store original file
                "indexed_in_vector_db": True
            }

            result = await self.files_collection.insert_one(file_doc)
            logger.info(f"File uploaded and indexed: {file_id} with {len(chunks)} chunks")

            return {
                "file_id": file_id,
                "file_name": file_name,
                "file_type": file_type,
                "size_bytes": len(file_content),
                "chunk_count": len(chunks),
                "upload_date": file_doc["upload_date"],
                "indexed_in_vector_db": True
            }
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise

    async def get_file(self, file_id: str):
        """Get file metadata"""
        try:
            file_doc = await self.files_collection.find_one({"file_id": file_id})
            if not file_doc:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="File not found"
                )
            return file_doc
        except Exception as e:
            logger.error(f"Error getting file: {e}")
            raise

    async def list_files(
        self,
        page: int = 1,
        limit: int = 20,
        file_type: str = None,
        uploaded_by: str = None
    ):
        """List files with filtering"""
        try:
            filters = {}
            if file_type:
                filters["file_type"] = file_type
            if uploaded_by:
                filters["uploaded_by"] = ObjectId(uploaded_by)

            total = await self.files_collection.count_documents(filters)

            files = await self.files_collection.find(filters)\
                .skip((page - 1) * limit)\
                .limit(limit)\
                .sort("upload_date", -1)\
                .to_list(limit)

            # Convert ObjectId fields to strings for JSON serialization
            serialized_files = []
            for file_doc in files:
                serialized_file = {
                    "file_id": file_doc["file_id"],
                    "file_name": file_doc["file_name"],
                    "file_type": file_doc["file_type"],
                    "size_bytes": file_doc["size_bytes"],
                    "chunk_count": file_doc.get("chunk_count", 0),
                    "upload_date": file_doc["upload_date"].isoformat() if isinstance(file_doc["upload_date"], datetime) else file_doc["upload_date"],
                    "uploaded_by": str(file_doc["uploaded_by"]),
                    "description": file_doc.get("description", ""),
                    "indexed_in_vector_db": file_doc.get("indexed_in_vector_db", False)
                }
                serialized_files.append(serialized_file)

            return {
                "total": total,
                "page": page,
                "limit": limit,
                "files": serialized_files
            }
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    async def delete_file(self, file_id: str):
        """Delete file from PostgreSQL vector DB and MongoDB"""
        try:
            file_doc = await self.files_collection.find_one({"file_id": file_id})

            if not file_doc:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="File not found"
                )

            # Delete from PostgreSQL vector database
            try:
                await self.vector_service.delete_document(file_id)
            except Exception as e:
                logger.error(f"Error deleting file from vector database: {e}")

            # Delete from MongoDB
            await self.files_collection.delete_one({"file_id": file_id})
            logger.info(f"File deleted: {file_id}")

            return True
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            raise

    async def get_file_content(self, file_id: str):
        """Get file content from MongoDB"""
        try:
            file_doc = await self.files_collection.find_one({"file_id": file_id})

            if not file_doc:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="File not found"
                )

            # Decode base64 content
            content = base64.b64decode(file_doc["file_content"])
            logger.info(f"File content retrieved: {file_id}")

            return content
        except Exception as e:
            logger.error(f"Error getting file content: {e}")
            raise
    
    async def search_documents(
        self,
        query: str,
        top_k: int = 5,
        file_type: str = None
    ) -> List[Dict]:
        """Search documents using semantic similarity"""
        try:
            # Generate query embedding
            query_embedding = await self.embedding_service.embed_text(query)
            
            # Build filter
            filter_dict = {}
            if file_type:
                filter_dict["file_type"] = file_type
            
            # Search PostgreSQL vector database
            results = await self.vector_service.search_similar_chunks(
                query_embedding,
                top_k=top_k,
                filter_dict=filter_dict if filter_dict else None
            )
            
            logger.info(f"Document search completed: {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            raise
    
    async def _extract_text(self, file_content: bytes, file_type: str) -> str:
        """Extract text from various file formats"""
        try:
            if file_type in ['txt', 'text']:
                return file_content.decode('utf-8')
            
            elif file_type == 'json':
                import json
                data = json.loads(file_content.decode('utf-8'))
                return json.dumps(data, indent=2)
            
            elif file_type == 'csv':
                return file_content.decode('utf-8')
            
            elif file_type == 'pdf':
                try:
                    import PyPDF2
                    import io
                    pdf_file = io.BytesIO(file_content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ""
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"
                    return text
                except ImportError:
                    logger.warning("PyPDF2 not installed, storing PDF as binary")
                    return f"[PDF Document: {len(file_content)} bytes]"
            
            elif file_type in ['doc', 'docx']:
                try:
                    import docx
                    import io
                    doc = docx.Document(io.BytesIO(file_content))
                    text = "\n".join([para.text for para in doc.paragraphs])
                    return text
                except ImportError:
                    logger.warning("python-docx not installed, storing DOCX as binary")
                    return f"[DOCX Document: {len(file_content)} bytes]"
            
            else:
                # Try to decode as text
                try:
                    return file_content.decode('utf-8')
                except:
                    return f"[Binary file: {len(file_content)} bytes]"
                    
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
            return f"[Could not extract text from {file_type} file]"
