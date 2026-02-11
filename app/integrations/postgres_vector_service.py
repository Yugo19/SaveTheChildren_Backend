from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Text, DateTime, JSON, select, delete, text
from pgvector.sqlalchemy import Vector
from app.config import settings
from app.core.logging import logger
from typing import List, Dict, Optional
from datetime import datetime, timezone


Base = declarative_base()


class DocumentChunk(Base):
    """PostgreSQL table for storing document embeddings"""
    __tablename__ = "document_chunks"
    
    id = Column(String, primary_key=True)
    file_id = Column(String, index=True, nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    chunk_size = Column(Integer)
    embedding = Column(Vector(384))  # Default dimension for local models
    meta_data = Column("metadata", JSON)  # Use meta_data as Python attribute, metadata as DB column
    created_at = Column(DateTime(timezone=False), default=datetime.utcnow)


class PostgresVectorService:
    def __init__(self, dimension: int = 384):
        """
        Initialize PostgreSQL vector service
        
        Args:
            dimension: Embedding dimension (384 for local, 768 for Google, 1536 for OpenAI)
        """
        self.dimension = dimension
        self.engine = create_async_engine(
            settings.POSTGRES_URI,
            echo=False,
            pool_size=10,
            max_overflow=20
        )
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        logger.info(f"PostgreSQL Vector Service initialized with dimension {dimension}")
    
    async def initialize(self):
        """Create tables and enable pgvector extension"""
        try:
            async with self.engine.begin() as conn:
                # Enable pgvector extension
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                
                # Create tables
                await conn.run_sync(Base.metadata.create_all)
                
            logger.info("PostgreSQL vector database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing PostgreSQL vector database: {e}")
            raise
    
    def update_dimension(self, dimension: int):
        """Update the expected dimension (must match embedding service)"""
        if self.dimension != dimension:
            logger.warning(f"Dimension mismatch: current={self.dimension}, requested={dimension}")
            logger.warning("You may need to drop and recreate the table with correct dimension")
        self.dimension = dimension
    
    async def upsert_document_chunks(
        self,
        file_id: str,
        chunks: List[Dict[str, any]],
        embeddings: List[List[float]],
        metadata: Dict = None
    ) -> bool:
        """
        Upload document chunks with embeddings to PostgreSQL
        
        Args:
            file_id: Unique file identifier
            chunks: List of chunk dictionaries with 'text' and 'chunk_index'
            embeddings: List of embedding vectors
            metadata: Additional metadata to attach to all chunks
        """
        try:
            async with self.async_session() as session:
                base_metadata = metadata or {}
                
                for chunk, embedding in zip(chunks, embeddings):
                    chunk_id = f"{file_id}_{chunk['chunk_index']}"
                    
                    chunk_metadata = {
                        **base_metadata,
                        "file_id": file_id,
                        "chunk_index": chunk['chunk_index']
                    }
                    
                    doc_chunk = DocumentChunk(
                        id=chunk_id,
                        file_id=file_id,
                        chunk_index=chunk['chunk_index'],
                        text=chunk['text'][:10000],  # Store full text
                        chunk_size=len(chunk['text']),
                        embedding=embedding,
                        meta_data=chunk_metadata
                    )
                    
                    # Upsert: delete if exists, then insert
                    await session.execute(
                        delete(DocumentChunk).where(DocumentChunk.id == chunk_id)
                    )
                    session.add(doc_chunk)
                
                await session.commit()
            
            logger.info(f"Upserted {len(chunks)} chunks for file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error upserting document chunks to PostgreSQL: {e}")
            raise
    
    async def search_similar_chunks(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        filter_dict: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Search for similar document chunks using cosine similarity
        
        Args:
            query_embedding: Query vector
            top_k: Number of results to return
            filter_dict: Metadata filters (e.g., {"file_id": "xyz"})
        """
        try:
            async with self.async_session() as session:
                # Build query with vector similarity search
                query = select(
                    DocumentChunk,
                    (1 - DocumentChunk.embedding.cosine_distance(query_embedding)).label("score")
                )
                
                # Apply filters if provided
                if filter_dict:
                    if "file_id" in filter_dict:
                        query = query.where(DocumentChunk.file_id == filter_dict["file_id"])
                
                # Order by similarity and limit
                query = query.order_by(
                    DocumentChunk.embedding.cosine_distance(query_embedding)
                ).limit(top_k)
                
                result = await session.execute(query)
                rows = result.all()
                
                chunks = []
                for row in rows:
                    doc_chunk = row[0]
                    score = row[1]
                    chunks.append({
                        "id": doc_chunk.id,
                        "score": float(score),
                        "text": doc_chunk.text,
                        "file_id": doc_chunk.file_id,
                        "chunk_index": doc_chunk.chunk_index,
                        "metadata": doc_chunk.meta_data or {}
                    })
                
                return chunks
            
        except Exception as e:
            logger.error(f"Error searching PostgreSQL vectors: {e}")
            raise
    
    async def delete_document(self, file_id: str) -> bool:
        """Delete all chunks for a document"""
        try:
            async with self.async_session() as session:
                await session.execute(
                    delete(DocumentChunk).where(DocumentChunk.file_id == file_id)
                )
                await session.commit()
            
            logger.info(f"Deleted document chunks for file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document from PostgreSQL: {e}")
            raise
    
    async def get_index_stats(self) -> Dict:
        """Get PostgreSQL vector database statistics"""
        try:
            async with self.async_session() as session:
                # Count total vectors
                result = await session.execute(
                    select(DocumentChunk)
                )
                total_count = len(result.all())
                
                # Count unique files
                result = await session.execute(
                    select(DocumentChunk.file_id).distinct()
                )
                unique_files = len(result.all())
                
                return {
                    "total_vector_count": total_count,
                    "unique_files": unique_files,
                    "dimension": self.dimension,
                    "storage": "postgresql_local"
                }
        except Exception as e:
            logger.error(f"Error getting PostgreSQL stats: {e}")
            raise
    
    async def close(self):
        """Close database connections"""
        await self.engine.dispose()
