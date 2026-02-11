from pinecone import Pinecone, ServerlessSpec
from app.config import settings
from app.core.logging import logger
from typing import List, Dict, Optional
import hashlib
import time


class PineconeService:
    def __init__(self):
        self.pc = Pinecone(api_key=settings.PINECONE_API_KEY)
        self.index_name = settings.PINECONE_INDEX
        self.dimension = None  # Will be set based on embedding service
        self._ensure_index()
        self.index = self.pc.Index(self.index_name)
    
    def _ensure_index(self, dimension: int = 384):
        """
        Create index if it doesn't exist
        
        Args:
            dimension: Embedding dimension (384 for local models, 768 for Google, 1536 for OpenAI)
        """
        try:
            existing_indexes = [index.name for index in self.pc.list_indexes()]
            
            if self.index_name not in existing_indexes:
                logger.info(f"Creating Pinecone index: {self.index_name} with dimension {dimension}")
                self.pc.create_index(
                    name=self.index_name,
                    dimension=dimension,
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud="aws",
                        region="us-east-1"
                    )
                )
                # Wait for index to be ready
                time.sleep(1)
                logger.info(f"Pinecone index created: {self.index_name}")
            else:
                # Get existing index dimension
                index_info = self.pc.describe_index(self.index_name)
                self.dimension = index_info.dimension
                logger.info(f"Using existing Pinecone index with dimension {self.dimension}")
        except Exception as e:
            logger.error(f"Error ensuring Pinecone index: {e}")
            raise
    
    def update_dimension(self, dimension: int):
        """Update the expected dimension (must match embedding service)"""
        if self.dimension and self.dimension != dimension:
            logger.warning(f"Dimension mismatch: index={self.dimension}, embeddings={dimension}")
            logger.warning("You may need to recreate the Pinecone index with correct dimension")
        self.dimension = dimension
    
    async def upsert_document_chunks(
        self,
        file_id: str,
        chunks: List[Dict[str, any]],
        embeddings: List[List[float]],
        metadata: Dict = None
    ) -> bool:
        """
        Upload document chunks with embeddings to Pinecone
        
        Args:
            file_id: Unique file identifier
            chunks: List of chunk dictionaries with 'text' and 'chunk_index'
            embeddings: List of embedding vectors
            metadata: Additional metadata to attach to all chunks
        """
        try:
            vectors = []
            base_metadata = metadata or {}
            
            for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                # Create unique ID for each chunk
                chunk_id = f"{file_id}_{chunk['chunk_index']}"
                
                # Combine metadata
                chunk_metadata = {
                    **base_metadata,
                    "file_id": file_id,
                    "chunk_index": chunk['chunk_index'],
                    "text": chunk['text'][:1000],  # Pinecone metadata limit
                    "chunk_size": len(chunk['text'])
                }
                
                vectors.append({
                    "id": chunk_id,
                    "values": embedding,
                    "metadata": chunk_metadata
                })
            
            # Upsert in batches of 100
            batch_size = 100
            for i in range(0, len(vectors), batch_size):
                batch = vectors[i:i + batch_size]
                self.index.upsert(vectors=batch)
            
            logger.info(f"Upserted {len(vectors)} chunks for file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error upserting document chunks to Pinecone: {e}")
            raise
    
    async def search_similar_chunks(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        filter_dict: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Search for similar document chunks
        
        Args:
            query_embedding: Query vector
            top_k: Number of results to return
            filter_dict: Metadata filters
        """
        try:
            results = self.index.query(
                vector=query_embedding,
                top_k=top_k,
                include_metadata=True,
                filter=filter_dict
            )
            
            chunks = []
            for match in results.matches:
                chunks.append({
                    "id": match.id,
                    "score": match.score,
                    "text": match.metadata.get("text", ""),
                    "file_id": match.metadata.get("file_id", ""),
                    "chunk_index": match.metadata.get("chunk_index", 0),
                    "metadata": match.metadata
                })
            
            return chunks
            
        except Exception as e:
            logger.error(f"Error searching Pinecone: {e}")
            raise
    
    async def delete_document(self, file_id: str) -> bool:
        """Delete all chunks for a document"""
        try:
            # Delete by filtering on file_id
            self.index.delete(filter={"file_id": file_id})
            logger.info(f"Deleted document chunks for file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting document from Pinecone: {e}")
            raise
    
    async def get_index_stats(self) -> Dict:
        """Get Pinecone index statistics"""
        try:
            stats = self.index.describe_index_stats()
            return {
                "total_vector_count": stats.total_vector_count,
                "dimension": stats.dimension,
                "index_fullness": stats.index_fullness
            }
        except Exception as e:
            logger.error(f"Error getting Pinecone stats: {e}")
            raise
