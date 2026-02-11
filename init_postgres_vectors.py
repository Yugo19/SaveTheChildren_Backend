#!/usr/bin/env python3
"""
Initialize PostgreSQL Vector Database
Run this after setting up PostgreSQL to ensure everything is configured correctly.
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.integrations.postgres_vector_service import PostgresVectorService
from app.config import settings
from app.core.logging import logger


async def initialize_database():
    """Initialize PostgreSQL vector database"""
    print("=" * 60)
    print("PostgreSQL Vector Database Initialization")
    print("=" * 60)
    
    try:
        # Create service instance
        print(f"\n✓ Connecting to: {settings.POSTGRES_URI.split('@')[1]}")
        vector_service = PostgresVectorService(dimension=384)
        
        # Initialize database (create tables and extension)
        print("✓ Creating pgvector extension...")
        await vector_service.initialize()
        
        # Check stats
        print("✓ Checking database status...")
        stats = await vector_service.get_index_stats()
        
        print("\n" + "=" * 60)
        print("Database Statistics:")
        print("=" * 60)
        print(f"Total Vectors: {stats['total_vector_count']}")
        print(f"Unique Files: {stats['unique_files']}")
        print(f"Embedding Dimension: {stats['dimension']}")
        print(f"Storage Type: {stats['storage']}")
        
        print("\n✅ PostgreSQL vector database initialized successfully!")
        print("=" * 60)
        
        # Close connections
        await vector_service.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error initializing database: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure PostgreSQL is running: docker-compose up -d postgres")
        print("2. Check connection string in .env: POSTGRES_URI")
        print("3. Verify pgvector extension is available")
        return False


async def test_vector_operations():
    """Test basic vector operations"""
    print("\n" + "=" * 60)
    print("Testing Vector Operations")
    print("=" * 60)
    
    try:
        vector_service = PostgresVectorService(dimension=384)
        await vector_service.initialize()
        
        # Test data
        test_chunks = [
            {"text": "This is a test document chunk.", "chunk_index": 0},
            {"text": "Another test chunk with different content.", "chunk_index": 1}
        ]
        test_embeddings = [
            [0.1] * 384,  # Dummy embedding
            [0.2] * 384   # Dummy embedding
        ]
        test_metadata = {
            "file_name": "test_document.txt",
            "file_type": "text/plain"
        }
        
        # Test upsert
        print("✓ Testing document upload...")
        await vector_service.upsert_document_chunks(
            file_id="test_file_123",
            chunks=test_chunks,
            embeddings=test_embeddings,
            metadata=test_metadata
        )
        print("  ✓ Upload successful")
        
        # Test search
        print("✓ Testing vector search...")
        results = await vector_service.search_similar_chunks(
            query_embedding=[0.15] * 384,
            top_k=2
        )
        print(f"  ✓ Found {len(results)} results")
        
        # Test delete
        print("✓ Testing document deletion...")
        await vector_service.delete_document("test_file_123")
        print("  ✓ Deletion successful")
        
        await vector_service.close()
        
        print("\n✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False


async def main():
    """Main initialization routine"""
    # Initialize database
    success = await initialize_database()
    
    if success:
        # Run tests
        test_success = await test_vector_operations()
        
        if test_success:
            print("\n🎉 Setup complete! Your PostgreSQL vector database is ready.")
            print("\nNext steps:")
            print("1. Start the application: uvicorn main:app --reload")
            print("2. Upload documents via API: POST /api/v1/files/upload")
            print("3. Chat with RAG: POST /api/v1/chatbot/conversations/{id}/message")
        else:
            print("\n⚠️  Database initialized but tests failed. Check logs.")
            sys.exit(1)
    else:
        print("\n❌ Initialization failed. Please fix errors and try again.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
