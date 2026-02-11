#!/usr/bin/env python3
"""
Quick test script for Pinecone RAG integration
Tests basic functionality without requiring full API setup
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_imports():
    """Test that all new modules can be imported"""
    print("Testing imports...")
    
    try:
        from app.integrations.pinecone_service import PineconeService
        print("✓ PineconeService imported")
    except Exception as e:
        print(f"✗ PineconeService import failed: {e}")
        return False
    
    try:
        from app.integrations.embedding_service import EmbeddingService
        print("✓ EmbeddingService imported")
    except Exception as e:
        print(f"✗ EmbeddingService import failed: {e}")
        return False
    
    try:
        from app.integrations.document_chunker import DocumentChunker
        print("✓ DocumentChunker imported")
    except Exception as e:
        print(f"✗ DocumentChunker import failed: {e}")
        return False
    
    try:
        from app.services.file_service import FileService
        print("✓ FileService imported")
    except Exception as e:
        print(f"✗ FileService import failed: {e}")
        return False
    
    try:
        from app.services.chatbot_service import ChatbotService
        print("✓ ChatbotService imported")
    except Exception as e:
        print(f"✗ ChatbotService import failed: {e}")
        return False
    
    return True

async def test_document_chunker():
    """Test document chunking"""
    print("\nTesting DocumentChunker...")
    
    from app.integrations.document_chunker import DocumentChunker
    
    chunker = DocumentChunker(chunk_size=100, chunk_overlap=20)
    
    test_text = """
    Child protection is a critical issue in Kenya. According to recent statistics,
    thousands of cases are reported annually. The government has implemented various
    programs to address this challenge. Community involvement is essential for
    effective child protection measures. Education and awareness campaigns play a
    vital role in preventing abuse and neglect.
    """
    
    chunks = chunker.chunk_text(test_text)
    
    print(f"  Text length: {len(test_text)} characters")
    print(f"  Number of chunks: {len(chunks)}")
    
    if len(chunks) > 0:
        print(f"  First chunk length: {len(chunks[0]['text'])} characters")
        print(f"  First chunk preview: {chunks[0]['text'][:50]}...")
        print("✓ Chunking works")
        return True
    else:
        print("✗ Chunking failed")
        return False

async def test_environment_vars():
    """Check if required environment variables are set"""
    print("\nChecking environment variables...")
    
    from app.config import settings
    
    required = {
        "PINECONE_API_KEY": settings.PINECONE_API_KEY,
        "GOOGLE_API_KEY": settings.GOOGLE_API_KEY,
        "GROQ_API_KEY": settings.GROQ_API_KEY,
        "DB_URI": settings.DB_URI,
    }
    
    all_set = True
    for key, value in required.items():
        if value and value != "":
            print(f"  ✓ {key} is set")
        else:
            print(f"  ✗ {key} is NOT set")
            all_set = False
    
    return all_set

async def test_file_extraction():
    """Test text extraction from different formats"""
    print("\nTesting text extraction...")
    
    from app.services.file_service import FileService
    from motor.motor_asyncio import AsyncIOMotorClient
    
    # Create a mock DB connection (won't actually connect)
    try:
        client = AsyncIOMotorClient("mongodb://localhost:27017", serverSelectionTimeoutMS=1000)
        db = client.test_db
        file_service = FileService(db)
        
        # Test plain text
        test_content = b"This is a test document about child protection."
        text = await file_service._extract_text(test_content, "txt")
        
        if text and len(text) > 0:
            print(f"  ✓ Text extraction works: '{text[:30]}...'")
            return True
        else:
            print("  ✗ Text extraction failed")
            return False
            
    except Exception as e:
        print(f"  ✗ Text extraction test failed: {e}")
        return False

async def main():
    """Run all tests"""
    print("=" * 60)
    print("Pinecone RAG Integration - Basic Tests")
    print("=" * 60)
    
    results = []
    
    # Test imports
    results.append(await test_imports())
    
    # Test chunker
    results.append(await test_document_chunker())
    
    # Test environment
    env_ok = await test_environment_vars()
    results.append(env_ok)
    
    # Test file extraction
    results.append(await test_file_extraction())
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("✓ All tests passed!")
        print("\nNext steps:")
        print("1. Ensure PINECONE_API_KEY and GOOGLE_API_KEY are set in .env")
        print("2. Start the API server: uvicorn main:app --reload")
        print("3. Upload a test document via POST /api/v1/files/upload")
        print("4. Try searching with POST /api/v1/files/search")
        print("5. Ask the chatbot about your documents")
    else:
        print(f"✗ {total - passed} test(s) failed")
        if not env_ok:
            print("\n⚠ Missing environment variables. See .env.example")
    
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
