from typing import List, Dict
from app.core.logging import logger
import re


class DocumentChunker:
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        """
        Initialize document chunker
        
        Args:
            chunk_size: Target size of each chunk in characters
            chunk_overlap: Overlap between chunks in characters
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    def chunk_text(self, text: str) -> List[Dict[str, any]]:
        """
        Split text into overlapping chunks
        
        Returns:
            List of dictionaries with 'text' and 'chunk_index'
        """
        try:
            # Clean text
            text = self._clean_text(text)
            
            if len(text) <= self.chunk_size:
                return [{"text": text, "chunk_index": 0}]
            
            chunks = []
            start = 0
            chunk_index = 0
            
            while start < len(text):
                # Calculate end position
                end = start + self.chunk_size
                
                # If not the last chunk, try to break at sentence boundary
                if end < len(text):
                    # Look for sentence endings
                    chunk_text = text[start:end]
                    last_period = chunk_text.rfind('. ')
                    last_newline = chunk_text.rfind('\n')
                    
                    break_point = max(last_period, last_newline)
                    
                    if break_point > self.chunk_size // 2:
                        end = start + break_point + 1
                
                chunk_text = text[start:end].strip()
                
                if chunk_text:
                    chunks.append({
                        "text": chunk_text,
                        "chunk_index": chunk_index
                    })
                    chunk_index += 1
                
                # Move start position with overlap
                start = end - self.chunk_overlap
                
                # Prevent infinite loop
                if start >= len(text) - self.chunk_overlap:
                    break
            
            logger.info(f"Created {len(chunks)} chunks from text of length {len(text)}")
            return chunks
            
        except Exception as e:
            logger.error(f"Error chunking text: {e}")
            raise
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters that might cause issues
        text = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        return text.strip()
    
    def chunk_document_by_sections(
        self,
        text: str,
        section_headers: List[str] = None
    ) -> List[Dict[str, any]]:
        """
        Split document by sections with headers
        
        Args:
            text: Full document text
            section_headers: List of regex patterns for section headers
        """
        try:
            if not section_headers:
                # Default section patterns
                section_headers = [
                    r'^#+\s+.+$',  # Markdown headers
                    r'^\d+\.\s+.+$',  # Numbered sections
                    r'^[A-Z][A-Z\s]+$',  # ALL CAPS headers
                ]
            
            # Split by lines
            lines = text.split('\n')
            chunks = []
            current_chunk = []
            chunk_index = 0
            
            for line in lines:
                # Check if line is a section header
                is_header = any(
                    re.match(pattern, line.strip())
                    for pattern in section_headers
                )
                
                if is_header and current_chunk:
                    # Save current chunk
                    chunk_text = '\n'.join(current_chunk).strip()
                    if chunk_text:
                        chunks.append({
                            "text": chunk_text,
                            "chunk_index": chunk_index
                        })
                        chunk_index += 1
                    current_chunk = []
                
                current_chunk.append(line)
            
            # Add last chunk
            if current_chunk:
                chunk_text = '\n'.join(current_chunk).strip()
                if chunk_text:
                    chunks.append({
                        "text": chunk_text,
                        "chunk_index": chunk_index
                    })
            
            logger.info(f"Created {len(chunks)} section-based chunks")
            return chunks
            
        except Exception as e:
            logger.error(f"Error chunking by sections: {e}")
            raise
