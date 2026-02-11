from typing import List, Dict, Optional
from app.core.logging import logger
import asyncio

# Try importing Google AI
try:
    from langchain_google_genai import GoogleGenerativeAIEmbeddings
    GOOGLE_AVAILABLE = True
except ImportError:
    logger.warning("Google GenAI not available for embeddings")
    GOOGLE_AVAILABLE = False

# Try importing Sentence Transformers (local, free)
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    logger.warning("Sentence Transformers not available. Install: pip install sentence-transformers")
    SENTENCE_TRANSFORMERS_AVAILABLE = False

# Try importing HuggingFace (free API)
try:
    from langchain_huggingface import HuggingFaceEmbeddings
    HUGGINGFACE_AVAILABLE = True
except ImportError:
    logger.warning("HuggingFace embeddings not available")
    HUGGINGFACE_AVAILABLE = False


class EmbeddingService:
    """
    Multi-provider embedding service with automatic fallback.
    Priority: Google AI (free tier) → Local Sentence-Transformers → HuggingFace
    """
    
    def __init__(self, preferred_provider: str = "auto"):
        """
        Initialize embedding service with fallback support
        
        Args:
            preferred_provider: "google", "local", "huggingface", or "auto" (default)
        """
        self.provider = None
        self.embeddings = None
        self.model_name = None
        self.dimension = 1536  # Default for Google/OpenAI compatibility
        
        # Try to initialize based on preference
        if preferred_provider == "auto":
            self._init_auto()
        elif preferred_provider == "google":
            self._init_google()
        elif preferred_provider == "local":
            self._init_local()
        elif preferred_provider == "huggingface":
            self._init_huggingface()
        else:
            logger.warning(f"Unknown provider: {preferred_provider}, using auto")
            self._init_auto()
        
        if self.provider:
            logger.info(f"Embedding service initialized with provider: {self.provider} (dimension: {self.dimension})")
        else:
            logger.error("No embedding provider available!")
    
    def _init_auto(self):
        """Auto-select best available provider"""
        # Try Google first (has free tier)
        if self._init_google():
            return
        
        # Fall back to local (completely free)
        if self._init_local():
            return
        
        # Last resort: HuggingFace
        if self._init_huggingface():
            return
    
    def _init_google(self) -> bool:
        """Initialize Google AI embeddings"""
        if not GOOGLE_AVAILABLE:
            return False
        
        try:
            self.embeddings = GoogleGenerativeAIEmbeddings(
                model="models/text-embedding-004"
            )
            self.provider = "google"
            self.model_name = "text-embedding-004"
            self.dimension = 768  # Google's embedding dimension
            logger.info("Google AI embeddings initialized (1,500 req/day free)")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize Google embeddings: {e}")
            return False
    
    def _init_local(self) -> bool:
        """Initialize local Sentence Transformers (completely free)"""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            return False
        
        try:
            # Use all-MiniLM-L6-v2: fast, 384-dim, good quality
            model_name = "all-MiniLM-L6-v2"
            self.embeddings = SentenceTransformer(model_name)
            self.provider = "local"
            self.model_name = model_name
            self.dimension = 384  # This model's dimension
            logger.info(f"Local embeddings initialized: {model_name} (FREE, unlimited)")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize local embeddings: {e}")
            return False
    
    def _init_huggingface(self) -> bool:
        """Initialize HuggingFace embeddings"""
        if not HUGGINGFACE_AVAILABLE:
            return False
        
        try:
            self.embeddings = HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2"
            )
            self.provider = "huggingface"
            self.model_name = "all-MiniLM-L6-v2"
            self.dimension = 384
            logger.info("HuggingFace embeddings initialized")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize HuggingFace embeddings: {e}")
            return False
    
    @property
    def available(self) -> bool:
        """Check if any embedding provider is available"""
        return self.provider is not None
    
    async def embed_text(self, text: str) -> List[float]:
        """Generate embedding for a single text with fallback"""
        if not self.available:
            raise RuntimeError("No embedding provider available")
        
        # Try current provider
        try:
            return await self._embed_with_provider(text, single=True)
        except Exception as e:
            logger.error(f"Error with {self.provider} provider: {e}")
            
            # Try fallback
            if await self._try_fallback():
                return await self._embed_with_provider(text, single=True)
            
            raise RuntimeError(f"All embedding providers failed: {e}")
    
    async def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts with fallback"""
        if not self.available:
            raise RuntimeError("No embedding provider available")
        
        # Try current provider
        try:
            return await self._embed_with_provider(texts, single=False)
        except Exception as e:
            logger.error(f"Error with {self.provider} provider: {e}")
            
            # Try fallback
            if await self._try_fallback():
                return await self._embed_with_provider(texts, single=False)
            
            raise RuntimeError(f"All embedding providers failed: {e}")
    
    async def _embed_with_provider(self, text_or_texts, single: bool) -> List:
        """Generate embeddings using current provider"""
        if self.provider == "google":
            # Google uses LangChain interface
            if single:
                return self.embeddings.embed_query(text_or_texts)
            else:
                return self.embeddings.embed_documents(text_or_texts)
        
        elif self.provider == "local":
            # Sentence Transformers - run in thread pool to not block
            loop = asyncio.get_event_loop()
            if single:
                embedding = await loop.run_in_executor(
                    None, 
                    self.embeddings.encode, 
                    text_or_texts
                )
                return embedding.tolist()
            else:
                embeddings = await loop.run_in_executor(
                    None,
                    self.embeddings.encode,
                    text_or_texts
                )
                return embeddings.tolist()
        
        elif self.provider == "huggingface":
            # HuggingFace uses LangChain interface
            if single:
                return self.embeddings.embed_query(text_or_texts)
            else:
                return self.embeddings.embed_documents(text_or_texts)
        
        else:
            raise RuntimeError(f"Unknown provider: {self.provider}")
    
    async def _try_fallback(self) -> bool:
        """Try to switch to fallback provider"""
        logger.warning(f"Attempting fallback from {self.provider}")
        
        current = self.provider
        
        # If Google failed, try local
        if current == "google":
            if self._init_local():
                logger.info("Switched to local embeddings")
                return True
            if self._init_huggingface():
                logger.info("Switched to HuggingFace embeddings")
                return True
        
        # If local failed, try HuggingFace
        elif current == "local":
            if self._init_huggingface():
                logger.info("Switched to HuggingFace embeddings")
                return True
            if self._init_google():
                logger.info("Switched to Google embeddings")
                return True
        
        # If HuggingFace failed, try others
        elif current == "huggingface":
            if self._init_local():
                logger.info("Switched to local embeddings")
                return True
            if self._init_google():
                logger.info("Switched to Google embeddings")
                return True
        
        logger.error("No fallback provider available")
        return False
    
    def get_info(self) -> Dict:
        """Get current provider information"""
        return {
            "provider": self.provider,
            "model": self.model_name,
            "dimension": self.dimension,
            "available": self.available,
            "supports_fallback": True
        }
