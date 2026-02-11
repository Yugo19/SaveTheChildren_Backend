from typing import Optional, List, Dict, Any
from langchain_groq import ChatGroq
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from app.config import settings
from app.core.logging import logger


class LLMClient:
    """Unified LLM client supporting multiple providers (Groq, Google Gemini, etc.)"""
    
    def __init__(self, provider: str = "groq", model: Optional[str] = None):
        """
        Initialize LLM client
        
        Args:
            provider: LLM provider ("groq" or "google")
            model: Specific model name (uses defaults if not provided)
        """
        self.provider = provider.lower()
        self.model = model
        self.client = self._initialize_client()
        
    def _initialize_client(self):
        """Initialize the appropriate LLM client"""
        try:
            if self.provider == "groq":
                return ChatGroq(
                    temperature=0.7,
                    model_name=self.model or "mixtral-8x7b-32768",
                    groq_api_key=settings.GROQ_API_KEY
                )
            elif self.provider == "google":
                return ChatGoogleGenerativeAI(
                    model=self.model or "gemini-pro",
                    google_api_key=settings.GOOGLE_API_KEY,
                    temperature=0.7
                )
            else:
                logger.warning(f"Unknown provider {self.provider}, defaulting to Groq")
                return ChatGroq(
                    temperature=0.7,
                    model_name="mixtral-8x7b-32768",
                    groq_api_key=settings.GROQ_API_KEY
                )
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            raise
    
    async def generate_response(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None
    ) -> str:
        """
        Generate a response from the LLM
        
        Args:
            prompt: User prompt
            system_prompt: System context
            max_tokens: Maximum tokens in response
            temperature: Temperature for response generation
            
        Returns:
            Generated response text
        """
        try:
            messages = []
            
            if system_prompt:
                messages.append(SystemMessage(content=system_prompt))
            
            messages.append(HumanMessage(content=prompt))
            
            response = self.client.invoke(messages)
            return response.content if hasattr(response, 'content') else str(response)
            
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise
    
    async def generate_streaming_response(
        self,
        prompt: str,
        system_prompt: Optional[str] = None
    ):
        """
        Generate a streaming response from the LLM
        
        Yields chunks of the response as they're generated
        """
        try:
            messages = []
            
            if system_prompt:
                messages.append(SystemMessage(content=system_prompt))
            
            messages.append(HumanMessage(content=prompt))
            
            for chunk in self.client.stream(messages):
                if hasattr(chunk, 'content'):
                    yield chunk.content
                    
        except Exception as e:
            logger.error(f"Error in streaming response: {e}")
            raise
    
    async def batch_generate(
        self,
        prompts: List[str],
        system_prompt: Optional[str] = None
    ) -> List[str]:
        """
        Generate responses for multiple prompts
        
        Args:
            prompts: List of prompts
            system_prompt: Common system context
            
        Returns:
            List of responses
        """
        try:
            responses = []
            for prompt in prompts:
                response = await self.generate_response(prompt, system_prompt)
                responses.append(response)
            return responses
            
        except Exception as e:
            logger.error(f"Error in batch generation: {e}")
            raise
    
    def switch_provider(self, provider: str, model: Optional[str] = None):
        """Switch to a different LLM provider"""
        try:
            self.provider = provider.lower()
            self.model = model
            self.client = self._initialize_client()
            logger.info(f"Switched to provider: {provider}")
        except Exception as e:
            logger.error(f"Failed to switch provider: {e}")
            raise
