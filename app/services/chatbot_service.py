from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime, timezone
from app.core.logging import logger
from app.config import settings
from app.integrations.postgres_vector_service import PostgresVectorService
from app.integrations.embedding_service import EmbeddingService
from typing import Optional, List, Dict
import uuid

try:
    from langchain_groq import ChatGroq
    from langchain_core.messages import HumanMessage, SystemMessage
except ImportError:
    logger.warning("LangChain dependencies not installed. Chatbot will use mock responses.")


class ChatbotService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.conversations_collection = db.conversations
        self.messages_collection = db.messages
        self.token_usage_collection = db.token_usage
        
        # Data source collections
        self.cases_collection = db.cases
        self.scraping_results_collection = db.scraping_results
        self.reports_collection = db.reports
        self.kenya_data_collection = db.kenya_api_data
        self.files_collection = db.files
        
        # PostgreSQL Vector and embedding services
        try:
            self.embedding_service = EmbeddingService(
                preferred_provider=settings.EMBEDDING_PROVIDER
            )
            
            self.vector_service = PostgresVectorService(
                dimension=self.embedding_service.dimension if self.embedding_service.available else 384
            )
            
            # Initialize PostgreSQL vector database
            import asyncio
            asyncio.create_task(self.vector_service.initialize())
            
            self.rag_available = self.embedding_service.available
            logger.info(f"RAG initialized with {self.embedding_service.provider} embeddings and PostgreSQL vectors")
        except Exception as e:
            logger.warning(f"RAG services unavailable: {e}")
            self.vector_service = None
            self.embedding_service = None
            self.rag_available = False
        
        # Initialize LLM (Groq)
        try:
            self.llm = ChatGroq(temperature=0.7, model_name="llama-3.1-8b-instant")
            self.llm_available = True
        except Exception as e:
            logger.warning(f"Failed to initialize Groq LLM: {e}. Using mock responses.")
            self.llm = None
            self.llm_available = False

    async def create_conversation(self, user_id: str, title: str = None) -> dict:
        """Create a new conversation"""
        try:
            conversation_id = str(uuid.uuid4())
            
            conv_doc = {
                "conversation_id": conversation_id,
                "title": title or f"Conversation {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                "user_id": ObjectId(user_id),
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "message_count": 0
            }
            
            result = await self.conversations_collection.insert_one(conv_doc)
            logger.info(f"Conversation created: {conversation_id}")
            
            return {
                "conversation_id": conversation_id,
                "title": conv_doc["title"],
                "created_at": conv_doc["created_at"]
            }
        except Exception as e:
            logger.error(f"Error creating conversation: {e}")
            raise

    async def send_message(self, conversation_id: str, user_id: str, message_text: str):
        """Send message and get AI response with data source integration"""
        try:
            conversation = await self.conversations_collection.find_one({
                "conversation_id": conversation_id,
                "user_id": ObjectId(user_id)
            })
            
            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            
            message_id = str(uuid.uuid4())
            user_msg_doc = {
                "message_id": message_id,
                "conversation_id": conversation_id,
                "sender": "user",
                "text": message_text,
                "timestamp": datetime.now(timezone.utc)
            }
            
            await self.messages_collection.insert_one(user_msg_doc)
            
            # Get relevant context from data sources
            context = await self._gather_context(message_text)
            
            # Get AI response with context
            ai_response, tokens_used = await self._get_ai_response(
                message_text, 
                conversation_id,
                context
            )
            
            ai_message_id = str(uuid.uuid4())
            ai_msg_doc = {
                "message_id": ai_message_id,
                "conversation_id": conversation_id,
                "sender": "assistant",
                "text": ai_response,
                "timestamp": datetime.now(timezone.utc),
                "context_sources": context.get("sources", []),
                "tokens_used": tokens_used
            }
            
            await self.messages_collection.insert_one(ai_msg_doc)
            
            # Track token usage
            if tokens_used > 0:
                await self._track_token_usage(user_id, tokens_used)
            
            await self.conversations_collection.update_one(
                {"conversation_id": conversation_id},
                {
                    "$inc": {"message_count": 2},
                    "$set": {"updated_at": datetime.now(timezone.utc)}
                }
            )
            
            logger.info(f"Message sent in conversation: {conversation_id}, tokens: {tokens_used}")
            
            return {
                "user_message": {
                    "message_id": message_id,
                    "text": message_text,
                    "timestamp": user_msg_doc["timestamp"]
                },
                "ai_response": {
                    "message_id": ai_message_id,
                    "text": ai_response,
                    "timestamp": ai_msg_doc["timestamp"],
                    "context_sources": context.get("sources", []),
                    "tokens_used": tokens_used
                }
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    async def get_conversation_history(self, conversation_id: str, user_id: str, limit: int = 50):
        """Get conversation message history"""
        try:
            conversation = await self.conversations_collection.find_one({
                "conversation_id": conversation_id,
                "user_id": ObjectId(user_id)
            })
            
            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            
            messages = await self.messages_collection.find(
                {"conversation_id": conversation_id}
            ).sort("timestamp", 1).limit(limit).to_list(limit)
            
            return {
                "conversation_id": conversation_id,
                "title": conversation["title"],
                "message_count": len(messages),
                "messages": [
                    {
                        "message_id": m["message_id"],
                        "sender": m["sender"],
                        "text": m["text"],
                        "timestamp": m["timestamp"]
                    }
                    for m in messages
                ]
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting conversation history: {e}")
            raise

    async def list_conversations(self, user_id: str, page: int = 1, limit: int = 20):
        """List user's conversations"""
        try:
            filters = {"user_id": ObjectId(user_id)}
            
            total = await self.conversations_collection.count_documents(filters)
            
            conversations = await self.conversations_collection.find(filters)\
                .skip((page - 1) * limit)\
                .limit(limit)\
                .sort("updated_at", -1)\
                .to_list(limit)
            
            return {
                "total": total,
                "page": page,
                "limit": limit,
                "conversations": [
                    {
                        "conversation_id": c["conversation_id"],
                        "title": c["title"],
                        "message_count": c["message_count"],
                        "created_at": c["created_at"],
                        "updated_at": c["updated_at"]
                    }
                    for c in conversations
                ]
            }
        except Exception as e:
            logger.error(f"Error listing conversations: {e}")
            raise

    async def delete_conversation(self, conversation_id: str, user_id: str):
        """Delete conversation"""
        try:
            await self.messages_collection.delete_many(
                {"conversation_id": conversation_id}
            )
            
            result = await self.conversations_collection.delete_one({
                "conversation_id": conversation_id,
                "user_id": ObjectId(user_id)
            })
            
            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            
            logger.info(f"Conversation deleted: {conversation_id}")
            return True
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting conversation: {e}")
            raise

    async def _get_ai_response(
        self, 
        message: str, 
        conversation_id: str,
        context: Optional[Dict] = None
    ) -> tuple[str, int]:
        """Get AI response using LangChain with integrated data sources"""
        try:
            tokens_used = 0
            
            if self.llm:
                # Get recent conversation history
                recent_messages = await self.messages_collection.find(
                    {"conversation_id": conversation_id}
                ).sort("timestamp", -1).limit(10).to_list(10)
                
                # Build conversation context
                conv_context = "\n".join([
                    f"{m['sender']}: {m['text']}"
                    for m in reversed(recent_messages)
                ])
                
                # Build data context from sources
                data_context = ""
                if context and context.get("data"):
                    data_context = f"\n\nRelevant Data:\n{context['data']}"
                
                # System prompt for child protection context
                system_prompt = """You are an AI assistant for a child protection platform in Kenya. 
You have access to:
- Case data from the Kenya Child Protection API
- Scraped web data about child violence indicators
- Uploaded documents (reports, policies, caregiver reports) with RAG capabilities
- Analytics and reports

Provide helpful, accurate responses about child protection issues, statistics, and guidance. 
Use the provided document context when available and cite sources.
Be empathetic and professional. If you don't have specific data, acknowledge that clearly.
"""
                
                user_prompt = f"""Conversation History:
{conv_context}

Current Question: {message}
{data_context}

Please provide a helpful response based on the available data and context."""
                
                try:
                    messages = [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=user_prompt)
                    ]
                    
                    response = self.llm.invoke(messages)
                    response_text = response.content if hasattr(response, 'content') else str(response)
                    
                    # Estimate tokens (rough approximation: 1 token ≈ 4 characters)
                    tokens_used = (len(system_prompt) + len(user_prompt) + len(response_text)) // 4
                    
                    return response_text, tokens_used
                    
                except Exception as e:
                    logger.warning(f"LLM error: {e}. Using default response.")
                    return "Thank you for your message. Our team is here to help with child protection services. Could you provide more details about your inquiry?", 0
            else:
                return "Thank you for your message. Our team is here to help with child protection services. The AI assistant is currently unavailable.", 0
                
        except Exception as e:
            logger.error(f"Error getting AI response: {e}")
            return "I apologize for the error. Please try again.", 0
    
    async def _gather_context(self, message: str) -> Dict:
        """Gather relevant context from various data sources including uploaded documents"""
        context = {
            "data": "",
            "sources": []
        }
        
        try:
            message_lower = message.lower()
            
            # Search uploaded documents using PostgreSQL vector search
            if self.rag_available:
                try:
                    # Generate embedding for the query
                    query_embedding = await self.embedding_service.embed_text(message)
                    
                    # Search for relevant document chunks
                    doc_results = await self.vector_service.search_similar_chunks(
                        query_embedding,
                        top_k=3
                    )
                    
                    if doc_results:
                        context["data"] += "\n\nRelevant Document Content:\n"
                        for i, result in enumerate(doc_results, 1):
                            context["data"] += f"\n[Source {i} - {result['metadata'].get('file_name', 'Unknown')} (Score: {result['score']:.2f})]:\n{result['text']}\n"
                        context["sources"].append("uploaded_documents")
                        logger.info(f"Found {len(doc_results)} relevant document chunks")
                except Exception as e:
                    logger.error(f"Error searching documents: {e}")
            
            # Check if query is about statistics or data
            if any(word in message_lower for word in ["how many", "statistics", "data", "cases", "reports", "county"]):
                # Get recent case statistics
                case_stats = await self._get_case_statistics()
                if case_stats:
                    context["data"] += f"\n\nCase Statistics:\n{case_stats}"
                    context["sources"].append("cases")
                
                # Check Kenya API data
                kenya_stats = await self._get_kenya_data_summary()
                if kenya_stats:
                    context["data"] += f"\n\nKenya API Data:\n{kenya_stats}"
                    context["sources"].append("kenya_api")
            
            # Check if query is about recent incidents or news
            if any(word in message_lower for word in ["recent", "news", "latest", "incident"]):
                scraped_data = await self._get_recent_scraped_data()
                if scraped_data:
                    context["data"] += f"\n\nRecent Information:\n{scraped_data}"
                    context["sources"].append("web_scraping")
            
            return context
            
        except Exception as e:
            logger.error(f"Error gathering context: {e}")
            return context
    
    async def _get_case_statistics(self) -> str:
        """Get summary statistics from cases"""
        try:
            pipeline = [
                {
                    "$facet": {
                        "total": [{"$count": "count"}],
                        "by_county": [
                            {"$group": {"_id": "$county", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                            {"$limit": 5}
                        ],
                        "by_abuse_type": [
                            {"$group": {"_id": "$abuse_type", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}},
                            {"$limit": 5}
                        ]
                    }
                }
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(1)
            
            if results and results[0]:
                data = results[0]
                total = data["total"][0]["count"] if data["total"] else 0
                
                summary = f"Total cases: {total}\n"
                
                if data["by_county"]:
                    summary += "Top counties: " + ", ".join([
                        f"{c['_id']} ({c['count']})" for c in data["by_county"][:3]
                    ]) + "\n"
                
                if data["by_abuse_type"]:
                    summary += "Top abuse types: " + ", ".join([
                        f"{c['_id']} ({c['count']})" for c in data["by_abuse_type"][:3]
                    ])
                
                return summary
            
            return ""
            
        except Exception as e:
            logger.error(f"Error getting case statistics: {e}")
            return ""
    
    async def _get_kenya_data_summary(self) -> str:
        """Get summary of Kenya API data"""
        try:
            latest = await self.kenya_data_collection.find_one(
                {},
                sort=[("fetched_at", -1)]
            )
            
            if latest:
                return f"Kenya API data: {latest.get('record_count', 0)} records (Last updated: {latest['fetched_at'].strftime('%Y-%m-%d')})"
            
            return ""
            
        except Exception as e:
            logger.error(f"Error getting Kenya data summary: {e}")
            return ""
    
    async def _get_recent_scraped_data(self) -> str:
        """Get recent scraped data summary"""
        try:
            recent = await self.scraping_results_collection.find(
                {"status": "success"},
                sort=[("timestamp", -1)]
            ).limit(3).to_list(3)
            
            if recent:
                summary = "Recent scraped information: "
                summary += f"{len(recent)} recent sources analyzed"
                return summary
            
            return ""
            
        except Exception as e:
            logger.error(f"Error getting scraped data: {e}")
            return ""
    
    async def _track_token_usage(self, user_id: str, tokens: int):
        """Track token usage for monitoring"""
        try:
            await self.token_usage_collection.insert_one({
                "user_id": ObjectId(user_id),
                "tokens": tokens,
                "timestamp": datetime.now(timezone.utc)
            })
        except Exception as e:
            logger.error(f"Error tracking token usage: {e}")
    
    async def get_token_usage_stats(self, user_id: Optional[str] = None) -> Dict:
        """Get token usage statistics"""
        try:
            match_filter = {}
            if user_id:
                match_filter["user_id"] = ObjectId(user_id)
            
            pipeline = [
                {"$match": match_filter},
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$timestamp"
                            }
                        },
                        "total_tokens": {"$sum": "$tokens"},
                        "request_count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id": -1}},
                {"$limit": 30}
            ]
            
            daily_stats = await self.token_usage_collection.aggregate(pipeline).to_list(30)
            
            # Get overall totals
            total_pipeline = [
                {"$match": match_filter},
                {
                    "$group": {
                        "_id": None,
                        "total_tokens": {"$sum": "$tokens"},
                        "total_requests": {"$sum": 1}
                    }
                }
            ]
            
            totals = await self.token_usage_collection.aggregate(total_pipeline).to_list(1)
            
            return {
                "daily_usage": [
                    {
                        "date": stat["_id"],
                        "tokens": stat["total_tokens"],
                        "requests": stat["request_count"]
                    }
                    for stat in daily_stats
                ],
                "totals": totals[0] if totals else {"total_tokens": 0, "total_requests": 0}
            }
            
        except Exception as e:
            logger.error(f"Error getting token usage stats: {e}")
            raise
    
    async def get_chatbot_health(self) -> Dict:
        """Get chatbot system health status"""
        try:
            embedding_info = self.embedding_service.get_info() if self.embedding_service else {}
            
            return {
                "llm_available": self.llm_available,
                "provider": "groq" if self.llm_available else "none",
                "model": "llama-3.1-8b-instant" if self.llm_available else "none",
                "rag_available": self.rag_available,
                "embedding_provider": embedding_info.get("provider", "none"),
                "embedding_model": embedding_info.get("model", "none"),
                "embedding_dimension": embedding_info.get("dimension", 0),
                "data_sources": {
                    "cases": await self.cases_collection.count_documents({}),
                    "kenya_api_data": await self.kenya_data_collection.count_documents({}),
                    "scraping_results": await self.scraping_results_collection.count_documents({}),
                    "reports": await self.reports_collection.count_documents({}),
                    "uploaded_files": await self.files_collection.count_documents({})
                },
                "vector_db_stats": await self.vector_service.get_index_stats() if self.rag_available else {},
                "status": "healthy" if self.llm_available and self.rag_available else "degraded"
            }
        except Exception as e:
            logger.error(f"Error getting chatbot health: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
