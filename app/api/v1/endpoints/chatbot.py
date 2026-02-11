from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import Optional
from app.db.client import get_database
from app.services.chatbot_service import ChatbotService
from app.core.security import get_current_user, TokenData
from app.core.logging import logger

router = APIRouter(prefix="/chatbot", tags=["Chatbot"])


class CreateConversationRequest(BaseModel):
    title: Optional[str] = None


class SendMessageRequest(BaseModel):
    message: str


@router.post("/conversations")
async def create_conversation(
    request: CreateConversationRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Create a new chat conversation"""
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.create_conversation(current_user.user_id, request.title)
    logger.info(f"Conversation created by {current_user.user_id}")
    return result


@router.get("/conversations")
async def list_conversations(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """List user's conversations"""
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.list_conversations(current_user.user_id, page, limit)
    logger.info(f"Conversations listed for {current_user.user_id}")
    return result


@router.get("/conversations/{conversation_id}")
async def get_conversation_history(
    conversation_id: str,
    limit: int = Query(50, ge=1, le=500),
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get conversation history"""
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.get_conversation_history(
        conversation_id,
        current_user.user_id,
        limit
    )
    logger.info(f"Conversation history retrieved for {current_user.user_id}")
    return result


@router.post("/conversations/{conversation_id}/message")
async def send_message(
    conversation_id: str,
    request: SendMessageRequest,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Send a message in a conversation"""
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.send_message(
        conversation_id,
        current_user.user_id,
        request.message
    )
    logger.info(f"Message sent by {current_user.user_id}")
    return result


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(
    conversation_id: str,
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Delete conversation"""
    chatbot_service = ChatbotService(db)
    await chatbot_service.delete_conversation(conversation_id, current_user.user_id)
    logger.info(f"Conversation deleted by {current_user.user_id}")
    return {"message": "Conversation deleted successfully"}


@router.get("/token-usage")
async def get_token_usage(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get token usage statistics for monitoring
    
    Returns:
    - Daily token usage for the last 30 days
    - Total tokens used
    - Total requests made
    """
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.get_token_usage_stats(current_user.user_id)
    logger.info(f"Token usage stats retrieved for {current_user.user_id}")
    return result


@router.get("/health")
async def get_chatbot_health(
    current_user: TokenData = Depends(get_current_user),
    db=Depends(get_database)
):
    """Get chatbot system health status
    
    Returns:
    - LLM availability status
    - Connected data sources counts
    - Overall system health
    """
    chatbot_service = ChatbotService(db)
    result = await chatbot_service.get_chatbot_health()
    logger.info(f"Chatbot health checked by {current_user.user_id}")
    return result
