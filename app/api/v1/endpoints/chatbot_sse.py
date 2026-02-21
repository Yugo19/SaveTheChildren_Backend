from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import AsyncGenerator, Optional
from app.db.client import get_database
from app.services.chatbot_service import ChatbotService
from app.core.security import verify_token, TokenData
from app.core.logging import logger
from datetime import datetime
from pydantic import BaseModel
import json
import asyncio

router = APIRouter(prefix="/chatbot", tags=["Chatbot SSE"])

# Optional bearer token for backward compatibility
optional_bearer = HTTPBearer(auto_error=False)


class MessageRequest(BaseModel):
    text: str
    conversation_id: str


@router.get("/sse/info", summary="SSE Connection Info", tags=["Chatbot SSE"])
async def sse_info():
    """
    Get information about the SSE endpoint for chatbot conversations.
    
    SSE (Server-Sent Events) provides real-time streaming responses compatible with Vercel.
    
    **Connection:** `GET /api/v1/chatbot/sse/stream/{conversation_id}`
    
    **Benefits:**
    - Real-time streaming responses (token-by-token)
    - Compatible with Vercel serverless functions
    - Works over standard HTTP/HTTPS
    - Automatic reconnection support
    - Lower latency for AI responses
    
    **Message Sending:** `POST /api/v1/chatbot/sse/send`
    
    **Event Types:**
    - `connected`: Connection established
    - `start`: Processing started
    - `chunk`: Streaming response chunks
    - `complete`: Response complete with full data
    - `error`: Error occurred
    - `heartbeat`: Keep-alive ping
    """
    return {
        "protocol": "SSE (Server-Sent Events)",
        "stream_endpoint": "/api/v1/chatbot/sse/stream/{conversation_id}",
        "send_endpoint": "/api/v1/chatbot/sse/send",
        "authentication": "Bearer token in Authorization header",
        "event_types": ["connected", "start", "chunk", "complete", "error", "heartbeat"],
        "example_connection": "GET /api/v1/chatbot/sse/stream/your-conversation-id with Authorization: Bearer YOUR_TOKEN",
        "required_roles": ["admin", "member"],
        "vercel_compatible": True
    }


@router.get("/sse/stream/{conversation_id}")
async def sse_stream(
    conversation_id: str,
    token: Optional[str] = Query(None, description="Authentication token (alternative to Authorization header)"),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(optional_bearer),
    db=Depends(get_database)
):
    """
    SSE endpoint for real-time chatbot conversation streaming
    
    This endpoint provides Server-Sent Events for streaming AI responses.
    Compatible with Vercel and serverless deployments.
    
    **Authentication:** Supports both methods:
    1. Authorization header: `Authorization: Bearer YOUR_JWT_TOKEN`
    2. Query parameter: `?token=YOUR_JWT_TOKEN` (for EventSource compatibility)
    
    **Connection:**
    - Client connects with: GET /api/v1/chatbot/sse/stream/{conversation_id}?token=YOUR_JWT_TOKEN
    - OR include Authorization: Bearer YOUR_JWT_TOKEN header
    
    **Event Format:**
    ```
    event: chunk
    data: {"type": "chunk", "data": "partial response text", "timestamp": "2024-01-01T00:00:00"}
    
    event: complete
    data: {"type": "complete", "message_id": "uuid", "full_text": "...", ...}
    ```
    
    **Client Implementation (JavaScript):**
    ```javascript
    // Using query parameter (recommended for EventSource)
    const eventSource = new EventSource(
        '/api/v1/chatbot/sse/stream/conversation-id?token=' + token
    );
    
    eventSource.addEventListener('chunk', (e) => {
        const data = JSON.parse(e.data);
        console.log('Chunk:', data.data);
    });
    
    eventSource.addEventListener('complete', (e) => {
        const data = JSON.parse(e.data);
        console.log('Complete:', data.full_text);
    });
    ```
    """
    
    # Authenticate user - prioritize header auth, fallback to query param
    current_user = None
    auth_token = None
    
    # Try Authorization header first (priority for Next.js frontend)
    if credentials and credentials.credentials:
        auth_token = credentials.credentials
    # Fall back to query parameter (for EventSource compatibility)
    elif token:
        auth_token = token
    
    if not auth_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Provide token via Authorization header or ?token query parameter"
        )
    
    try:
        current_user = verify_token(auth_token)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )
    
    # Check role permissions
    if current_user.role not in ["admin", "member"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    
    # Verify conversation exists and belongs to user
    logger.info(f"Looking for conversation {conversation_id} for user {current_user.user_id} (ObjectId: {current_user.user_id_obj})")
    conversation = await db.conversations.find_one({
        "conversation_id": conversation_id,
        "user_id": current_user.user_id_obj
    })
    
    logger.info(f"Conversation found: {conversation is not None}")
    
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found or access denied"
        )
    
    async def event_generator() -> AsyncGenerator[str, None]:
        """Generate SSE events"""
        try:
            logger.info(f"Starting SSE stream for conversation {conversation_id}")
            
            # Send connected event immediately
            connected_event = {
                'type': 'connected',
                'conversation_id': conversation_id,
                'title': conversation.get('title', 'Untitled'),
                'timestamp': datetime.utcnow().isoformat()
            }
            yield f"event: connected\ndata: {json.dumps(connected_event)}\n\n"
            
            logger.info(f"Sent connected event for conversation {conversation_id}")
            
            # Keep connection alive with heartbeat
            heartbeat_interval = 5  # seconds - keep connection alive more frequently
            iteration_count = 0
            
            while True:
                iteration_count += 1
                
                # Send heartbeat to keep connection alive
                heartbeat_event = {
                    'type': 'heartbeat',
                    'timestamp': datetime.utcnow().isoformat()
                }
                yield f"event: heartbeat\ndata: {json.dumps(heartbeat_event)}\n\n"
                
                if iteration_count % 10 == 0:
                    logger.info(f"SSE heartbeat {iteration_count} sent for conversation {conversation_id}")
                
                # Wait before next heartbeat
                await asyncio.sleep(heartbeat_interval)
                
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for conversation {conversation_id}")
            raise
        except Exception as e:
            logger.error(f"SSE stream error: {e}", exc_info=True)
            error_event = {
                'type': 'error',
                'error': 'Stream error',
                'timestamp': datetime.utcnow().isoformat()
            }
            yield f"event: error\ndata: {json.dumps(error_event)}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
            "Access-Control-Allow-Origin": "*",  # Allow CORS for SSE
        }
    )


@router.post("/sse/send", summary="Send message via SSE")
async def sse_send_message(
    message_request: MessageRequest,
    token: Optional[str] = Query(None, description="Authentication token (alternative to Authorization header)"),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(optional_bearer),
    db=Depends(get_database)
):
    """
    Send a message and get streaming response via SSE
    
    This endpoint processes a message and returns a streaming response.
    Unlike WebSocket, this uses standard HTTP POST with SSE for response streaming.
    
    **Authentication:** Supports both methods:
    1. Authorization header: `Authorization: Bearer YOUR_JWT_TOKEN`
    2. Query parameter: `?token=YOUR_JWT_TOKEN`
    
    **Request Body:**
    ```json
    {
        "conversation_id": "conversation-uuid",
        "text": "your message here"
    }
    ```
    
    **Response:** Streaming SSE events
    - `start`: Processing started
    - `chunk`: Response chunks (multiple events)
    - `complete`: Final response with metadata
    """
    
    # Authenticate user - prioritize header auth, fallback to query param
    current_user = None
    auth_token = None
    
    # Try Authorization header first (priority for Next.js frontend)
    if credentials and credentials.credentials:
        auth_token = credentials.credentials
    # Fall back to query parameter (for EventSource compatibility)
    elif token:
        auth_token = token
    
    if not auth_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Provide token via Authorization header or ?token query parameter"
        )
    
    try:
        current_user = verify_token(auth_token)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token"
        )
    
    # Check role permissions
    if current_user.role not in ["admin", "member"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    
    # Verify conversation exists and belongs to user
    conversation = await db.conversations.find_one({
        "conversation_id": message_request.conversation_id,
        "user_id": current_user.user_id_obj
    })
    
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found or access denied"
        )
    
    if not message_request.text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Message text cannot be empty"
        )
    
    chatbot_service = ChatbotService(db)
    
    async def response_generator() -> AsyncGenerator[str, None]:
        """Generate streaming response"""
        try:
            # Send start event
            yield f"event: start\ndata: {json.dumps({'type': 'start', 'timestamp': datetime.utcnow().isoformat()})}\n\n"
            
            # Process message
            result = await chatbot_service.send_message(
                conversation_id=message_request.conversation_id,
                user_id=current_user.user_id,
                message_text=message_request.text
            )
            
            # Stream response in chunks
            ai_response = result["ai_response"]["text"]
            words = ai_response.split()
            chunk_size = 5  # words per chunk
            
            for i in range(0, len(words), chunk_size):
                chunk = " ".join(words[i:i + chunk_size])
                if i + chunk_size < len(words):
                    chunk += " "
                
                chunk_data = {
                    "type": "chunk",
                    "data": chunk,
                    "timestamp": datetime.utcnow().isoformat()
                }
                yield f"event: chunk\ndata: {json.dumps(chunk_data)}\n\n"
                
                # Small delay to simulate streaming
                await asyncio.sleep(0.05)
            
            # Send complete event
            complete_data = {
                "type": "complete",
                "user_message": {
                    "message_id": result["user_message"]["message_id"],
                    "text": result["user_message"]["text"],
                    "timestamp": result["user_message"]["timestamp"].isoformat()
                },
                "ai_response": {
                    "message_id": result["ai_response"]["message_id"],
                    "full_text": ai_response,
                    "timestamp": result["ai_response"]["timestamp"].isoformat(),
                    "context_sources": result["ai_response"].get("context_sources", []),
                    "tokens_used": result["ai_response"].get("tokens_used", 0)
                }
            }
            yield f"event: complete\ndata: {json.dumps(complete_data)}\n\n"
            
        except Exception as e:
            logger.error(f"Error in SSE response stream: {e}")
            error_data = {
                "type": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
            yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
    
    return StreamingResponse(
        response_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )
