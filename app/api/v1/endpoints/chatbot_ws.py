from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, status
from typing import Dict, Optional
from app.db.client import get_database
from app.services.chatbot_service import ChatbotService
from app.core.security import verify_token
from app.core.logging import logger
from datetime import datetime
import json

router = APIRouter(prefix="/chatbot", tags=["Chatbot WebSocket"])


class ConnectionManager:
    """Manage WebSocket connections"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, conversation_id: str, user_id: str):
        """Accept and register a WebSocket connection"""
        await websocket.accept()
        connection_key = f"{user_id}:{conversation_id}"
        self.active_connections[connection_key] = websocket
        logger.info(f"WebSocket connected: {connection_key}")
    
    def disconnect(self, conversation_id: str, user_id: str):
        """Remove a WebSocket connection"""
        connection_key = f"{user_id}:{conversation_id}"
        if connection_key in self.active_connections:
            del self.active_connections[connection_key]
            logger.info(f"WebSocket disconnected: {connection_key}")
    
    async def send_message(self, websocket: WebSocket, message: dict):
        """Send a JSON message to a WebSocket client"""
        await websocket.send_json(message)
    
    async def send_text_chunk(self, websocket: WebSocket, chunk: str):
        """Send a text chunk for streaming"""
        await websocket.send_json({
            "type": "chunk",
            "data": chunk,
            "timestamp": datetime.utcnow().isoformat()
        })


manager = ConnectionManager()


@router.get("/ws/info", summary="WebSocket Connection Info", tags=["Chatbot WebSocket"])
async def websocket_info():
    """
    Get information about the WebSocket endpoint for chatbot conversations.
    
    The WebSocket endpoint provides real-time streaming responses with lower latency.
    
    **Connection URL:** `ws://your-host/api/v1/chatbot/ws/{conversation_id}?token=YOUR_JWT_TOKEN`
    
    **Benefits:**
    - Real-time streaming responses (token-by-token)
    - Lower latency for multi-turn conversations
    - Persistent connection (no repeated auth overhead)
    - Better UX with typing indicators
    
    **Client Message Format:**
    ```json
    {
        "type": "message",
        "text": "your question here"
    }
    ```
    
    **Server Response Types:**
    - `connected`: Connection established
    - `start`: Processing started
    - `chunk`: Streaming response chunks
    - `complete`: Response complete with full data
    - `error`: Error occurred
    - `pong`: Heartbeat response
    
    See the WebSocket demo at `/websocket_demo.html` for example usage.
    """
    return {
        "protocol": "WebSocket",
        "endpoint": "/api/v1/chatbot/ws/{conversation_id}",
        "authentication": "Query parameter: ?token=JWT_TOKEN",
        "supported_message_types": ["message", "ping"],
        "response_types": ["connected", "start", "chunk", "complete", "error", "pong"],
        "example_connection": "ws://localhost:8000/api/v1/chatbot/ws/your-conversation-id?token=your-jwt-token",
        "required_roles": ["admin", "member"],
        "demo_page": "/websocket_demo.html"
    }


@router.websocket("/ws/{conversation_id}")
async def websocket_chatbot(
    websocket: WebSocket,
    conversation_id: str,
    token: str = Query(..., description="Authentication token"),
    db=Depends(get_database)
):
    """
    WebSocket endpoint for real-time chatbot conversations
    
    Benefits over HTTP:
    - Real-time streaming responses (token-by-token)
    - Lower latency for multi-turn conversations
    - Persistent connection (no repeated auth overhead)
    - Better UX with typing indicators
    - Efficient for long-running AI responses
    
    Connection:
    - Client connects with: ws://host/api/v1/chatbot/ws/{conversation_id}?token=JWT_TOKEN
    
    Message Format (Client to Server):
    {
        "type": "message",
        "text": "user message here"
    }
    
    Response Format (Server to Client):
    {
        "type": "start",  // Message processing started
        "message_id": "uuid"
    }
    {
        "type": "chunk",  // Streaming token chunks
        "data": "partial response text",
        "timestamp": "2024-01-01T00:00:00"
    }
    {
        "type": "complete",  // Message complete
        "message_id": "uuid",
        "full_text": "complete response",
        "context_sources": [...],
        "tokens_used": 150,
        "timestamp": "2024-01-01T00:00:00"
    }
    {
        "type": "error",  // Error occurred
        "error": "error message"
    }
    """
    
    # Authenticate user
    try:
        token_data = verify_token(token)
        user_id = token_data.user_id
        user_role = token_data.role
        
        # Check role permissions (Admin & Member only)
        if user_role not in ["admin", "member"]:
            logger.error(f"WebSocket access denied for role: {user_role}")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
            
    except Exception as e:
        logger.error(f"WebSocket authentication failed: {e}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    # Connect WebSocket
    await manager.connect(websocket, conversation_id, user_id)
    
    chatbot_service = ChatbotService(db)
    
    try:
        # Verify conversation exists and belongs to user
        conversation = await db.conversations.find_one({
            "conversation_id": conversation_id,
            "user_id": token_data.user_id_obj
        })
        
        if not conversation:
            await manager.send_message(websocket, {
                "type": "error",
                "error": "Conversation not found or access denied"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Send connection success
        await manager.send_message(websocket, {
            "type": "connected",
            "conversation_id": conversation_id,
            "title": conversation.get("title"),
            "message": "WebSocket connected successfully"
        })
        
        # Listen for messages
        while True:
            try:
                data = await websocket.receive_json()
                
                if data.get("type") == "message":
                    message_text = data.get("text", "").strip()
                    
                    if not message_text:
                        await manager.send_message(websocket, {
                            "type": "error",
                            "error": "Message text cannot be empty"
                        })
                        continue
                    
                    # Process message with streaming
                    await process_message_stream(
                        websocket=websocket,
                        chatbot_service=chatbot_service,
                        conversation_id=conversation_id,
                        user_id=user_id,
                        message_text=message_text
                    )
                
                elif data.get("type") == "ping":
                    # Heartbeat to keep connection alive
                    await manager.send_message(websocket, {
                        "type": "pong",
                        "timestamp": datetime.utcnow().isoformat()
                    })
                
                else:
                    await manager.send_message(websocket, {
                        "type": "error",
                        "error": f"Unknown message type: {data.get('type')}"
                    })
                    
            except json.JSONDecodeError:
                await manager.send_message(websocket, {
                    "type": "error",
                    "error": "Invalid JSON format"
                })
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for conversation {conversation_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await manager.send_message(websocket, {
                "type": "error",
                "error": "Internal server error"
            })
        except:
            pass
    finally:
        manager.disconnect(conversation_id, user_id)


async def process_message_stream(
    websocket: WebSocket,
    chatbot_service: ChatbotService,
    conversation_id: str,
    user_id: str,
    message_text: str
):
    """Process a message and stream the response"""
    try:
        # Send "processing started" message
        await manager.send_message(websocket, {
            "type": "start",
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Use the existing service method (for now)
        # TODO: Implement true streaming with LangChain streaming
        result = await chatbot_service.send_message(
            conversation_id=conversation_id,
            user_id=user_id,
            message_text=message_text
        )
        
        # Simulate streaming by sending chunks
        # In production, you'd modify the service to yield chunks
        ai_response = result["ai_response"]["text"]
        
        # Stream response in chunks (simulating token-by-token)
        words = ai_response.split()
        chunk_size = 5  # words per chunk
        
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i:i + chunk_size])
            if i + chunk_size < len(words):
                chunk += " "
            
            await manager.send_text_chunk(websocket, chunk)
            # Small delay to simulate streaming (remove in production with real streaming)
            import asyncio
            await asyncio.sleep(0.05)
        
        # Send complete message
        await manager.send_message(websocket, {
            "type": "complete",
            "user_message": result["user_message"],
            "ai_response": {
                "message_id": result["ai_response"]["message_id"],
                "full_text": ai_response,
                "timestamp": result["ai_response"]["timestamp"].isoformat(),
                "context_sources": result["ai_response"].get("context_sources", []),
                "tokens_used": result["ai_response"].get("tokens_used", 0)
            }
        })
        
    except Exception as e:
        logger.error(f"Error processing message stream: {e}")
        await manager.send_message(websocket, {
            "type": "error",
            "error": str(e)
        })
