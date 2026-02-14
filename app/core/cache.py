"""
In-memory caching for frequently accessed data
"""
from datetime import datetime, timedelta
from typing import Any, Optional
import hashlib
import json
from app.core.logging import logger


class SimpleCache:
    """Simple in-memory cache with TTL"""
    
    def __init__(self, ttl: int = 300):
        self._cache = {}
        self._ttl = ttl
    
    def _generate_key(self, prefix: str, **kwargs) -> str:
        """Generate cache key from parameters"""
        key_data = f"{prefix}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self._cache:
            value, expiry = self._cache[key]
            if datetime.now() < expiry:
                return value
            else:
                # Remove expired entry
                del self._cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set cache value with TTL"""
        expiry = datetime.now() + timedelta(seconds=ttl or self._ttl)
        self._cache[key] = (value, expiry)
    
    def invalidate(self, pattern: Optional[str] = None):
        """Invalidate cache entries matching pattern"""
        if pattern:
            keys_to_delete = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_delete:
                del self._cache[key]
            logger.info(f"Invalidated {len(keys_to_delete)} cache entries matching '{pattern}'")
        else:
            self._cache.clear()
            logger.info("Cache cleared completely")
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self._cache)


# Global cache instance
cache = SimpleCache(ttl=300)
