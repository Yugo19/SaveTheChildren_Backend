import hashlib
import secrets
import string
from typing import Any, Dict, List, Optional, TypeVar
from datetime import datetime, timedelta, timezone
import json
from app.core.logging import logger

T = TypeVar('T')


class Helpers:
    """Common helper utilities"""
    
    @staticmethod
    def generate_random_string(length: int = 32, use_special: bool = False) -> str:
        """Generate random string"""
        chars = string.ascii_letters + string.digits
        if use_special:
            chars += string.punctuation
        
        return ''.join(secrets.choice(chars) for _ in range(length))
    
    @staticmethod
    def hash_string(value: str, algorithm: str = 'sha256') -> str:
        """Hash a string value"""
        if algorithm == 'sha256':
            return hashlib.sha256(value.encode()).hexdigest()
        elif algorithm == 'sha512':
            return hashlib.sha512(value.encode()).hexdigest()
        elif algorithm == 'md5':
            return hashlib.md5(value.encode()).hexdigest()
        else:
            logger.warning(f"Unknown hash algorithm: {algorithm}")
            return hashlib.sha256(value.encode()).hexdigest()
    
    @staticmethod
    def hash_file(file_path: str, algorithm: str = 'sha256') -> str:
        """Calculate file hash"""
        hash_obj = hashlib.new(algorithm)
        
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Error hashing file: {e}")
            raise
    
    @staticmethod
    def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """Flatten nested dictionary"""
        items = []
        
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(Helpers.flatten_dict(v, new_key, sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(
                            Helpers.flatten_dict(item, f"{new_key}[{i}]", sep).items()
                        )
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        
        return dict(items)
    
    @staticmethod
    def unflatten_dict(d: Dict[str, Any], sep: str = '.') -> Dict[str, Any]:
        """Unflatten dictionary"""
        result = {}
        
        for key, value in d.items():
            parts = key.split(sep)
            current = result
            
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            current[parts[-1]] = value
        
        return result
    
    @staticmethod
    def merge_dicts(dict1: Dict, dict2: Dict, recursive: bool = True) -> Dict:
        """Merge two dictionaries"""
        result = dict1.copy()
        
        for key, value in dict2.items():
            if recursive and isinstance(value, dict) and key in result:
                result[key] = Helpers.merge_dicts(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @staticmethod
    def chunk_list(lst: List[T], chunk_size: int) -> List[List[T]]:
        """Split list into chunks"""
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
    
    @staticmethod
    def flatten_list(lst: List[Any]) -> List[Any]:
        """Flatten nested list"""
        result = []
        
        for item in lst:
            if isinstance(item, list):
                result.extend(Helpers.flatten_list(item))
            else:
                result.append(item)
        
        return result
    
    @staticmethod
    def deduplicate_list(lst: List[T], key=None) -> List[T]:
        """Remove duplicates from list while preserving order"""
        seen = set()
        result = []
        
        for item in lst:
            identifier = key(item) if key else item
            
            if identifier not in seen:
                seen.add(identifier)
                result.append(item)
        
        return result
    
    @staticmethod
    def calculate_time_difference(start: datetime, end: datetime) -> Dict[str, int]:
        """Calculate difference between two timestamps"""
        diff = end - start
        
        days = diff.days
        seconds = diff.seconds
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        return {
            'days': days,
            'hours': hours,
            'minutes': minutes,
            'seconds': secs,
            'total_seconds': int(diff.total_seconds())
        }
    
    @staticmethod
    def get_future_date(days: int = 0, hours: int = 0, minutes: int = 0) -> datetime:
        """Get future date from now"""
        delta = timedelta(days=days, hours=hours, minutes=minutes)
        return datetime.now(timezone.utc) + delta
    
    @staticmethod
    def get_past_date(days: int = 0, hours: int = 0, minutes: int = 0) -> datetime:
        """Get past date from now"""
        delta = timedelta(days=days, hours=hours, minutes=minutes)
        return datetime.now(timezone.utc) - delta
    
    @staticmethod
    def sanitize_string(value: str, remove_special: bool = False) -> str:
        """Sanitize string by removing/replacing special characters"""
        if remove_special:
            # Keep only alphanumeric, spaces, and basic punctuation
            value = ''.join(c for c in value if c.isalnum() or c in ' -._')
        else:
            # Replace multiple spaces with single space
            value = ' '.join(value.split())
        
        return value.strip()
    
    @staticmethod
    def truncate_string(value: str, length: int = 100, suffix: str = '...') -> str:
        """Truncate string to max length"""
        if len(value) <= length:
            return value
        
        return value[:length - len(suffix)] + suffix
    
    @staticmethod
    def deep_get(d: Dict[str, Any], key_path: str, default: Any = None, sep: str = '.') -> Any:
        """Safely get nested dictionary value"""
        keys = key_path.split(sep)
        current = d
        
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default
            
            if current is None:
                return default
        
        return current
    
    @staticmethod
    def deep_set(d: Dict[str, Any], key_path: str, value: Any, sep: str = '.') -> Dict[str, Any]:
        """Safely set nested dictionary value"""
        keys = key_path.split(sep)
        current = d
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
        return d
    
    @staticmethod
    def safe_json_dumps(obj: Any, default_on_error: str = "{}") -> str:
        """Safely serialize object to JSON"""
        try:
            return json.dumps(obj, default=str)
        except Exception as e:
            logger.error(f"Error serializing to JSON: {e}")
            return default_on_error
    
    @staticmethod
    def safe_json_loads(json_str: str, default_on_error: Any = None) -> Any:
        """Safely deserialize JSON string"""
        try:
            return json.loads(json_str)
        except Exception as e:
            logger.error(f"Error deserializing JSON: {e}")
            return default_on_error or {}
    
    @staticmethod
    def get_size_in_human_format(size_bytes: int) -> str:
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        
        return f"{size_bytes:.2f} PB"
    
    @staticmethod
    def retry_on_exception(
        func,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 1.0,
        exceptions: tuple = (Exception,)
    ):
        """Retry function on exception with exponential backoff"""
        import time
        
        attempt = 0
        current_delay = delay
        
        while attempt < max_attempts:
            try:
                return func()
            except exceptions as e:
                attempt += 1
                
                if attempt >= max_attempts:
                    logger.error(f"Max retries exceeded: {e}")
                    raise
                
                logger.warning(f"Attempt {attempt} failed, retrying in {current_delay}s: {e}")
                time.sleep(current_delay)
                current_delay *= backoff
    
    @staticmethod
    def group_by(items: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict]]:
        """Group list items by a dictionary key"""
        result = {}
        
        for item in items:
            group_key = item.get(key)
            
            if group_key not in result:
                result[group_key] = []
            
            result[group_key].append(item)
        
        return result
