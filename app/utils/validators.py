import re
from typing import Any, Optional, List
from datetime import datetime
from email_validator import validate_email, EmailNotValidError
from app.core.logging import logger


class ValidatorError(Exception):
    """Custom exception for validation errors"""
    pass


class Validators:
    """Common validation utilities"""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email address"""
        try:
            validate_email(email)
            return True
        except EmailNotValidError as e:
            logger.warning(f"Invalid email: {email}")
            return False
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Validate phone number (international format)"""
        # Remove common formatting characters
        clean_phone = re.sub(r'[\s\-\(\)\.]+', '', phone)
        
        # Check if it's 10-15 digits
        if re.match(r'^\+?1?\d{9,14}$', clean_phone):
            return True
        
        logger.warning(f"Invalid phone: {phone}")
        return False
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format"""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if url_pattern.match(url):
            return True
        
        logger.warning(f"Invalid URL: {url}")
        return False
    
    @staticmethod
    def validate_uuid(uuid_str: str) -> bool:
        """Validate UUID format"""
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            re.IGNORECASE
        )
        return bool(uuid_pattern.match(uuid_str))
    
    @staticmethod
    def validate_date(date_str: str, format: str = "%Y-%m-%d") -> bool:
        """Validate date string against format"""
        try:
            datetime.strptime(date_str, format)
            return True
        except ValueError:
            logger.warning(f"Invalid date format: {date_str}")
            return False
    
    @staticmethod
    def validate_iso_date(date_str: str) -> bool:
        """Validate ISO 8601 date format"""
        try:
            datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return True
        except ValueError:
            logger.warning(f"Invalid ISO date: {date_str}")
            return False
    
    @staticmethod
    def validate_string_length(value: str, min_length: int = 1, max_length: int = 255) -> bool:
        """Validate string length"""
        if not isinstance(value, str):
            return False
        
        if len(value) < min_length or len(value) > max_length:
            logger.warning(f"String length out of range: {len(value)}")
            return False
        
        return True
    
    @staticmethod
    def validate_integer_range(value: Any, min_val: int = 0, max_val: int = 100) -> bool:
        """Validate integer within range"""
        try:
            int_val = int(value)
            if min_val <= int_val <= max_val:
                return True
            logger.warning(f"Integer out of range: {int_val}")
            return False
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_enum(value: str, allowed_values: List[str]) -> bool:
        """Validate value against allowed list"""
        if value in allowed_values:
            return True
        logger.warning(f"Invalid enum value: {value}")
        return False
    
    @staticmethod
    def validate_not_empty(value: Any) -> bool:
        """Validate value is not empty/null"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            logger.warning("Value is empty or None")
            return False
        return True
    
    @staticmethod
    def validate_json(value: str) -> bool:
        """Validate JSON string"""
        import json
        try:
            json.loads(value)
            return True
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid JSON")
            return False
    
    @staticmethod
    def validate_password_strength(password: str, min_length: int = 8) -> tuple[bool, str]:
        """
        Validate password strength
        
        Returns:
            Tuple of (is_valid, message)
        """
        if len(password) < min_length:
            return False, f"Password must be at least {min_length} characters"
        
        if not re.search(r'[A-Z]', password):
            return False, "Password must contain at least one uppercase letter"
        
        if not re.search(r'[a-z]', password):
            return False, "Password must contain at least one lowercase letter"
        
        if not re.search(r'\d', password):
            return False, "Password must contain at least one digit"
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character"
        
        return True, "Password is strong"
    
    @staticmethod
    def validate_mongo_id(mongo_id: str) -> bool:
        """Validate MongoDB ObjectId format"""
        if len(mongo_id) != 24:
            return False
        
        try:
            int(mongo_id, 16)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_latitude_longitude(lat: float, lon: float) -> bool:
        """Validate latitude and longitude"""
        try:
            lat = float(lat)
            lon = float(lon)
            
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return True
            
            logger.warning(f"Invalid coordinates: {lat}, {lon}")
            return False
        except (ValueError, TypeError):
            return False
