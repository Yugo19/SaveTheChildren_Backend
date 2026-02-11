from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
from decimal import Decimal
import json
from app.core.logging import logger


class Formatters:
    """Data formatting utilities"""
    
    @staticmethod
    def format_currency(amount: Union[int, float, Decimal], currency: str = "USD") -> str:
        """Format amount as currency"""
        try:
            amount = float(amount)
            
            currency_symbols = {
                "USD": "$",
                "EUR": "€",
                "GBP": "£",
                "JPY": "¥",
                "KES": "KSh",
                "INR": "₹"
            }
            
            symbol = currency_symbols.get(currency, currency)
            return f"{symbol} {amount:,.2f}"
        except Exception as e:
            logger.error(f"Error formatting currency: {e}")
            return str(amount)
    
    @staticmethod
    def format_percentage(value: Union[int, float], decimal_places: int = 2) -> str:
        """Format value as percentage"""
        try:
            percentage = float(value) * 100
            return f"{percentage:.{decimal_places}f}%"
        except Exception as e:
            logger.error(f"Error formatting percentage: {e}")
            return str(value)
    
    @staticmethod
    def format_number(value: Union[int, float], decimal_places: int = 2, use_separator: bool = True) -> str:
        """Format number with optional thousand separator"""
        try:
            if use_separator:
                return f"{float(value):,.{decimal_places}f}"
            else:
                return f"{float(value):.{decimal_places}f}"
        except Exception as e:
            logger.error(f"Error formatting number: {e}")
            return str(value)
    
    @staticmethod
    def format_datetime(
        dt: Union[datetime, str],
        format: str = "%Y-%m-%d %H:%M:%S"
    ) -> str:
        """Format datetime object"""
        try:
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            
            return dt.strftime(format)
        except Exception as e:
            logger.error(f"Error formatting datetime: {e}")
            return str(dt)
    
    @staticmethod
    def format_date(dt: Union[datetime, str], format: str = "%Y-%m-%d") -> str:
        """Format date only"""
        return Formatters.format_datetime(dt, format)
    
    @staticmethod
    def format_time(dt: Union[datetime, str], format: str = "%H:%M:%S") -> str:
        """Format time only"""
        return Formatters.format_datetime(dt, format)
    
    @staticmethod
    def format_relative_time(dt: Union[datetime, str]) -> str:
        """Format datetime as relative time (e.g., '2 hours ago')"""
        try:
            if isinstance(dt, str):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            
            now = datetime.now(timezone.utc)
            diff = now - dt
            
            seconds = diff.total_seconds()
            
            if seconds < 60:
                return f"{int(seconds)} seconds ago"
            elif seconds < 3600:
                minutes = int(seconds / 60)
                return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
            elif seconds < 86400:
                hours = int(seconds / 3600)
                return f"{hours} hour{'s' if hours > 1 else ''} ago"
            elif seconds < 604800:
                days = int(seconds / 86400)
                return f"{days} day{'s' if days > 1 else ''} ago"
            elif seconds < 2592000:
                weeks = int(seconds / 604800)
                return f"{weeks} week{'s' if weeks > 1 else ''} ago"
            else:
                months = int(seconds / 2592000)
                return f"{months} month{'s' if months > 1 else ''} ago"
        except Exception as e:
            logger.error(f"Error formatting relative time: {e}")
            return str(dt)
    
    @staticmethod
    def format_bytes(bytes_size: int) -> str:
        """Format bytes as human readable (B, KB, MB, GB, etc.)"""
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        size = float(bytes_size)
        
        for unit in units:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        
        return f"{size:.2f} PB"
    
    @staticmethod
    def format_phone(phone: str, format_type: str = "international") -> str:
        """Format phone number"""
        # Remove all non-digit characters
        digits = ''.join(c for c in phone if c.isdigit())
        
        if format_type == "international":
            # +1-XXX-XXX-XXXX
            if len(digits) >= 10:
                if len(digits) == 10:
                    return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
                elif len(digits) == 11:
                    return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:11]}"
        elif format_type == "local":
            # (XXX) XXX-XXXX
            if len(digits) == 10:
                return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
        
        return phone
    
    @staticmethod
    def format_address(
        street: str,
        city: str,
        state: str = "",
        postal_code: str = "",
        country: str = ""
    ) -> str:
        """Format address"""
        parts = [street, city]
        
        if state:
            parts.append(state)
        
        if postal_code:
            parts.append(postal_code)
        
        if country:
            parts.append(country)
        
        return ", ".join(p.strip() for p in parts if p.strip())
    
    @staticmethod
    def format_name(first_name: str, last_name: str, name_format: str = "full") -> str:
        """Format person name"""
        first = first_name.strip() if first_name else ""
        last = last_name.strip() if last_name else ""
        
        if name_format == "full":
            return f"{first} {last}".strip()
        elif name_format == "last_first":
            return f"{last}, {first}".strip().rstrip(',')
        elif name_format == "initials":
            return f"{first[0] if first else ''}{last[0] if last else ''}".upper()
        elif name_format == "first_only":
            return first
        elif name_format == "last_only":
            return last
        
        return f"{first} {last}".strip()
    
    @staticmethod
    def format_table(
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None,
        include_index: bool = False
    ) -> str:
        """Format data as ASCII table"""
        if not data:
            return "No data"
        
        # Use provided headers or extract from first dict
        if not headers:
            headers = list(data[0].keys())
        
        # Calculate column widths
        col_widths = {header: len(str(header)) for header in headers}
        
        for row in data:
            for header in headers:
                value = str(row.get(header, ''))
                col_widths[header] = max(col_widths[header], len(value))
        
        # Build table
        lines = []
        separator = '+' + '+'.join(
            '-' * (col_widths[h] + 2) for h in headers
        ) + '+'
        
        lines.append(separator)
        
        # Header row
        header_row = '|' + '|'.join(
            f" {str(h):<{col_widths[h]}} " for h in headers
        ) + '|'
        lines.append(header_row)
        lines.append(separator)
        
        # Data rows
        for i, row in enumerate(data):
            row_values = []
            for header in headers:
                value = str(row.get(header, ''))
                row_values.append(f" {value:<{col_widths[header]}} ")
            
            row_str = '|' + '|'.join(row_values) + '|'
            
            if include_index:
                row_str = f"| {i:<2} " + row_str[1:]
            
            lines.append(row_str)
        
        lines.append(separator)
        return '\n'.join(lines)
    
    @staticmethod
    def format_json(data: Any, indent: int = 2, sort_keys: bool = True) -> str:
        """Format data as pretty JSON"""
        try:
            return json.dumps(data, indent=indent, sort_keys=sort_keys, default=str)
        except Exception as e:
            logger.error(f"Error formatting JSON: {e}")
            return str(data)
    
    @staticmethod
    def format_csv_row(values: List[Any], delimiter: str = ",", quote_all: bool = False) -> str:
        """Format values as CSV row"""
        def quote_value(val):
            val_str = str(val)
            needs_quote = quote_all or delimiter in val_str or '"' in val_str or '\n' in val_str
            
            if needs_quote:
                # Escape quotes by doubling them
                val_str = val_str.replace('"', '""')
                return f'"{val_str}"'
            return val_str
        
        return delimiter.join(quote_value(v) for v in values)
    
    @staticmethod
    def format_enum_name(value: str) -> str:
        """Format enum name to readable format"""
        # Convert SCREAMING_SNAKE_CASE to Readable Name
        return value.lower().replace('_', ' ').title()
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
        """Truncate text to max length"""
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)].rstrip() + suffix
    
    @staticmethod
    def format_slug(text: str) -> str:
        """Convert text to URL-friendly slug"""
        import re
        
        # Convert to lowercase
        slug = text.lower()
        
        # Replace spaces and underscores with hyphens
        slug = re.sub(r'[\s_]+', '-', slug)
        
        # Remove all non-alphanumeric characters except hyphens
        slug = re.sub(r'[^a-z0-9\-]', '', slug)
        
        # Remove multiple consecutive hyphens
        slug = re.sub(r'-+', '-', slug)
        
        # Strip hyphens from start and end
        slug = slug.strip('-')
        
        return slug
    
    @staticmethod
    def format_status(status: str, style: str = "badge") -> str:
        """Format status string with styling markers"""
        status_colors = {
            "active": "✓ Active",
            "inactive": "✗ Inactive",
            "pending": "⏳ Pending",
            "completed": "✓ Completed",
            "failed": "✗ Failed",
            "warning": "⚠ Warning",
            "error": "✗ Error",
            "success": "✓ Success",
        }
        
        return status_colors.get(status.lower(), status)
