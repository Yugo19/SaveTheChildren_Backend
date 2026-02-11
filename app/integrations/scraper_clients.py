import asyncio
from typing import Optional, Dict, List, Any
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup
import json
from app.core.logging import logger
from datetime import datetime, timezone


class ScraperClient:
    """Base class for web scraping with async support"""
    
    def __init__(self, timeout: int = 30, headers: Optional[Dict] = None):
        """
        Initialize scraper client
        
        Args:
            timeout: Request timeout in seconds
            headers: Custom headers for requests
        """
        self.timeout = timeout
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session: Optional[ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = TCPConnector(limit_per_host=5)
        self.session = ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML content from URL"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")
        
        try:
            async with self.session.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logger.warning(f"Failed to fetch {url}: status {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    async def fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch JSON content from URL"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")
        
        try:
            async with self.session.get(
                url,
                headers=self.headers,
                timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Failed to fetch JSON from {url}: status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching JSON from {url}: {e}")
            return None
    
    async def fetch_multiple(self, urls: List[str]) -> List[Optional[str]]:
        """Fetch HTML from multiple URLs concurrently"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")
        
        tasks = [self.fetch_html(url) for url in urls]
        return await asyncio.gather(*tasks)


class WebPageScraper(ScraperClient):
    """Scraper for extracting structured data from web pages"""
    
    async def extract_text(self, url: str, css_selector: Optional[str] = None) -> Optional[str]:
        """Extract text content from a URL"""
        try:
            html = await self.fetch_html(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            if css_selector:
                elements = soup.select(css_selector)
                text = '\n'.join([elem.get_text() for elem in elements])
            else:
                text = soup.get_text()
            
            return text.strip()
        except Exception as e:
            logger.error(f"Error extracting text from {url}: {e}")
            return None
    
    async def extract_links(self, url: str) -> List[Dict[str, str]]:
        """Extract all links from a page"""
        try:
            html = await self.fetch_html(url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            for a_tag in soup.find_all('a', href=True):
                links.append({
                    'text': a_tag.get_text().strip(),
                    'href': a_tag['href']
                })
            
            return links
        except Exception as e:
            logger.error(f"Error extracting links from {url}: {e}")
            return []
    
    async def extract_tables(self, url: str) -> List[List[Dict]]:
        """Extract all tables from a page"""
        try:
            html = await self.fetch_html(url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            tables = []
            
            for table in soup.find_all('table'):
                headers = []
                rows = []
                
                # Extract headers
                for th in table.find_all('th'):
                    headers.append(th.get_text().strip())
                
                # Extract rows
                for tr in table.find_all('tr')[1:]:  # Skip header row
                    cells = [td.get_text().strip() for td in tr.find_all('td')]
                    if headers:
                        row_dict = dict(zip(headers, cells))
                    else:
                        row_dict = {'columns': cells}
                    rows.append(row_dict)
                
                tables.append(rows)
            
            return tables
        except Exception as e:
            logger.error(f"Error extracting tables from {url}: {e}")
            return []
    
    async def extract_metadata(self, url: str) -> Dict[str, Any]:
        """Extract metadata (title, description, etc.) from a page"""
        try:
            html = await self.fetch_html(url)
            if not html:
                return {}
            
            soup = BeautifulSoup(html, 'html.parser')
            metadata = {
                'url': url,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Title
            title = soup.find('title')
            if title:
                metadata['title'] = title.get_text()
            
            # Meta description
            description = soup.find('meta', attrs={'name': 'description'})
            if description:
                metadata['description'] = description.get('content', '')
            
            # Open Graph tags
            og_title = soup.find('meta', attrs={'property': 'og:title'})
            if og_title:
                metadata['og_title'] = og_title.get('content', '')
            
            og_description = soup.find('meta', attrs={'property': 'og:description'})
            if og_description:
                metadata['og_description'] = og_description.get('content', '')
            
            og_image = soup.find('meta', attrs={'property': 'og:image'})
            if og_image:
                metadata['og_image'] = og_image.get('content', '')
            
            return metadata
        except Exception as e:
            logger.error(f"Error extracting metadata from {url}: {e}")
            return {}


class APIScraper(ScraperClient):
    """Scraper for consuming REST APIs"""
    
    async def get(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """GET request to API"""
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            async with self.session.get(
                url,
                params=params,
                headers=self.headers,
                timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return await response.json()
                logger.warning(f"API GET failed: {url}, status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error in API GET request: {e}")
            return None
    
    async def post(
        self,
        url: str,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Optional[Dict]:
        """POST request to API"""
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            async with self.session.post(
                url,
                data=data,
                json=json_data,
                headers=self.headers,
                timeout=self.timeout
            ) as response:
                if response.status in [200, 201]:
                    return await response.json()
                logger.warning(f"API POST failed: {url}, status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error in API POST request: {e}")
            return None
    
    async def paginated_get(
        self,
        url: str,
        param_name: str = "page",
        start_page: int = 1,
        max_pages: Optional[int] = None
    ) -> List[Dict]:
        """Fetch paginated API data"""
        try:
            all_data = []
            page = start_page
            
            while True:
                if max_pages and page - start_page >= max_pages:
                    break
                
                params = {param_name: page}
                response = await self.get(url, params)
                
                if not response:
                    break
                
                if isinstance(response, list):
                    all_data.extend(response)
                elif isinstance(response, dict):
                    # Handle different API response formats
                    if 'data' in response:
                        all_data.extend(response['data'])
                    elif 'results' in response:
                        all_data.extend(response['results'])
                    else:
                        all_data.append(response)
                
                page += 1
            
            return all_data
        except Exception as e:
            logger.error(f"Error in paginated API request: {e}")
            return []
