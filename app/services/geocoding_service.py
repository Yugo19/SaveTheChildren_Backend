"""
Geocoding service to fetch coordinates for Kenyan counties and sub-counties.
Uses Nominatim (OpenStreetMap) - free, no API key required.
"""
from typing import Optional, Dict, List
import aiohttp
import asyncio
from app.core.logging import logger
from datetime import datetime, timezone


class GeocodingService:
    """Service to geocode Kenyan counties and sub-counties"""
    
    # Pre-defined coordinates for all 47 Kenyan counties (center points)
    KENYA_COUNTY_COORDS = {
        "Mombasa": {"lat": -4.0435, "lon": 39.6682},
        "Kwale": {"lat": -4.1833, "lon": 39.4500},
        "Kilifi": {"lat": -3.6307, "lon": 39.8493},
        "Tana River": {"lat": -1.8000, "lon": 39.9000},
        "Lamu": {"lat": -2.2717, "lon": 40.9020},
        "Taita Taveta": {"lat": -3.3167, "lon": 38.3500},
        "Garissa": {"lat": -0.4536, "lon": 39.6401},
        "Wajir": {"lat": 1.7471, "lon": 40.0573},
        "Mandera": {"lat": 3.9366, "lon": 41.8560},
        "Marsabit": {"lat": 2.3284, "lon": 37.9899},
        "Isiolo": {"lat": 0.3556, "lon": 37.5833},
        "Meru": {"lat": 0.0500, "lon": 37.6500},
        "Tharaka Nithi": {"lat": -0.2833, "lon": 37.7667},
        "Embu": {"lat": -0.5333, "lon": 37.4500},
        "Kitui": {"lat": -1.3667, "lon": 38.0167},
        "Machakos": {"lat": -1.5167, "lon": 37.2667},
        "Makueni": {"lat": -2.2667, "lon": 37.8333},
        "Nyandarua": {"lat": -0.1833, "lon": 36.4833},
        "Nyeri": {"lat": -0.4167, "lon": 36.9500},
        "Kirinyaga": {"lat": -0.6589, "lon": 37.3833},
        "Murang'a": {"lat": -0.7833, "lon": 37.0000},
        "Kiambu": {"lat": -1.1714, "lon": 36.8356},
        "Turkana": {"lat": 3.1167, "lon": 35.5989},
        "West Pokot": {"lat": 1.6189, "lon": 35.3667},
        "Samburu": {"lat": 1.2153, "lon": 36.9453},
        "Trans Nzoia": {"lat": 1.0500, "lon": 34.9500},
        "Uasin Gishu": {"lat": 0.5500, "lon": 35.3000},
        "Elgeyo Marakwet": {"lat": 0.8667, "lon": 35.4667},
        "Nandi": {"lat": 0.1833, "lon": 35.1167},
        "Baringo": {"lat": 0.8167, "lon": 36.0833},
        "Laikipia": {"lat": 0.3667, "lon": 36.7833},
        "Nakuru": {"lat": -0.3031, "lon": 36.0800},
        "Narok": {"lat": -1.0833, "lon": 35.8667},
        "Kajiado": {"lat": -2.0980, "lon": 36.7820},
        "Kericho": {"lat": -0.3667, "lon": 35.2833},
        "Bomet": {"lat": -0.8000, "lon": 35.3000},
        "Kakamega": {"lat": 0.2827, "lon": 34.7519},
        "Vihiga": {"lat": 0.0667, "lon": 34.7000},
        "Bungoma": {"lat": 0.5635, "lon": 34.5606},
        "Busia": {"lat": 0.4346, "lon": 34.1115},
        "Siaya": {"lat": -0.0500, "lon": 34.2833},
        "Kisumu": {"lat": -0.0917, "lon": 34.7680},
        "Homa Bay": {"lat": -0.5167, "lon": 34.4500},
        "Migori": {"lat": -1.0634, "lon": 34.4731},
        "Kisii": {"lat": -0.6770, "lon": 34.7800},
        "Nyamira": {"lat": -0.5667, "lon": 34.9333},
        "Nairobi": {"lat": -1.2921, "lon": 36.8219}
    }
    
    DEFAULT_COORDS = {"lat": -1.2921, "lon": 36.8219}  # Nairobi
    
    def __init__(self):
        self.cache: Dict[str, Dict] = {}
        self.last_request_time: Optional[datetime] = None
        
    async def geocode_location(
        self,
        county: str,
        sub_county: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Get coordinates for a Kenyan county/sub-county.
        Returns dict with 'lat' and 'lon' keys.
        """
        try:
            cache_key = f"{county}|{sub_county or 'center'}"
            
            # Check cache
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            # Use predefined county coordinates if no sub-county
            if not sub_county and county in self.KENYA_COUNTY_COORDS:
                coords = self.KENYA_COUNTY_COORDS[county]
                self.cache[cache_key] = coords
                return coords
            
            # Try geocoding sub-county via Nominatim
            if sub_county:
                coords = await self._geocode_nominatim(county, sub_county)
                if coords:
                    self.cache[cache_key] = coords
                    return coords
            
            # Fallback to county center
            if county in self.KENYA_COUNTY_COORDS:
                coords = self.KENYA_COUNTY_COORDS[county]
                self.cache[cache_key] = coords
                return coords
            
            # Default to Nairobi
            logger.warning(f"No coordinates found for {county}/{sub_county}")
            return self.DEFAULT_COORDS
            
        except Exception as e:
            logger.error(f"Error geocoding {county}/{sub_county}: {e}")
            return self.DEFAULT_COORDS
    
    async def _geocode_nominatim(
        self,
        county: str,
        sub_county: str
    ) -> Optional[Dict[str, float]]:
        """
        Geocode using Nominatim (OpenStreetMap) - Free, no API key.
        Rate limited to 1 request per second.
        """
        try:
            # Rate limiting
            if self.last_request_time:
                elapsed = (datetime.now(timezone.utc) - self.last_request_time).total_seconds()
                if elapsed < 1.0:
                    await asyncio.sleep(1.0 - elapsed)
            
            query = f"{sub_county}, {county}, Kenya"
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": query,
                "format": "json",
                "limit": 1,
                "countrycodes": "ke"
            }
            headers = {
                "User-Agent": "SaveTheChildren-Backend/1.0 (Child Protection System)"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    self.last_request_time = datetime.now(timezone.utc)
                    
                    if response.status == 200:
                        data = await response.json()
                        if data and len(data) > 0:
                            result = data[0]
                            coords = {
                                "lat": float(result["lat"]),
                                "lon": float(result["lon"])
                            }
                            logger.info(f"Geocoded {query}: {coords}")
                            return coords
                        
            return None
                        
        except Exception as e:
            logger.error(f"Nominatim geocoding error: {e}")
            return None
    
    async def batch_geocode(self, locations: List[Dict[str, str]]) -> List[Dict]:
        """
        Geocode multiple locations with rate limiting.
        locations: List of dicts with 'county' and optional 'sub_county'
        """
        results = []
        
        for location in locations:
            county = location.get("county")
            sub_county = location.get("sub_county")
            
            if not county:
                continue
            
            coords = await self.geocode_location(county, sub_county)
            
            result = {
                **location,
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "coordinates": [coords["lon"], coords["lat"]]  # GeoJSON [lon, lat]
            }
            
            results.append(result)
            await asyncio.sleep(0.1)
        
        return results
