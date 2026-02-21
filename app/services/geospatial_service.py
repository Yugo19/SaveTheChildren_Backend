from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime
from app.core.logging import logger
from app.utils.severity_mapping import get_severity_aggregation_stage
from typing import Optional, Tuple
from app.db.redis_client import get_redis
import json
import hashlib


class GeospatialService:
    CACHE_TTL_SECONDS = 60 * 60 * 4  # 4 hours
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases
        self.redis = get_redis()
    
    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate cache key based on method and parameters"""
        params_str = json.dumps(kwargs, sort_keys=True)
        params_hash = hashlib.md5(params_str.encode()).hexdigest()
        return f"geospatial:{method}:{params_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[dict]:
        """Get cached result from Redis"""
        try:
            cached_json = await self.redis.get(cache_key)
            if cached_json:
                logger.info(f"Returning cached result for: {cache_key}")
                return json.loads(cached_json)
        except Exception as e:
            logger.warning(f"Cache read error: {str(e)}")
        return None
    
    async def _save_to_cache(self, cache_key: str, data: dict):
        """Save result to Redis cache"""
        try:
            await self.redis.setex(
                cache_key,
                self.CACHE_TTL_SECONDS,
                json.dumps(data)
            )
            logger.info(f"Cached result for: {cache_key} (TTL: 4 hours)")
        except Exception as e:
            logger.warning(f"Cache write error: {str(e)}")

    async def get_nearby_cases(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10
    ):
        """Get cases near a specific location"""
        # Check cache first
        cache_key = self._get_cache_key("nearby", latitude=latitude, longitude=longitude, radius_km=radius_km)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Convert radius to degrees (approximate)
            radius_degrees = radius_km / 111.0
            
            pipeline = [
                {
                    "$geoNear": {
                        "near": {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        },
                        "distanceField": "distance",
                        "maxDistance": radius_degrees * 111000,
                        "spherical": True
                    }
                },
                {"$limit": 50}
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            logger.info(f"Nearby cases retrieved for location {latitude}, {longitude}")
            
            result = {
                "latitude": latitude,
                "longitude": longitude,
                "radius_km": radius_km,
                "cases_found": len(results),
                "cases": [
                    {
                        "case_id": c.get("case_id"),
                        "county": c.get("county"),
                        "latitude": c.get("latitude"),
                        "longitude": c.get("longitude"),
                        "severity": c.get("severity"),
                        "distance_km": c.get("distance", 0) / 1000
                    }
                    for c in results
                ]
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting nearby cases: {e}")
            raise

    async def get_hotspots(self, radius_km: float = 5):
        """Get case hotspots/clusters"""
        # Check cache first
        cache_key = self._get_cache_key("hotspots", radius_km=radius_km)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            pipeline = [
                {
                    "$match": {
                        "location": {"$exists": True},
                        "latitude": {"$exists": True, "$ne": None},
                        "longitude": {"$exists": True, "$ne": None}
                    }
                },
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$group": {
                        "_id": {
                            "latitude": {"$round": ["$latitude", 1]},
                            "longitude": {"$round": ["$longitude", 1]}
                        },
                        "count": {"$sum": 1},
                        "high_severity": {
                            "$sum": {"$cond": [{"$eq": ["$derived_severity", "high"]}, 1, 0]}
                        },
                        "abuse_types": {"$push": "$abuse_type"}
                    }
                },
                {"$sort": {"count": -1}},
                {"$limit": 100}
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            logger.info("Hotspots retrieved")
            
            result = {
                "hotspots": [
                    {
                        "latitude": h["_id"]["latitude"],
                        "longitude": h["_id"]["longitude"],
                        "case_count": h["count"],
                        "high_severity_count": h["high_severity"],
                        "abuse_types": list(set(h["abuse_types"]))
                    }
                    for h in results
                ]
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting hotspots: {e}")
            raise

    async def get_county_boundaries(self, source: Optional[str] = None):
        """Get case statistics by county with geographic info"""
        # Check cache first
        cache_key = self._get_cache_key("county_boundaries", source=source)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            match_filter = {"county": {"$exists": True}}
            if source:
                match_filter["source"] = source
            
            pipeline = [
                {"$match": match_filter},
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$group": {
                        "_id": "$county",
                        "case_count": {"$sum": 1},
                        "high_severity": {
                            "$sum": {"$cond": [{"$eq": ["$derived_severity", "high"]}, 1, 0]}
                        },
                        "open_cases": {
                            "$sum": {"$cond": [{"$eq": ["$status", "open"]}, 1, 0]}
                        },
                        "avg_latitude": {"$avg": "$latitude"},
                        "avg_longitude": {"$avg": "$longitude"},
                        "abuse_types": {"$addToSet": "$abuse_type"}
                    }
                },
                {"$sort": {"case_count": -1}}
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            logger.info("County boundaries retrieved")
            
            result = {
                "counties": [
                    {
                        "county": c["_id"],
                        "case_count": c["case_count"],
                        "high_severity_count": c["high_severity"],
                        "open_cases": c["open_cases"],
                        "center_latitude": c["avg_latitude"],
                        "center_longitude": c["avg_longitude"],
                        "abuse_types": [a for a in c.get("abuse_types", []) if a]
                    }
                    for c in results
                ]
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting county boundaries: {e}")
            raise

    async def get_heatmap_data(
        self,
        county: Optional[str] = None,
        abuse_type: Optional[str] = None,
        source: Optional[str] = None
    ):
        """Get heatmap data for visualization"""
        # Check cache first
        cache_key = self._get_cache_key("heatmap", county=county, abuse_type=abuse_type, source=source)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            filters = {
                "latitude": {"$exists": True, "$ne": None},
                "longitude": {"$exists": True, "$ne": None}
            }
            
            if county:
                filters["county"] = county.strip()  # Trim whitespace
            if abuse_type:
                filters["abuse_type"] = abuse_type.strip()
            if source:
                filters["source"] = source
            
            cases = await self.cases_collection.find(
                filters,
                {
                    "latitude": 1,
                    "longitude": 1,
                    "severity": 1,
                    "case_id": 1,
                    "status": 1
                }
            ).to_list(1000)  # Limit for performance
            
            # Convert severity/status to weight
            severity_weights = {"high": 3, "medium": 2, "low": 1}
            status_weights = {"open": 3, "in_progress": 2, "closed": 1}
            
            heatmap_points = [
                {
                    "latitude": c["latitude"],
                    "longitude": c["longitude"],
                    "lat": c["latitude"],  # Alias for compatibility
                    "lng": c["longitude"],  # Alias for compatibility
                    "weight": severity_weights.get(c.get("severity"), status_weights.get(c.get("status"), 1)),
                    "case_id": c.get("case_id")
                }
                for c in cases if c.get("latitude") and c.get("longitude")
            ]
            
            logger.info(f"Heatmap data retrieved ({len(heatmap_points)} points)")
            
            result = {
                "total_points": len(heatmap_points),
                "filters": {
                    "county": county,
                    "abuse_type": abuse_type,
                    "source": source
                },
                "points": heatmap_points
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting heatmap data: {e}")
            raise

    async def get_case_density(self, zoom_level: int = 10):
        """Get case density grid"""
        # Check cache first
        cache_key = self._get_cache_key("density", zoom_level=zoom_level)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            severity_expr = get_severity_aggregation_stage()
            
            # Create a grid based on zoom level
            grid_size = 1.0 / (2 ** (zoom_level - 1))
            
            pipeline = [
                {
                    "$match": {
                        "latitude": {"$exists": True, "$ne": None},
                        "longitude": {"$exists": True, "$ne": None}
                    }
                },
                {"$addFields": {"derived_severity": severity_expr}},
                {
                    "$group": {
                        "_id": {
                            "lat_grid": {
                                "$multiply": [
                                    {"$floor": {"$divide": ["$latitude", grid_size]}},
                                    grid_size
                                ]
                            },
                            "lon_grid": {
                                "$multiply": [
                                    {"$floor": {"$divide": ["$longitude", grid_size]}},
                                    grid_size
                                ]
                            }
                        },
                        "count": {"$sum": 1},
                        "high_severity": {
                            "$sum": {"$cond": [{"$eq": ["$derived_severity", "high"]}, 1, 0]}
                        }
                    }
                }
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            logger.info("Case density retrieved")
            
            result = {
                "zoom_level": zoom_level,
                "grid_size": grid_size,
                "density_cells": [
                    {
                        "latitude": d["_id"]["lat_grid"],
                        "longitude": d["_id"]["lon_grid"],
                        "total_cases": d["count"],
                        "high_severity_cases": d["high_severity"]
                    }
                    for d in results
                ]
            }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting case density: {e}")
            raise

    async def get_map_data(
        self,
        county: Optional[str] = None,
        abuse_type: Optional[str] = None,
        year: Optional[int] = None,
        source: Optional[str] = None,
        format: str = "geojson"
    ):
        """Get map data in GeoJSON or simple format for visualization"""
        # Check cache first
        cache_key = self._get_cache_key("map_data", county=county, abuse_type=abuse_type, year=year, source=source, format=format)
        cached = await self._get_from_cache(cache_key)
        if cached:
            return cached
        
        try:
            # Build query filters
            filters = {}
            if county:
                filters["county"] = county.strip()  # Trim whitespace
            if abuse_type:
                filters["abuse_type"] = abuse_type.strip()
            if year:
                filters["year"] = year
            if source:
                filters["source"] = source
            
            # Aggregate cases by sub-county/location
            pipeline = [
                {"$match": filters},
                {
                    "$group": {
                        "_id": {
                            "county": "$county",
                            "sub_county": "$sub_county",
                            "latitude": "$latitude",
                            "longitude": "$longitude"
                        },
                        "case_count": {"$sum": 1},
                        "open_cases": {
                            "$sum": {"$cond": [{"$eq": ["$status", "open"]}, 1, 0]}
                        },
                        "abuse_types": {"$addToSet": "$abuse_type"}
                    }
                },
                {
                    "$match": {
                        "_id.latitude": {"$exists": True, "$ne": None},
                        "_id.longitude": {"$exists": True, "$ne": None}
                    }
                },
                {"$sort": {"case_count": -1}}
            ]
            
            results = await self.cases_collection.aggregate(pipeline).to_list(None)
            
            # Format for map display
            map_points = []
            geojson_features = []
            
            for r in results:
                county_name = r["_id"].get("county")
                sub_county_name = r["_id"].get("sub_county")
                lat = r["_id"].get("latitude")
                lon = r["_id"].get("longitude")
                
                if not lat or not lon:
                    continue
                
                point = {
                    "county": county_name,
                    "sub_county": sub_county_name,
                    "latitude": lat,
                    "longitude": lon,
                    "case_count": r["case_count"],
                    "open_cases": r["open_cases"],
                    "abuse_types": [a for a in r.get("abuse_types", []) if a]
                }
                
                map_points.append(point)
                
                # GeoJSON Feature format
                geojson_features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]  # [longitude, latitude]
                    },
                    "properties": {
                        "county": county_name,
                        "sub_county": sub_county_name,
                        "case_count": r["case_count"],
                        "open_cases": r["open_cases"],
                        "abuse_types": [a for a in r.get("abuse_types", []) if a]
                    }
                })
            
            logger.info(f"Map data retrieved ({len(map_points)} locations)")
            
            if format == "geojson":
                result = {
                    "type": "FeatureCollection",
                    "features": geojson_features,
                    "metadata": {
                        "total_locations": len(map_points),
                        "total_cases": sum(p["case_count"] for p in map_points),
                        "filters": {
                            "county": county,
                            "abuse_type": abuse_type,
                            "year": year,
                            "source": source
                        }
                    }
                }
            else:
                # Simple format
                result = {
                    "points": map_points,
                    "total_locations": len(map_points),
                    "total_cases": sum(p["case_count"] for p in map_points),
                    "filters": {
                        "county": county,
                        "abuse_type": abuse_type,
                        "year": year,
                        "source": source
                    }
                }
            
            # Cache the result
            await self._save_to_cache(cache_key, result)
            return result
        except Exception as e:
            logger.error(f"Error getting map data: {e}")
            raise
