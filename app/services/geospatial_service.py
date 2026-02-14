from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from bson import ObjectId
from datetime import datetime
from app.core.logging import logger
from app.utils.severity_mapping import get_severity_aggregation_stage
from typing import Optional, Tuple


class GeospatialService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.cases_collection = db.cases

    async def get_nearby_cases(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 10
    ):
        """Get cases near a specific location"""
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
            
            return {
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
        except Exception as e:
            logger.error(f"Error getting nearby cases: {e}")
            raise

    async def get_hotspots(self, radius_km: float = 5):
        """Get case hotspots/clusters"""
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
            
            return {
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
        except Exception as e:
            logger.error(f"Error getting hotspots: {e}")
            raise

    async def get_county_boundaries(self, source: Optional[str] = None):
        """Get case statistics by county with geographic info"""
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
            
            return {
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
            
            return {
                "total_points": len(heatmap_points),
                "filters": {
                    "county": county,
                    "abuse_type": abuse_type,
                    "source": source
                },
                "points": heatmap_points
            }
        except Exception as e:
            logger.error(f"Error getting heatmap data: {e}")
            raise

    async def get_case_density(self, zoom_level: int = 10):
        """Get case density grid"""
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
            
            return {
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
                return {
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
                return {
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
        except Exception as e:
            logger.error(f"Error getting map data: {e}")
            raise
