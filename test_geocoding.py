"""
Test script for Kenya API geocoding integration.
Run this to verify the geocoding service works correctly.
"""
import asyncio
import sys
sys.path.append('/home/yugo/SaveTheChildren_Backend')

from app.services.geocoding_service import GeocodingService


async def test_geocoding():
    """Test geocoding service"""
    print("🧪 Testing Kenya Geocoding Service\n")
    
    geocoding = GeocodingService()
    
    # Test 1: County center
    print("Test 1: County center (pre-defined)")
    coords = await geocoding.geocode_location("Nairobi")
    print(f"  Nairobi: {coords}")
    assert coords["lat"] == -1.2921
    assert coords["lon"] == 36.8219
    print("  ✅ Pass\n")
    
    # Test 2: Sub-county geocoding
    print("Test 2: Sub-county geocoding (Nominatim)")
    coords = await geocoding.geocode_location("Nairobi", "Westlands")
    print(f"  Westlands, Nairobi: {coords}")
    assert coords["lat"] is not None
    assert coords["lon"] is not None
    print("  ✅ Pass\n")
    
    # Test 3: Batch geocoding
    print("Test 3: Batch geocoding")
    locations = [
        {"county": "Nairobi", "sub_county": "Westlands"},
        {"county": "Mombasa", "sub_county": "Mvita"},
        {"county": "Kisumu"}
    ]
    results = await geocoding.batch_geocode(locations)
    print(f"  Geocoded {len(results)} locations:")
    for r in results:
        print(f"    - {r['sub_county'] or 'center'}, {r['county']}: ({r['latitude']}, {r['longitude']})")
    print("  ✅ Pass\n")
    
    # Test 4: Cache usage
    print("Test 4: Cache usage")
    coords1 = await geocoding.geocode_location("Nairobi")
    coords2 = await geocoding.geocode_location("Nairobi")
    assert coords1 == coords2
    print(f"  Cache working: {len(geocoding.cache)} cached locations")
    print("  ✅ Pass\n")
    
    # Test 5: Unknown location (fallback)
    print("Test 5: Unknown location (fallback to default)")
    coords = await geocoding.geocode_location("UnknownCounty")
    print(f"  Unknown County: {coords}")
    assert coords == geocoding.DEFAULT_COORDS
    print("  ✅ Pass\n")
    
    print("✅ All tests passed! Geocoding service ready.")
    print(f"\n📍 Total cached locations: {len(geocoding.cache)}")


if __name__ == "__main__":
    asyncio.run(test_geocoding())
