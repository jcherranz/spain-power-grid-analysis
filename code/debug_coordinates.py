"""Debug script to check coordinate extraction"""
import requests
import json

# Test query for Madrid
query = """
[out:json][timeout:30];
(
  node["power"="plant"][!"proposed"][!"construction"](40.3,-3.8,40.5,-3.6);
  way["power"="plant"][!"proposed"][!"construction"](40.3,-3.8,40.5,-3.6);
  relation["power"="plant"][!"proposed"][!"construction"](40.3,-3.8,40.5,-3.6);
);
out center;
"""

response = requests.post("http://overpass-api.de/api/interpreter", data=query)
data = response.json()

print(f"Found {len(data['elements'])} power plants")
for elem in data['elements']:
    print(f"\nType: {elem['type']}, ID: {elem['id']}")
    print(f"Name: {elem.get('tags', {}).get('name', 'No name')}")
    
    if elem['type'] == 'node':
        print(f"Coordinates: {elem.get('lat')}, {elem.get('lon')}")
    elif 'center' in elem:
        print(f"Center: {elem['center'].get('lat')}, {elem['center'].get('lon')}")
    else:
        print("No coordinates found")
    
    print(f"Tags: {elem.get('tags', {})}")