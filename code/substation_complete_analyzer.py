"""
Complete analyzer that finds ALL types of power plants connected to a substation
including wind farms (relations), solar farms, and traditional plants
"""

import requests
import pandas as pd
import json
from datetime import datetime
import os
import logging
import time
import math

class CompleteSubstationAnalyzer:
    def __init__(self):
        """Initialize the analyzer"""
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/complete_substation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.overpass_url = "http://overpass-api.de/api/interpreter"
        self.timeout = 180  # Longer timeout for complex queries
        
    def query_overpass(self, query):
        """Execute Overpass query with error handling"""
        try:
            self.logger.debug(f"Query: {query[:200]}...")
            response = requests.post(self.overpass_url, data=query, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            return None
    
    def find_all_connected_plants(self, substation_id):
        """Find ALL plants connected to substation using multiple methods"""
        self.logger.info("="*60)
        self.logger.info(f"Complete analysis for substation ID: {substation_id}")
        self.logger.info("="*60)
        
        all_plants = []
        
        # Method 1: Find plants that contain this substation as a member
        plants_containing_sub = self.find_plants_containing_substation(substation_id)
        all_plants.extend(plants_containing_sub)
        
        # Method 2: Trace power lines from substation
        plants_via_lines = self.trace_power_lines_to_plants(substation_id)
        all_plants.extend(plants_via_lines)
        
        # Method 3: Search area around substation
        plants_nearby = self.find_plants_near_substation(substation_id)
        all_plants.extend(plants_nearby)
        
        # Deduplicate and compile results
        unique_plants = self.deduplicate_plants(all_plants)
        
        # Get substation info for report
        sub_info = self.get_substation_info(substation_id)
        
        # Save comprehensive results
        self.save_complete_results(unique_plants, sub_info)
        
        return unique_plants
    
    def find_plants_containing_substation(self, substation_id):
        """Find power plant relations that have this substation as a member"""
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Find relations that have this substation as a member
          relation["power"="plant"](bw:{substation_id});
          
          // Also get all their members
          >;
        );
        out body;
        """
        
        self.logger.info("Method 1: Searching for plants containing this substation...")
        data = self.query_overpass(query)
        
        plants = []
        if data:
            for elem in data['elements']:
                if elem['type'] == 'relation' and elem.get('tags', {}).get('power') == 'plant':
                    # Check if substation is a member
                    for member in elem.get('members', []):
                        if member.get('ref') == substation_id:
                            plants.append({
                                'plant': elem,
                                'connection_method': 'substation_as_member',
                                'member_role': member.get('role', 'no_role')
                            })
                            self.logger.info(f"  Found: {elem.get('tags', {}).get('name', 'Unnamed')} (relation)")
                            break
        
        self.logger.info(f"  Method 1 found {len(plants)} plants")
        return plants
    
    def trace_power_lines_to_plants(self, substation_id):
        """Trace all power lines from substation to find connected plants"""
        # First get substation area
        query = f"""
        [out:json][timeout:60];
        way({substation_id});
        out bb;
        """
        
        data = self.query_overpass(query)
        if not data or not data['elements']:
            return []
            
        bounds = data['elements'][0].get('bounds', {})
        
        # Expand bounds for search
        expansion = 0.001
        bbox = f"{bounds['minlat']-expansion},{bounds['minlon']-expansion},{bounds['maxlat']+expansion},{bounds['maxlon']+expansion}"
        
        # Get all power infrastructure in area
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Substation and its nodes
          way({substation_id});
          node(w);
          
          // Power lines and terminals in area
          way["power"~"line|minor_line|cable"]({bbox});
          node["power"~"terminal|tower|pole"]({bbox});
          
          // Get all nodes
          node(w);
        );
        out body;
        """
        
        self.logger.info("Method 2: Tracing power lines from substation...")
        data = self.query_overpass(query)
        
        if not data:
            return []
            
        # Find connected lines
        connected_lines = self.identify_connected_lines(data, substation_id)
        self.logger.info(f"  Found {len(connected_lines)} connected power lines")
        
        # Trace each line to plants
        plants = []
        for line_id in connected_lines:
            self.logger.info(f"  Tracing line {line_id}...")
            line_plants = self.trace_line_full_path(line_id)
            plants.extend(line_plants)
            time.sleep(0.3)  # Be nice to API
        
        self.logger.info(f"  Method 2 found {len(plants)} plant connections")
        return plants
    
    def identify_connected_lines(self, data, substation_id):
        """Identify power lines connected to substation"""
        # Build node lookup
        nodes = {}
        substation_nodes = set()
        terminals_in_area = set()
        
        for elem in data['elements']:
            if elem['type'] == 'node':
                nodes[elem['id']] = elem
                if elem.get('tags', {}).get('power') in ['terminal', 'tower', 'pole']:
                    terminals_in_area.add(elem['id'])
            elif elem['type'] == 'way' and elem['id'] == substation_id:
                substation_nodes = set(elem.get('nodes', []))
        
        # Find connected lines
        connected_lines = []
        
        for elem in data['elements']:
            if elem['type'] == 'way' and elem.get('tags', {}).get('power') in ['line', 'minor_line', 'cable']:
                line_nodes = elem.get('nodes', [])
                
                # Check direct connection
                if set(line_nodes) & substation_nodes:
                    connected_lines.append(elem['id'])
                    continue
                
                # Check connection via terminals
                if line_nodes:
                    endpoints = [line_nodes[0], line_nodes[-1]]
                    for endpoint in endpoints:
                        if endpoint in terminals_in_area:
                            # Check if terminal is very close to substation
                            if endpoint in nodes and self.is_node_near_nodes(nodes[endpoint], substation_nodes, nodes, 0.05):
                                connected_lines.append(elem['id'])
                                break
        
        return list(set(connected_lines))
    
    def is_node_near_nodes(self, node, target_node_ids, all_nodes, max_dist_km):
        """Check if a node is near any of the target nodes"""
        if 'lat' not in node or 'lon' not in node:
            return False
            
        for target_id in target_node_ids:
            if target_id in all_nodes:
                target = all_nodes[target_id]
                if 'lat' in target and 'lon' in target:
                    dist = self.calculate_distance(
                        node['lat'], node['lon'],
                        target['lat'], target['lon']
                    )
                    if dist < max_dist_km:
                        return True
        return False
    
    def trace_line_full_path(self, line_id):
        """Trace a power line through the network to find plants"""
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Start with the line
          way({line_id});
          
          // Get connected lines (3 levels deep)
          way(bn)["power"~"line|minor_line|cable"];
          way(bn)["power"~"line|minor_line|cable"];
          way(bn)["power"~"line|minor_line|cable"];
          
          // Get all nodes
          node(w);
          
          // Find connected plants (ways and relations)
          way(bn)["power"="plant"];
          relation(bn)["power"="plant"];
          
          // Get full data
          >;
        );
        out body;
        """
        
        data = self.query_overpass(query)
        plants = []
        
        if data:
            for elem in data['elements']:
                if elem.get('tags', {}).get('power') == 'plant':
                    plants.append({
                        'plant': elem,
                        'connection_method': 'power_line_trace',
                        'connecting_line': line_id
                    })
        
        return plants
    
    def find_plants_near_substation(self, substation_id):
        """Find plants in vicinity of substation"""
        # Get substation location
        query = f"""
        [out:json][timeout:30];
        way({substation_id});
        out center;
        """
        
        data = self.query_overpass(query)
        if not data or not data['elements']:
            return []
            
        elem = data['elements'][0]
        if 'center' not in elem:
            return []
            
        lat = elem['center']['lat']
        lon = elem['center']['lon']
        
        # Search for plants within 5km
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          way["power"="plant"](around:5000,{lat},{lon});
          relation["power"="plant"](around:5000,{lat},{lon});
          >;
        );
        out body;
        """
        
        self.logger.info("Method 3: Searching for plants near substation...")
        data = self.query_overpass(query)
        
        plants = []
        if data:
            for elem in data['elements']:
                if elem.get('tags', {}).get('power') == 'plant':
                    # Calculate distance
                    plant_lat, plant_lon = self.get_element_center(elem, data['elements'])
                    if plant_lat and plant_lon:
                        distance = self.calculate_distance(lat, lon, plant_lat, plant_lon)
                        plants.append({
                            'plant': elem,
                            'connection_method': 'proximity',
                            'distance_km': round(distance, 2)
                        })
                        self.logger.info(f"  Found nearby: {elem.get('tags', {}).get('name', 'Unnamed')} ({distance:.1f}km)")
        
        self.logger.info(f"  Method 3 found {len(plants)} nearby plants")
        return plants
    
    def get_element_center(self, element, all_elements):
        """Get center coordinates of an element"""
        if element['type'] == 'node':
            return element.get('lat'), element.get('lon')
        elif element['type'] == 'way':
            # Calculate from nodes
            node_ids = element.get('nodes', [])
            lats, lons = [], []
            for elem in all_elements:
                if elem['type'] == 'node' and elem['id'] in node_ids:
                    if 'lat' in elem and 'lon' in elem:
                        lats.append(elem['lat'])
                        lons.append(elem['lon'])
            if lats and lons:
                return sum(lats)/len(lats), sum(lons)/len(lons)
        elif element['type'] == 'relation':
            # Use first way member's center
            for member in element.get('members', []):
                if member['type'] == 'way':
                    for elem in all_elements:
                        if elem['type'] == 'way' and elem['id'] == member['ref']:
                            return self.get_element_center(elem, all_elements)
        return None, None
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance in km between two points"""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    
    def get_substation_info(self, substation_id):
        """Get detailed substation information"""
        query = f"""
        [out:json][timeout:30];
        way({substation_id});
        out body;
        """
        
        data = self.query_overpass(query)
        if data and data['elements']:
            return data['elements'][0]
        return None
    
    def deduplicate_plants(self, plants):
        """Remove duplicate plants, keeping best connection info"""
        unique = {}
        
        for plant_info in plants:
            plant = plant_info['plant']
            plant_id = f"{plant['type']}_{plant['id']}"
            
            if plant_id not in unique:
                unique[plant_id] = plant_info
            else:
                # Keep the one with better connection method
                priority = {
                    'substation_as_member': 1,
                    'power_line_trace': 2,
                    'proximity': 3
                }
                
                current_priority = priority.get(unique[plant_id]['connection_method'], 4)
                new_priority = priority.get(plant_info['connection_method'], 4)
                
                if new_priority < current_priority:
                    unique[plant_id] = plant_info
        
        return list(unique.values())
    
    def save_complete_results(self, plants, substation_info):
        """Save comprehensive analysis results"""
        if not substation_info:
            self.logger.error("No substation info available")
            return
            
        os.makedirs("outputs", exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sub_id = substation_info['id']
        sub_name = substation_info.get('tags', {}).get('name', 'Unknown')
        
        # Prepare data for DataFrame
        rows = []
        for plant_info in plants:
            plant = plant_info['plant']
            tags = plant.get('tags', {})
            
            row = {
                'substation_id': sub_id,
                'substation_name': sub_name,
                'substation_voltage': substation_info.get('tags', {}).get('voltage', ''),
                'substation_operator': substation_info.get('tags', {}).get('operator', ''),
                'plant_id': plant['id'],
                'plant_type': plant['type'],  # way or relation
                'plant_name': tags.get('name', 'Unnamed Plant'),
                'plant_operator': tags.get('operator', ''),
                'plant_source': tags.get('plant:source', tags.get('generator:source', '')),
                'plant_output': tags.get('plant:output:electricity', ''),
                'site_type': tags.get('site', ''),  # e.g., wind_farm
                'connection_method': plant_info['connection_method'],
                'connection_details': plant_info.get('member_role', 
                                    plant_info.get('connecting_line', 
                                    plant_info.get('distance_km', ''))),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            rows.append(row)
        
        if rows:
            df = pd.DataFrame(rows)
            
            # Save Excel with multiple sheets
            excel_file = f"outputs/complete_analysis_{sub_id}_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                # Main results
                df.to_excel(writer, sheet_name='All_Connections', index=False)
                
                # Summary by connection method
                summary_method = df.groupby('connection_method').agg({
                    'plant_id': 'count',
                    'plant_output': lambda x: ', '.join(x.dropna().unique())
                }).rename(columns={'plant_id': 'count'})
                summary_method.to_excel(writer, sheet_name='By_Method')
                
                # Summary by plant type
                summary_type = df.groupby('plant_source').agg({
                    'plant_id': 'count',
                    'plant_output': lambda x: ', '.join(x.dropna().unique()),
                    'plant_name': lambda x: ', '.join(x.unique()[:5])  # First 5 names
                }).rename(columns={'plant_id': 'count'})
                summary_type.to_excel(writer, sheet_name='By_Type')
                
                # Overall summary
                summary = {
                    'Substation': f"{sub_name} ({sub_id})",
                    'Total Connected Plants': len(df),
                    'Wind Farms': len(df[df['plant_source'].str.contains('wind', case=False, na=False)]),
                    'Solar Plants': len(df[df['plant_source'].str.contains('solar', case=False, na=False)]),
                    'Direct Members': len(df[df['connection_method'] == 'substation_as_member']),
                    'Via Power Lines': len(df[df['connection_method'] == 'power_line_trace']),
                    'Nearby (< 5km)': len(df[df['connection_method'] == 'proximity'])
                }
                pd.DataFrame([summary]).T.to_excel(writer, sheet_name='Summary', header=['Value'])
            
            # Also save CSV
            csv_file = f"outputs/complete_analysis_{sub_id}_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            
            self.logger.info("="*60)
            self.logger.info(f"ANALYSIS COMPLETE!")
            self.logger.info(f"Substation: {sub_name}")
            self.logger.info(f"Total plants found: {len(df)}")
            self.logger.info(f"Results saved to: {excel_file}")
            self.logger.info("="*60)
        else:
            self.logger.warning("No plants found!")
            
            # Save empty result
            empty_df = pd.DataFrame([{
                'substation_id': sub_id,
                'substation_name': sub_name,
                'status': 'No connected plants found',
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }])
            csv_file = f"outputs/no_plants_{sub_id}_{timestamp}.csv"
            empty_df.to_csv(csv_file, index=False)

def main():
    analyzer = CompleteSubstationAnalyzer()
    
    # Analyze SET Los Vientos
    analyzer.find_all_connected_plants(170140947)

if __name__ == "__main__":
    main()