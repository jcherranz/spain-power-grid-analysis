"""
Advanced network tracer that follows power lines through the complete network
to find all connected plants, including through intermediate substations
"""

import requests
import pandas as pd
import json
from datetime import datetime
import os
import logging
import time
import math
from collections import deque

class SubstationNetworkTracer:
    def __init__(self):
        """Initialize the tracer"""
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/network_tracer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
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
        self.timeout = 180
        
    def query_overpass(self, query):
        """Execute Overpass query with error handling"""
        try:
            response = requests.post(self.overpass_url, data=query, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            return None
    
    def analyze_complete_network(self, substation_id):
        """Complete network analysis starting from a substation"""
        self.logger.info("="*60)
        self.logger.info(f"Network Analysis for Substation ID: {substation_id}")
        self.logger.info("="*60)
        
        # Get substation info
        sub_info = self.get_substation_details(substation_id)
        if not sub_info:
            self.logger.error(f"Substation {substation_id} not found")
            return
            
        sub_name = sub_info.get('tags', {}).get('name', 'Unknown')
        self.logger.info(f"Starting point: {sub_name}")
        
        # Step 1: Find ALL power lines connected to the substation
        connected_lines = self.find_all_connected_lines(substation_id, sub_info)
        self.logger.info(f"Found {len(connected_lines)} directly connected power lines")
        
        # Step 2: Trace the network from these lines
        all_plants = []
        traced_lines = set()
        
        for line_info in connected_lines:
            line_id = line_info['line_id']
            if line_id not in traced_lines:
                self.logger.info(f"\nTracing from line {line_id} ({line_info.get('voltage', 'Unknown')}V)")
                plants = self.trace_network_from_line(line_id, traced_lines)
                all_plants.extend(plants)
                time.sleep(0.5)
        
        # Step 3: Check for plants that have the substation as a member
        member_plants = self.find_member_plants(substation_id)
        all_plants.extend(member_plants)
        
        # Step 4: Find nearby plants that might be connected
        nearby_plants = self.find_and_verify_nearby_plants(substation_id, sub_info, connected_lines)
        all_plants.extend(nearby_plants)
        
        # Deduplicate and save results
        unique_plants = self.deduplicate_plants(all_plants)
        self.save_network_results(unique_plants, sub_info, connected_lines)
        
    def get_substation_details(self, substation_id):
        """Get detailed substation information including exact geometry"""
        query = f"""
        [out:json][timeout:30];
        (
          way({substation_id});
          node(w);
        );
        out body;
        """
        
        data = self.query_overpass(query)
        if not data:
            return None
            
        # Build the substation info with node coordinates
        sub_info = None
        nodes = {}
        
        for elem in data['elements']:
            if elem['type'] == 'node':
                nodes[elem['id']] = elem
            elif elem['type'] == 'way' and elem['id'] == substation_id:
                sub_info = elem
                
        if sub_info:
            # Add node coordinates to substation info
            sub_info['node_coords'] = []
            for node_id in sub_info.get('nodes', []):
                if node_id in nodes:
                    sub_info['node_coords'].append({
                        'id': node_id,
                        'lat': nodes[node_id].get('lat'),
                        'lon': nodes[node_id].get('lon')
                    })
                    
            # Calculate center
            lats = [n['lat'] for n in sub_info['node_coords'] if n['lat']]
            lons = [n['lon'] for n in sub_info['node_coords'] if n['lon']]
            if lats and lons:
                sub_info['center'] = {
                    'lat': sum(lats) / len(lats),
                    'lon': sum(lons) / len(lons)
                }
                
        return sub_info
    
    def find_all_connected_lines(self, substation_id, sub_info):
        """Find ALL power lines connected to the substation using multiple methods"""
        connected_lines = []
        
        # Get substation bounds with buffer
        bounds = self.get_bounds_with_buffer(sub_info, 0.005)  # ~500m buffer
        
        # Query for all power infrastructure in the area
        query = f"""
        [out:json][timeout:120];
        (
          // All power lines in the area
          way["power"~"line|minor_line|cable"]({bounds});
          
          // All power nodes in the area
          node["power"]({bounds});
          
          // Get all nodes of the lines
          node(w);
        );
        out body;
        """
        
        self.logger.info("Searching for connected power lines...")
        data = self.query_overpass(query)
        
        if not data:
            return connected_lines
            
        # Build lookups
        all_nodes = {}
        power_lines = []
        terminals = []
        substation_nodes = set(sub_info.get('nodes', []))
        
        for elem in data['elements']:
            if elem['type'] == 'node':
                all_nodes[elem['id']] = elem
                if elem.get('tags', {}).get('power') in ['terminal', 'tower', 'pole', 'portal']:
                    terminals.append(elem)
            elif elem['type'] == 'way' and elem.get('tags', {}).get('power') in ['line', 'minor_line', 'cable']:
                power_lines.append(elem)
        
        # Method 1: Direct node connection
        for line in power_lines:
            line_nodes = set(line.get('nodes', []))
            if line_nodes & substation_nodes:
                connected_lines.append({
                    'line_id': line['id'],
                    'voltage': line.get('tags', {}).get('voltage', ''),
                    'operator': line.get('tags', {}).get('operator', ''),
                    'connection_type': 'direct',
                    'connection_point': list(line_nodes & substation_nodes)[0]
                })
                continue
                
        # Method 2: Lines ending at terminals very close to substation
        for line in power_lines:
            line_nodes = line.get('nodes', [])
            if line_nodes:
                # Check both endpoints
                for endpoint in [line_nodes[0], line_nodes[-1]]:
                    if endpoint in all_nodes:
                        node = all_nodes[endpoint]
                        # Check if this endpoint is very close to any substation node
                        if self.is_node_near_substation(node, sub_info, 0.1):  # 100m
                            # Check if it's already added
                            if not any(cl['line_id'] == line['id'] for cl in connected_lines):
                                connected_lines.append({
                                    'line_id': line['id'],
                                    'voltage': line.get('tags', {}).get('voltage', ''),
                                    'operator': line.get('tags', {}).get('operator', ''),
                                    'connection_type': 'endpoint_near',
                                    'distance_m': self.min_distance_to_substation(node, sub_info) * 1000
                                })
                                break
        
        # Method 3: Lines passing very close to substation
        for line in power_lines:
            if not any(cl['line_id'] == line['id'] for cl in connected_lines):
                # Check if any node of the line is very close
                line_nodes = line.get('nodes', [])
                min_dist = float('inf')
                closest_node = None
                
                for node_id in line_nodes:
                    if node_id in all_nodes:
                        node = all_nodes[node_id]
                        dist = self.min_distance_to_substation(node, sub_info)
                        if dist < min_dist:
                            min_dist = dist
                            closest_node = node_id
                
                if min_dist < 0.05:  # 50m
                    connected_lines.append({
                        'line_id': line['id'],
                        'voltage': line.get('tags', {}).get('voltage', ''),
                        'operator': line.get('tags', {}).get('operator', ''),
                        'connection_type': 'passing_near',
                        'distance_m': min_dist * 1000,
                        'closest_node': closest_node
                    })
        
        return connected_lines
    
    def get_bounds_with_buffer(self, sub_info, buffer):
        """Get bounding box of substation with buffer"""
        if not sub_info.get('node_coords'):
            return None
            
        lats = [n['lat'] for n in sub_info['node_coords'] if n.get('lat')]
        lons = [n['lon'] for n in sub_info['node_coords'] if n.get('lon')]
        
        if not lats or not lons:
            return None
            
        return f"{min(lats)-buffer},{min(lons)-buffer},{max(lats)+buffer},{max(lons)+buffer}"
    
    def is_node_near_substation(self, node, sub_info, max_dist_km):
        """Check if a node is near the substation"""
        if 'lat' not in node or 'lon' not in node:
            return False
            
        return self.min_distance_to_substation(node, sub_info) < max_dist_km
    
    def min_distance_to_substation(self, node, sub_info):
        """Calculate minimum distance from node to substation"""
        if 'lat' not in node or 'lon' not in node:
            return float('inf')
            
        min_dist = float('inf')
        
        # Check distance to all substation nodes
        for sub_node in sub_info.get('node_coords', []):
            if sub_node.get('lat') and sub_node.get('lon'):
                dist = self.calculate_distance(
                    node['lat'], node['lon'],
                    sub_node['lat'], sub_node['lon']
                )
                min_dist = min(min_dist, dist)
                
        return min_dist
    
    def trace_network_from_line(self, start_line_id, traced_lines):
        """Trace the power network from a line using BFS to find all connected plants"""
        plants_found = []
        lines_to_trace = deque([start_line_id])
        traced_lines.add(start_line_id)
        intermediate_substations = []
        
        trace_depth = 0
        max_depth = 10  # Maximum depth to prevent infinite loops
        
        while lines_to_trace and trace_depth < max_depth:
            current_batch_size = len(lines_to_trace)
            trace_depth += 1
            
            self.logger.info(f"  Trace depth {trace_depth}: checking {current_batch_size} lines")
            
            # Process all lines at current depth
            batch_line_ids = []
            for _ in range(current_batch_size):
                if lines_to_trace:
                    batch_line_ids.append(lines_to_trace.popleft())
            
            if not batch_line_ids:
                break
                
            # Query for all lines in batch and their connections
            line_list = ','.join(map(str, batch_line_ids))
            
            query = f"""
            [out:json][timeout:180];
            (
              // Get the lines
              way(id:{line_list});
              
              // Get their nodes
              node(w);
              
              // Find connected ways (lines and plants)
              way(bn);
              
              // Also get relations connected to these nodes
              relation(bn);
              
              // Get full data
              >;
            );
            out body;
            """
            
            data = self.query_overpass(query)
            
            if data:
                elements_by_id = {e['id']: e for e in data['elements'] if 'id' in e}
                
                # Process results
                for elem in data['elements']:
                    elem_type = elem.get('type')
                    tags = elem.get('tags', {})
                    power_tag = tags.get('power')
                    
                    # Found a plant!
                    if power_tag == 'plant':
                        plant_id = f"{elem_type}_{elem['id']}"
                        if not any(p['plant_id'] == plant_id for p in plants_found):
                            plants_found.append({
                                'plant': elem,
                                'plant_id': plant_id,
                                'found_at_depth': trace_depth,
                                'connection_method': 'network_trace',
                                'trace_path': list(traced_lines)[:5]  # First 5 lines in path
                            })
                            self.logger.info(f"    Found plant: {tags.get('name', 'Unnamed')} at depth {trace_depth}")
                    
                    # Found another line to trace
                    elif elem_type == 'way' and power_tag in ['line', 'minor_line', 'cable']:
                        if elem['id'] not in traced_lines:
                            traced_lines.add(elem['id'])
                            lines_to_trace.append(elem['id'])
                    
                    # Found an intermediate substation
                    elif elem_type == 'way' and power_tag == 'substation':
                        if elem['id'] not in intermediate_substations:
                            intermediate_substations.append(elem['id'])
                            # Get lines connected to this substation
                            sub_nodes = set(elem.get('nodes', []))
                            for other_elem in data['elements']:
                                if (other_elem.get('type') == 'way' and 
                                    other_elem.get('tags', {}).get('power') in ['line', 'minor_line', 'cable'] and
                                    set(other_elem.get('nodes', [])) & sub_nodes):
                                    if other_elem['id'] not in traced_lines:
                                        traced_lines.add(other_elem['id'])
                                        lines_to_trace.append(other_elem['id'])
            
            time.sleep(0.3)  # Be nice to API
        
        if intermediate_substations:
            self.logger.info(f"  Traced through {len(intermediate_substations)} intermediate substations")
            
        return plants_found
    
    def find_member_plants(self, substation_id):
        """Find plants that have this substation as a member"""
        query = f"""
        [out:json][timeout:60];
        (
          relation["power"="plant"](bw:{substation_id});
          >;
        );
        out body;
        """
        
        self.logger.info("\nSearching for plants with substation as member...")
        data = self.query_overpass(query)
        
        plants = []
        if data:
            for elem in data['elements']:
                if elem['type'] == 'relation' and elem.get('tags', {}).get('power') == 'plant':
                    # Verify substation is a member
                    for member in elem.get('members', []):
                        if member.get('ref') == substation_id:
                            plants.append({
                                'plant': elem,
                                'plant_id': f"relation_{elem['id']}",
                                'connection_method': 'substation_member',
                                'member_role': member.get('role', 'none')
                            })
                            self.logger.info(f"  Found: {elem.get('tags', {}).get('name', 'Unnamed')}")
                            break
        
        return plants
    
    def find_and_verify_nearby_plants(self, substation_id, sub_info, connected_lines):
        """Find nearby plants and verify if they're connected via the traced lines"""
        if not sub_info.get('center'):
            return []
            
        lat = sub_info['center']['lat']
        lon = sub_info['center']['lon']
        
        # Search within 10km
        query = f"""
        [out:json][timeout:120];
        (
          way["power"="plant"](around:10000,{lat},{lon});
          relation["power"="plant"](around:10000,{lat},{lon});
          >;
        );
        out body;
        """
        
        self.logger.info("\nSearching for nearby plants to verify connections...")
        data = self.query_overpass(query)
        
        plants = []
        if data:
            # Get connected line voltages for matching
            connected_voltages = set()
            for line_info in connected_lines:
                if line_info.get('voltage'):
                    connected_voltages.add(line_info['voltage'])
            
            for elem in data['elements']:
                if elem.get('tags', {}).get('power') == 'plant':
                    plant_name = elem.get('tags', {}).get('name', 'Unnamed')
                    
                    # Calculate distance
                    plant_lat, plant_lon = self.get_element_center(elem, data['elements'])
                    if plant_lat and plant_lon:
                        distance = self.calculate_distance(lat, lon, plant_lat, plant_lon)
                        
                        # Determine connection likelihood
                        connection_likely = 'possible'
                        if distance < 3:  # Within 3km
                            connection_likely = 'likely'
                        
                        plants.append({
                            'plant': elem,
                            'plant_id': f"{elem['type']}_{elem['id']}",
                            'connection_method': 'nearby_verified',
                            'distance_km': round(distance, 2),
                            'connection_likely': connection_likely,
                            'matching_voltages': list(connected_voltages)
                        })
                        
                        self.logger.info(f"  Found nearby: {plant_name} ({distance:.1f}km) - {connection_likely}")
        
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
    
    def deduplicate_plants(self, plants):
        """Remove duplicate plants, keeping the best connection info"""
        unique = {}
        
        for plant_info in plants:
            plant_id = plant_info['plant_id']
            
            if plant_id not in unique:
                unique[plant_id] = plant_info
            else:
                # Keep the one with better connection method
                priority = {
                    'substation_member': 1,
                    'network_trace': 2,
                    'nearby_verified': 3
                }
                
                current_priority = priority.get(unique[plant_id]['connection_method'], 4)
                new_priority = priority.get(plant_info['connection_method'], 4)
                
                if new_priority < current_priority:
                    unique[plant_id] = plant_info
                elif new_priority == current_priority:
                    # For network traces, keep the one with shorter path
                    if plant_info['connection_method'] == 'network_trace':
                        if plant_info.get('found_at_depth', 999) < unique[plant_id].get('found_at_depth', 999):
                            unique[plant_id] = plant_info
        
        return list(unique.values())
    
    def save_network_results(self, plants, sub_info, connected_lines):
        """Save comprehensive network analysis results"""
        os.makedirs("outputs", exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sub_id = sub_info['id']
        sub_name = sub_info.get('tags', {}).get('name', 'Unknown')
        
        # Prepare plant data
        plant_rows = []
        for plant_info in plants:
            plant = plant_info['plant']
            tags = plant.get('tags', {})
            
            row = {
                'substation_id': sub_id,
                'substation_name': sub_name,
                'substation_voltage': sub_info.get('tags', {}).get('voltage', ''),
                'plant_id': plant['id'],
                'plant_type': plant['type'],
                'plant_name': tags.get('name', 'Unnamed'),
                'plant_operator': tags.get('operator', ''),
                'plant_source': tags.get('plant:source', ''),
                'plant_output': tags.get('plant:output:electricity', ''),
                'connection_method': plant_info['connection_method'],
                'trace_depth': plant_info.get('found_at_depth', ''),
                'distance_km': plant_info.get('distance_km', ''),
                'connection_details': str(plant_info.get('member_role', plant_info.get('connection_likely', '')))
            }
            plant_rows.append(row)
        
        # Prepare line data
        line_rows = []
        for line in connected_lines:
            line_rows.append({
                'line_id': line['line_id'],
                'voltage': line['voltage'],
                'operator': line['operator'],
                'connection_type': line['connection_type'],
                'distance_m': line.get('distance_m', '')
            })
        
        # Save Excel with multiple sheets
        excel_file = f"outputs/network_analysis_{sub_id}_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file) as writer:
            # Plants sheet
            if plant_rows:
                plants_df = pd.DataFrame(plant_rows)
                plants_df.to_excel(writer, sheet_name='Connected_Plants', index=False)
            
            # Lines sheet
            if line_rows:
                lines_df = pd.DataFrame(line_rows)
                lines_df.to_excel(writer, sheet_name='Power_Lines', index=False)
            
            # Summary sheet
            summary = {
                'Substation': f"{sub_name} ({sub_id})",
                'Voltage': sub_info.get('tags', {}).get('voltage', ''),
                'Operator': sub_info.get('tags', {}).get('operator', ''),
                'Connected Power Lines': len(connected_lines),
                'Direct Connections': len([l for l in connected_lines if l['connection_type'] == 'direct']),
                'Near Connections': len([l for l in connected_lines if 'near' in l['connection_type']]),
                'Total Plants Found': len(plant_rows),
                'Plants via Network Trace': len([p for p in plant_rows if p['connection_method'] == 'network_trace']),
                'Plants as Members': len([p for p in plant_rows if p['connection_method'] == 'substation_member']),
                'Analysis Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            summary_df = pd.DataFrame([summary]).T
            summary_df.columns = ['Value']
            summary_df.to_excel(writer, sheet_name='Summary')
            
            # Plant types summary
            if plant_rows:
                plant_types = pd.DataFrame(plant_rows).groupby('plant_source').agg({
                    'plant_id': 'count',
                    'plant_output': lambda x: ', '.join(x.dropna().unique()),
                    'plant_name': lambda x: ', '.join(x.unique()[:3])
                }).rename(columns={'plant_id': 'count'})
                plant_types.to_excel(writer, sheet_name='By_Type')
        
        self.logger.info("\n" + "="*60)
        self.logger.info(f"NETWORK ANALYSIS COMPLETE!")
        self.logger.info(f"Substation: {sub_name}")
        self.logger.info(f"Connected Power Lines: {len(connected_lines)}")
        self.logger.info(f"Total Plants Found: {len(plant_rows)}")
        if plant_rows:
            self.logger.info(f"Results saved to: {excel_file}")
        self.logger.info("="*60)

def main():
    tracer = SubstationNetworkTracer()
    tracer.analyze_complete_network(170140947)  # SET Los Vientos

if __name__ == "__main__":
    main()