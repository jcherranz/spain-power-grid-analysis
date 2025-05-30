"""
Advanced analyzer that finds power line connections even when they connect
through terminals, bays, or connection points near the substation
"""

import requests
import pandas as pd
import json
from datetime import datetime
import os
import logging
import time
import math

class AdvancedSubstationAnalyzer:
    def __init__(self):
        """Initialize the analyzer"""
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/advanced_substation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
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
        self.timeout = 120
        
    def query_overpass(self, query):
        """Execute Overpass query with error handling"""
        try:
            response = requests.post(self.overpass_url, data=query, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            return None
    
    def get_substation_bbox(self, substation_id):
        """Get bounding box of substation for spatial queries"""
        query = f"""
        [out:json][timeout:30];
        way({substation_id});
        out bb;
        """
        
        data = self.query_overpass(query)
        if data and data['elements']:
            elem = data['elements'][0]
            bounds = elem.get('bounds', {})
            return {
                'minlat': bounds.get('minlat'),
                'minlon': bounds.get('minlon'),
                'maxlat': bounds.get('maxlat'),
                'maxlon': bounds.get('maxlon')
            }
        return None
    
    def find_all_connections(self, substation_id):
        """Find all power infrastructure connected or very close to substation"""
        # Get substation bounds
        bbox = self.get_substation_bbox(substation_id)
        if not bbox:
            return None
            
        # Expand bbox slightly to catch nearby connections
        expansion = 0.001  # ~100m
        bbox['minlat'] -= expansion
        bbox['minlon'] -= expansion
        bbox['maxlat'] += expansion
        bbox['maxlon'] += expansion
        
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Get the substation itself
          way({substation_id});
          node(w);
          
          // Get all power infrastructure in and around the substation
          node["power"]({bbox['minlat']},{bbox['minlon']},{bbox['maxlat']},{bbox['maxlon']});
          way["power"]({bbox['minlat']},{bbox['minlon']},{bbox['maxlat']},{bbox['maxlon']});
          
          // Get terminals, towers, and connection points
          node["power"~"terminal|tower|pole|portal"]({bbox['minlat']},{bbox['minlon']},{bbox['maxlat']},{bbox['maxlon']});
          
          // Get all nodes of power lines to check endpoints
          way["power"~"line|minor_line|cable"]({bbox['minlat']},{bbox['minlon']},{bbox['maxlat']},{bbox['maxlon']});
          node(w);
        );
        out body;
        """
        
        self.logger.info(f"Querying comprehensive infrastructure around substation...")
        return self.query_overpass(query)
    
    def analyze_connections(self, substation_id):
        """Analyze all connections to a substation"""
        self.logger.info("="*60)
        self.logger.info(f"Advanced analysis for substation ID: {substation_id}")
        self.logger.info("="*60)
        
        # Get all infrastructure data
        data = self.find_all_connections(substation_id)
        if not data:
            self.logger.error("Failed to get infrastructure data")
            return
            
        # Organize elements
        elements_by_id = {}
        substation_info = None
        power_lines = []
        terminals = []
        substation_nodes = set()
        
        for elem in data['elements']:
            elements_by_id[elem['id']] = elem
            
            if elem['type'] == 'way' and elem['id'] == substation_id:
                substation_info = elem
                substation_nodes = set(elem.get('nodes', []))
            elif elem['type'] == 'way' and elem.get('tags', {}).get('power') in ['line', 'minor_line', 'cable']:
                power_lines.append(elem)
            elif elem['type'] == 'node' and elem.get('tags', {}).get('power') in ['terminal', 'tower', 'pole', 'portal']:
                terminals.append(elem)
        
        if not substation_info:
            self.logger.error("Substation info not found")
            return
            
        sub_name = substation_info.get('tags', {}).get('name', 'Unknown')
        self.logger.info(f"Analyzing: {sub_name}")
        self.logger.info(f"Found {len(power_lines)} power lines in area")
        self.logger.info(f"Found {len(terminals)} terminals/towers")
        
        # Find connections
        connected_lines = []
        
        # Method 1: Direct node connections
        for line in power_lines:
            line_nodes = set(line.get('nodes', []))
            if line_nodes & substation_nodes:
                connected_lines.append({
                    'line': line,
                    'connection_type': 'direct_node',
                    'connection_point': list(line_nodes & substation_nodes)[0]
                })
        
        # Method 2: Lines ending at terminals near/in substation
        for line in power_lines:
            line_nodes = line.get('nodes', [])
            if line_nodes:
                # Check endpoints
                endpoints = [line_nodes[0], line_nodes[-1]]
                for endpoint in endpoints:
                    # Is this endpoint a terminal?
                    if endpoint in [t['id'] for t in terminals]:
                        connected_lines.append({
                            'line': line,
                            'connection_type': 'via_terminal',
                            'connection_point': endpoint
                        })
                    # Is this endpoint inside substation bounds?
                    elif endpoint in elements_by_id:
                        node = elements_by_id[endpoint]
                        if self.is_node_near_substation(node, substation_info, elements_by_id):
                            connected_lines.append({
                                'line': line,
                                'connection_type': 'endpoint_near',
                                'connection_point': endpoint
                            })
        
        self.logger.info(f"Found {len(connected_lines)} connected lines")
        
        # Now trace each line to find plants
        all_connections = []
        processed_lines = set()
        
        for conn in connected_lines:
            line = conn['line']
            line_id = line['id']
            
            if line_id in processed_lines:
                continue
            processed_lines.add(line_id)
            
            line_tags = line.get('tags', {})
            self.logger.info(f"Tracing line {line_id} ({line_tags.get('voltage', 'Unknown')}V) - {conn['connection_type']}")
            
            # Trace to plants
            plants = self.trace_full_line(line_id)
            
            if plants:
                for plant in plants:
                    plant_tags = plant.get('tags', {})
                    connection = {
                        'substation_id': substation_id,
                        'substation_name': sub_name,
                        'substation_voltage': substation_info.get('tags', {}).get('voltage', ''),
                        'substation_operator': substation_info.get('tags', {}).get('operator', ''),
                        'line_id': line_id,
                        'line_voltage': line_tags.get('voltage', ''),
                        'line_name': line_tags.get('name', ''),
                        'line_ref': line_tags.get('ref', ''),
                        'connection_type': conn['connection_type'],
                        'plant_id': plant['id'],
                        'plant_name': plant_tags.get('name', 'Unnamed Plant'),
                        'plant_operator': plant_tags.get('operator', ''),
                        'plant_source': plant_tags.get('plant:source', ''),
                        'plant_output': plant_tags.get('plant:output:electricity', '')
                    }
                    all_connections.append(connection)
                    self.logger.info(f"  -> Connected to: {connection['plant_name']}")
            
            time.sleep(0.3)  # Be nice to the API
        
        # Save results
        self.save_results(all_connections, substation_info, connected_lines)
    
    def is_node_near_substation(self, node, substation, all_elements):
        """Check if a node is very close to substation"""
        # Simple check - within ~50m of any substation node
        if 'lat' not in node or 'lon' not in node:
            return False
            
        for sub_node_id in substation.get('nodes', []):
            if sub_node_id in all_elements:
                sub_node = all_elements[sub_node_id]
                if 'lat' in sub_node and 'lon' in sub_node:
                    dist = self.calculate_distance(
                        node['lat'], node['lon'],
                        sub_node['lat'], sub_node['lon']
                    )
                    if dist < 0.05:  # ~50m
                        return True
        return False
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance in km between two points"""
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    
    def trace_full_line(self, line_id):
        """Trace a line through the network to find connected plants"""
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Start with the line
          way({line_id});
          
          // Get connected lines recursively (up to 3 levels)
          way(bn)["power"~"line|minor_line|cable"];
          way(bn)["power"~"line|minor_line|cable"];
          way(bn)["power"~"line|minor_line|cable"];
          
          // Get all nodes
          node(w);
          
          // Find connected plants
          way(bn)["power"="plant"];
        );
        out body;
        """
        
        data = self.query_overpass(query)
        if data:
            plants = [e for e in data['elements'] 
                     if e['type'] == 'way' and e.get('tags', {}).get('power') == 'plant']
            return plants
        return []
    
    def save_results(self, connections, substation_info, connected_lines):
        """Save comprehensive results"""
        os.makedirs("outputs", exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        sub_id = substation_info['id']
        
        if connections:
            # Save full connection details
            df = pd.DataFrame(connections)
            df['analysis_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            excel_file = f"outputs/advanced_substation_{sub_id}_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                df.to_excel(writer, sheet_name='Plant_Connections', index=False)
                
                # Summary
                summary = {
                    'Substation ID': sub_id,
                    'Substation Name': substation_info.get('tags', {}).get('name', 'Unknown'),
                    'Connected Plants': len(df['plant_id'].unique()),
                    'Connected Lines': len(set(c['line']['id'] for c in connected_lines)),
                    'Direct Connections': len([c for c in connected_lines if c['connection_type'] == 'direct_node']),
                    'Terminal Connections': len([c for c in connected_lines if c['connection_type'] == 'via_terminal']),
                    'Nearby Connections': len([c for c in connected_lines if c['connection_type'] == 'endpoint_near'])
                }
                pd.DataFrame([summary]).to_excel(writer, sheet_name='Summary', index=False)
                
                # Line details
                line_df = pd.DataFrame([{
                    'line_id': c['line']['id'],
                    'voltage': c['line'].get('tags', {}).get('voltage', ''),
                    'operator': c['line'].get('tags', {}).get('operator', ''),
                    'name': c['line'].get('tags', {}).get('name', ''),
                    'connection_type': c['connection_type']
                } for c in connected_lines])
                line_df.to_excel(writer, sheet_name='Connected_Lines', index=False)
            
            self.logger.info(f"Saved results to {excel_file}")
        else:
            # Save line info even without plants
            line_df = pd.DataFrame([{
                'line_id': c['line']['id'],
                'connection_type': c['connection_type'],
                'voltage': c['line'].get('tags', {}).get('voltage', '')
            } for c in connected_lines])
            
            csv_file = f"outputs/substation_{sub_id}_lines_{timestamp}.csv"
            line_df.to_csv(csv_file, index=False)
            self.logger.info(f"No plants found, saved line info to {csv_file}")

def main():
    analyzer = AdvancedSubstationAnalyzer()
    analyzer.analyze_connections(170140947)  # SET Los Vientos

if __name__ == "__main__":
    main()