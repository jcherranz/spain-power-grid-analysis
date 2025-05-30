"""
Spain Power Grid Analysis - Simple Power Plant to Substation Analyzer
Extracts operational power infrastructure from OpenStreetMap
"""

import requests
import pandas as pd
import json
from datetime import datetime
import os
import logging
import time

class SpainPowerAnalyzer:
    def __init__(self):
        """Initialize the analyzer with configuration"""
        # Setup logging
        log_file = f"logs/analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        os.makedirs("../logs", exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.overpass_url = "http://overpass-api.de/api/interpreter"
        self.timeout = 60
        
        # Test area: Madrid Metropolitan Area (small area for quick testing)
        self.test_area = {
            'name': 'Madrid_Metropolitan_Area',
            'bbox': '40.3,-3.8,40.5,-3.6'  # South,West,North,East
        }
        
        self.logger.info("Spain Power Analyzer initialized")
        self.logger.info(f"Test area: {self.test_area['name']}")
        
    def query_osm_data(self, query):
        """Execute Overpass query and return results"""
        self.logger.info("Executing Overpass query...")
        try:
            response = requests.post(
                self.overpass_url,
                data=query,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            return None
    
    def extract_power_infrastructure(self):
        """Extract power plants, substations, and power lines from OSM"""
        bbox = self.test_area['bbox']
        
        # Query for all power infrastructure with center coordinates
        query = f"""
        [out:json][timeout:{self.timeout}];
        (
          // Power plants
          node["power"="plant"][!"proposed"][!"construction"]({bbox});
          way["power"="plant"][!"proposed"][!"construction"]({bbox});
          relation["power"="plant"][!"proposed"][!"construction"]({bbox});
          
          // Substations
          node["power"="substation"][!"proposed"][!"construction"]({bbox});
          way["power"="substation"][!"proposed"][!"construction"]({bbox});
          relation["power"="substation"][!"proposed"][!"construction"]({bbox});
          
          // Power lines (for reference)
          way["power"~"line|minor_line|cable"][!"proposed"][!"construction"]({bbox});
        );
        out center;
        """
        
        self.logger.info(f"Querying area: {bbox}")
        data = self.query_osm_data(query)
        
        if not data:
            return None, None, None
            
        # Process results
        elements = data.get('elements', [])
        self.logger.info(f"Retrieved {len(elements)} total elements")
        
        # Separate into categories
        plants = []
        substations = []
        lines_count = 0
        
        for elem in elements:
            tags = elem.get('tags', {})
            
            if tags.get('power') == 'plant':
                plants.append(self.process_plant(elem))
            elif tags.get('power') == 'substation':
                substations.append(self.process_substation(elem))
            elif tags.get('power') in ['line', 'minor_line', 'cable']:
                lines_count += 1
        
        self.logger.info(f"Found: {len(plants)} plants, {len(substations)} substations, {lines_count} power lines")
        
        return pd.DataFrame(plants), pd.DataFrame(substations), lines_count
    
    def process_plant(self, element):
        """Extract relevant plant information"""
        tags = element.get('tags', {})
        
        # Get coordinates
        lat, lon = self.get_coordinates(element)
        
        return {
            'id': element.get('id'),
            'name': tags.get('name', 'Unnamed Plant'),
            'operator': tags.get('operator', ''),
            'source': tags.get('plant:source', tags.get('generator:source', '')),
            'output': tags.get('plant:output:electricity', tags.get('generator:output:electricity', '')),
            'lat': lat,
            'lon': lon,
            'voltage': tags.get('voltage', ''),
            'type': 'plant'
        }
    
    def process_substation(self, element):
        """Extract relevant substation information"""
        tags = element.get('tags', {})
        
        # Get coordinates
        lat, lon = self.get_coordinates(element)
        
        return {
            'id': element.get('id'),
            'name': tags.get('name', 'Unnamed Substation'),
            'operator': tags.get('operator', ''),
            'voltage': tags.get('voltage', ''),
            'lat': lat,
            'lon': lon,
            'substation_type': tags.get('substation', ''),
            'type': 'substation'
        }
    
    def get_coordinates(self, element):
            """Extract coordinates from OSM element"""
            if element['type'] == 'node':
                return element.get('lat'), element.get('lon')
            elif 'center' in element:
                return element['center'].get('lat'), element['center'].get('lon')
            else:
                # For ways without center, return None
                return None, None
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Simple distance calculation in km"""
        if None in [lat1, lon1, lat2, lon2]:
            return None
        
        # Haversine formula (simplified)
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth radius in km
        
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    def analyze_connections(self, plants_df, substations_df):
        """Analyze likely connections between plants and substations"""
        connections = []
        
        if plants_df.empty or substations_df.empty:
            self.logger.warning("No plants or substations found for connection analysis")
            return pd.DataFrame()
        
        for _, plant in plants_df.iterrows():
            for _, substation in substations_df.iterrows():
                distance = self.calculate_distance(
                    plant['lat'], plant['lon'],
                    substation['lat'], substation['lon']
                )
                
                if distance is not None:
                    # Simple heuristic: closer = more likely connected
                    likely = "Yes" if distance < 10 else "Maybe" if distance < 25 else "Unlikely"
                    
                    connections.append({
                        'plant_id': plant['id'],
                        'plant_name': plant['name'],
                        'plant_operator': plant['operator'],
                        'plant_source': plant['source'],
                        'substation_id': substation['id'],
                        'substation_name': substation['name'],
                        'substation_operator': substation['operator'],
                        'substation_voltage': substation['voltage'],
                        'distance_km': round(distance, 2),
                        'connection_likely': likely,
                        'plant_lat': plant['lat'],
                        'plant_lon': plant['lon'],
                        'substation_lat': substation['lat'],
                        'substation_lon': substation['lon']
                    })
        
        return pd.DataFrame(connections)
    
    def run_analysis(self):
        """Main analysis execution"""
        self.logger.info("="*60)
        self.logger.info("Starting Spain Power Grid Analysis")
        self.logger.info("="*60)
        
        start_time = time.time()
        
        # Extract data
        plants_df, substations_df, lines_count = self.extract_power_infrastructure()
        
        if plants_df is None:
            self.logger.error("Failed to retrieve data from OpenStreetMap")
            return
        
        # Analyze connections
        connections_df = self.analyze_connections(plants_df, substations_df)
        
        # Prepare summary
        summary_data = {
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'test_area': self.test_area['name'],
            'bbox': self.test_area['bbox'],
            'total_plants': len(plants_df) if plants_df is not None else 0,
            'total_substations': len(substations_df) if substations_df is not None else 0,
            'power_lines_in_area': lines_count,
            'likely_connections': len(connections_df[connections_df['connection_likely'] == 'Yes']) if not connections_df.empty else 0,
            'maybe_connections': len(connections_df[connections_df['connection_likely'] == 'Maybe']) if not connections_df.empty else 0,
            'runtime_seconds': round(time.time() - start_time, 2)
        }
        
        # Save results
        self.save_results(plants_df, substations_df, connections_df, summary_data)
        
        # Log summary
        self.logger.info("="*60)
        self.logger.info("Analysis Complete!")
        self.logger.info(f"Plants found: {summary_data['total_plants']}")
        self.logger.info(f"Substations found: {summary_data['total_substations']}")
        self.logger.info(f"Likely connections: {summary_data['likely_connections']}")
        self.logger.info(f"Runtime: {summary_data['runtime_seconds']} seconds")
        self.logger.info("="*60)
    
    def save_results(self, plants_df, substations_df, connections_df, summary):
        """Save results to CSV and Excel files"""
        output_dir = "outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save main analysis (connections)
        if not connections_df.empty:
            # Add summary info to connections
            connections_df['analysis_date'] = summary['analysis_date']
            connections_df['test_area'] = summary['test_area']
            
            # Save as Excel
            excel_file = f"{output_dir}/spain_power_analysis_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                # Main analysis
                connections_df.to_excel(writer, sheet_name='Connections', index=False)
                
                # Raw data
                if not plants_df.empty:
                    plants_df.to_excel(writer, sheet_name='Plants', index=False)
                if not substations_df.empty:
                    substations_df.to_excel(writer, sheet_name='Substations', index=False)
                
                # Summary
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            self.logger.info(f"Excel saved: {excel_file}")
            
            # Also save main results as CSV
            csv_file = f"{output_dir}/connections_{timestamp}.csv"
            connections_df.to_csv(csv_file, index=False)
            self.logger.info(f"CSV saved: {csv_file}")
        else:
            self.logger.warning("No connections found to save")
            
            # Save whatever we found
            if not plants_df.empty:
                plants_df.to_csv(f"{output_dir}/plants_only_{timestamp}.csv", index=False)
            if not substations_df.empty:
                substations_df.to_csv(f"{output_dir}/substations_only_{timestamp}.csv", index=False)

def main():
    """Main execution function"""
    analyzer = SpainPowerAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
