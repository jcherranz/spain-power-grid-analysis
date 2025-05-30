# Spain Power Grid Analysis

Extract and analyze operational power plants and substations in Spain using OpenStreetMap data.

## Features
- Extracts power infrastructure from OpenStreetMap
- Analyzes plant-to-substation connections
- Exports results to Excel and CSV
- Currently covers Madrid area (easily expandable)

## Quick Start
```bash
# Install dependencies
pip install -r requirements.txt

# Run analysis
python code/simple_power_analyzer.py
Results

Found 3 power plants and 85 substations in Madrid
Identified 135 likely connections based on proximity
Output saved to Excel with detailed analysis

Next Steps

Expand to all of Spain
Add more cities (Barcelona, Valencia, Seville)
Improve connection logic with network analysis
