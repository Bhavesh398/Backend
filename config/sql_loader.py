"""
Data File Loader
Loads satellite and debris data from CSV files when Supabase is unavailable
"""
import csv
from pathlib import Path
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


def load_satellites_from_file() -> List[Dict]:
    """Load satellite data from CSV or JSON file"""
    import json
    
    # Try CSV first
    csv_file = DATA_DIR / "satellites.csv"
    if csv_file.exists():
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = []
                for row in reader:
                    # Convert numeric fields
                    record = {}
                    for key, value in row.items():
                        if value == '' or value.lower() == 'null':
                            record[key] = None
                        elif key in ['sat_x', 'sat_y', 'sat_z', 'sat_vx', 'sat_vy', 'sat_vz', 
                                     'altitude', 'latitude', 'longitude', 'velocity_kmps', 
                                     'inclination', 'period', 'mass', 'altitude_km']:
                            try:
                                record[key] = float(value)
                            except (ValueError, TypeError):
                                record[key] = value
                        elif key in ['sat_id', 'norad_id']:
                            try:
                                record[key] = int(value) if value.isdigit() else value
                            except (ValueError, TypeError):
                                record[key] = value
                        else:
                            record[key] = value
                    records.append(record)
            
            logger.info(f"Loaded {len(records)} satellites from CSV file")
            return records
        except Exception as e:
            logger.error(f"Error loading satellites from CSV: {e}")
    
    # Fall back to JSON
    json_file = DATA_DIR / "satellites.json"
    if json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                records = json.load(f)
            logger.info(f"Loaded {len(records)} satellites from JSON file")
            return records
        except Exception as e:
            logger.error(f"Error loading satellites from JSON: {e}")
    
    logger.warning("No satellite data files found (CSV or JSON)")
    return []


def load_debris_from_file() -> List[Dict]:
    """Load debris data from CSV or JSON file"""
    import json
    
    # Try CSV first
    csv_file = DATA_DIR / "debris.csv"
    if csv_file.exists():
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = []
                for row in reader:
                    # Convert numeric fields
                    record = {}
                    for key, value in row.items():
                        if value == '' or value.lower() == 'null':
                            record[key] = None
                        elif key in ['deb_x', 'deb_y', 'deb_z', 'deb_vx', 'deb_vy', 'deb_vz',
                                     'altitude', 'latitude', 'longitude', 'velocity_kmps',
                                     'size_estimate_m', 'altitude_km']:
                            try:
                                record[key] = float(value)
                            except (ValueError, TypeError):
                                record[key] = value
                        else:
                            record[key] = value
                    records.append(record)
            
            logger.info(f"Loaded {len(records)} debris from CSV file")
            return records
        except Exception as e:
            logger.error(f"Error loading debris from CSV: {e}")
    
    # Fall back to JSON
    json_file = DATA_DIR / "debris.json"
    if json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                records = json.load(f)
            logger.info(f"Loaded {len(records)} debris from JSON file")
            return records
        except Exception as e:
            logger.error(f"Error loading debris from JSON: {e}")
    
    logger.warning("No debris data files found (CSV or JSON)")
    return []


# Legacy function names for backwards compatibility
load_satellites_from_sql = load_satellites_from_file
load_debris_from_sql = load_debris_from_file
