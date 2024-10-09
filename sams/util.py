from datetime import datetime
from loguru import logger
from sams.config import RAW_DATA_DIR, LOGS
import os
import sqlite3
import pandas as pd

def is_valid_date(date_string):
    formats = [
        "%Y-%m-%d",          # Format 1: 2024-08-26
        "%d-%m-%Y",          # Format 2: 26-08-2024
        "%m/%d/%Y",          # Format 3: 08/26/2024
        "%d %b %Y",          # Format 4: 26 Aug 2024
        "%B %d, %Y",         # Format 5: August 26, 2024
        "%Y-%m-%d %H:%M:%S", # Format 6: 2024-08-26 15:30:00
        # Add more formats as needed
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            return True, parsed_date  # Date is valid, return the parsed date
        except ValueError:
            continue  # Try the next format
    
    return False, None  # No formats matched, date is invalid   

def get_existing_modules(table_name: str = "students") -> list:

    if table_name not in ["students", "institutes"]:
        raise ValueError(f"Invalid table name: {table_name}")
    
    db_path = os.path.join(RAW_DATA_DIR, "sams.db")
    counts_path = os.path.join(LOGS, table_name + "_count.csv")

    if not os.path.exists(db_path):
        return []
    
    if not os.path.exists(counts_path):
        return []
    
    counts = pd.read_csv(counts_path)
    
    if table_name == "students":
        query = """SELECT module, academic_year, COUNT(*) AS num_observations 
                    FROM students 
                    GROUP BY module, academic_year"""
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(query)
            existing_modules = cur.fetchall()

        existing_modules = [list(row) for row in existing_modules]
        existing_modules = [(module, year) for module, year, num_obs in existing_modules 
                            if num_obs == counts[(counts["module"] == module) & (counts["academic_year"] == year)]["count"].values[0]]
        return existing_modules
    
    else:
        return []

    
