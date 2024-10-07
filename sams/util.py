from datetime import datetime
from loguru import logger

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