from pathlib import Path
from hamilton.function_modifiers import (
    datasaver,
    parameterize,
    source,
    value,
    load_from,
    save_to,
    cache
)

from hamilton.io import utils
import pandas as pd
import sqlite3

from loguru import logger
import os
import json


db_path = "/home/sakshi/sams/data/raw/sams.db"

if os.path.exists(db_path):
    print("Database file found!")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
   
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    if tables:
        print("Tables in the database:")
        for table in tables:
            print(table[0])
    else:
        print("No tables found in the database.")
    conn.close()
else:
    print("Database file not found at the given path.")


conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Function to show column names of a table
def describe_table(table_name):
    try:
        print(f"\nVariable of the table: {table_name}")
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        for col in columns:
            print(f"{col[1]} ({col[2]})")
    except sqlite3.Error as e:
        print(f"Error describing table {table_name}: {e}")


# Function to show the number of records in a table
def count_records(table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    count = cursor.fetchone()[0]
    print(f"\nNumber of records in {table_name}: {count}")
    
# Describe both tables
describe_table("students")
count_records("students")

conn.close()

# to flatten the option_data json
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Fetch required columns from students table
cursor.execute("""
    SELECT 
        aadhar_no AS student_id,
        student_name,
        gender,
        district,
        social_category,
        highest_qualification,
        option_data
    FROM students
    WHERE option_data IS NOT NULL AND option_data != '[]'
""")
rows = cursor.fetchall()
conn.close()

# Flatten option_data
all_records = []
for row in rows:
    student_id, name, gender, district, social_cat, qualification, data = row
    try:
        options = json.loads(data)
        for opt in options:
            opt['aadhar_no'] = student_id
            opt['student_name'] = name
            opt['gender'] = gender
            opt['district'] = district
            opt['social_category'] = social_cat
            opt['highest_qualification'] = qualification
                        
            all_records.append(opt)
    except json.JSONDecodeError:
        continue

# Create DataFrame
df = pd.DataFrame(all_records)


#cleaning the data
def clean_option_applications_df(sams_option_raw_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning data...")

    # Drop unnecessary columns
    df = df.drop_duplicates()
    df = df.dropna(subset=["student_name", "gender"])

    # Clean string fields
    df["Institute_Name"] = df["Institute_Name"].astype(str).str.strip()

    return df

    def option_summary_statistics(clean_option_applications_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Generating summary statistics by district...")

        summary = clean_option_applications_df.groupby("district").agg(
            total_applications=("student_id", "count"),
            unique_students=("student_id", "nunique"),
            unique_institutes=("Institute_Name", "nunique"),
            unique_options=("Option_No", "nunique")
        ).reset_index()

    
        #Calculating average applications per student
        summary["avg_applications_per_student"] = (
            summary["total_applications"] / summary["unique_students"]
        )

        logger.info(f"Summary Statistics:\n{summary}")

        return summary
