from pathlib import Path
import pandas as pd
import sqlite3
from loguru import logger
import os
import json


db_path = "/home/sakshi/sams/data/raw/sams.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

## Fetch selected columns from students table where module is ITI and option_data is not empty
cursor.execute("""
    SELECT 
        barcode,
        aadhar_no,
        gender,
        district,
        highest_qualification,
        had_two_year_full_time_work_exp_after_tenth,
        year,      
        option_data
    FROM students
    WHERE option_data IS NOT NULL AND option_data != '[]' AND module = 'ITI'
""")
rows = cursor.fetchall()
conn.close()

# Flatten option_data and enrich with student details
all_records = []
for row in rows:
    barcode, aadhar_no, gender, district, qualification, work_exp, year, data = row
    try:
        options = json.loads(data)
        for opt in options:
            opt['barcode'] = barcode
            opt['aadhar_no'] = aadhar_no
            opt['gender'] = gender
            opt['district'] = district
            opt['highest_qualification'] = qualification
            opt['had_two_year_full_time_work_exp_after_tenth'] = work_exp
            opt['year'] = year
            all_records.append(opt)
    except json.JSONDecodeError:
        continue
option_data = pd.DataFrame(all_records)

# Rename columns
option_data.rename(columns={'had_two_year_full_time_work_exp_after_tenth': 'work_exp',
                   'highest_qualification': 'qualification'}, inplace=True)

# Reorder columns so that 'aadhar_no', 'barcode', 'year' appear first
if not option_data.empty:
    cols = ['aadhar_no', 'barcode', 'year'] + [c for c in option_data.columns if c not in ['aadhar_no', 'barcode', 'year']]
    df = option_data[cols]


#Calculate total applications by year
def total_applications_by_year(option_data: pd.DataFrame) -> pd.DataFrame:
    option_data = option_data.dropna(subset=["year"])
    option_data["year"] = option_data["year"].astype(int)
    return option_data.groupby("year").size().reset_index(name="total_applications_by_year")


# Calculate average number of applications per student by year using barcode
def average_applications_per_student(option_data: pd.DataFrame) -> pd.DataFrame:
    df = option_data.groupby(["year", "barcode"]).size().reset_index(name="application_count")
    return (
        df.groupby("year")["application_count"]
        .mean()
        .reset_index()
        .rename(columns={"application_count": "average_applications_per_student"})
    )

#print(total_applications_by_year(option_data))
#print(average_applications_per_student(option_data))

# Calculate average number of applications per student by year using aadhar_no
option_data_avg = option_data.dropna(subset=["year"])
applications_per_student_per_year = (
    option_data_avg.groupby(["year", 'aadhar_no']).size().reset_index(name="applications_per_student")
)
avg_applications_by_year_option_data = (
    applications_per_student_per_year.groupby("year")["applications_per_student"]
    .mean()
    .reset_index(name="averag_applications_per_student")
)
print(avg_applications_by_year_option_data)


# Count unique Aadhar numbers and barcodes
unique_aadhar = option_data['aadhar_no'].nunique()
unique_barcode = option_data['barcode'].nunique()
print(f"Unique Aadhar Numbers: {unique_aadhar}")
print(f"Unique Barcodes: {unique_barcode}")

# Barcodes mapped to multiple Aadhar numbers
barcode_to_multiple_aadhar = option_data.groupby('barcode')['aadhar_no'].nunique()
invalid_barcodes = barcode_to_multiple_aadhar[barcode_to_multiple_aadhar > 1]
print(f"Barcodes with multiple Aadhar numbers: {len(invalid_barcodes)}")

# Aadhar numbers mapped to multiple barcodes
aadhar_to_multiple_barcodes = option_data.groupby('aadhar_no')['barcode'].nunique()
invalid_aadhars = aadhar_to_multiple_barcodes[aadhar_to_multiple_barcodes > 1]
print(f"Aadhar numbers linked to multiple barcodes: {len(invalid_aadhars)}")


# Count inconsistent aadhar-barcode links per year (where one aadhar is linked to multiple barcodes)
def count_inconsistent_aadhar_barcode_by_year(option_data):
    # Group by 'year' and 'aadhar_no', count unique 'barcode' for each 'aadhar_no' in each year
    result = option_data.groupby(['year', 'aadhar_no'])['barcode'].nunique()
    
    # Filter for inconsistent aadhar_no (more than one barcode per aadhar_no in the same year)
    inconsistent = result[result > 1]
    
    # Count inconsistencies per year
    inconsistencies_by_year = inconsistent.groupby('year').count()
    all_years = option_data['year'].sort_values().unique()
    inconsistencies_by_year = inconsistencies_by_year.reindex(all_years, fill_value=0)

    return inconsistencies_by_year.reset_index(name='inconsistent_aadhar_count')

inconsistent_by_year = count_inconsistent_aadhar_barcode_by_year(option_data)
inconsistent_by_year


# Get the first 5 aadhar numbers linked to multiple barcodes
sample_invalid_aadhars = invalid_aadhars.head(5).index

for aadhar in sample_invalid_aadhars:
    barcodes = option_data[option_data['aadhar_no'] == aadhar]['barcode'].unique()
    barcode_list = "', '".join(barcodes)
    print(f"Aadhar number '{aadhar}' is linked to {len(barcodes)} barcodes: ['{barcode_list}']")
