import pandas as pd
import sqlite3
import json
from typing import Dict, List

# Root node to extract option data from the database
def option_data() -> pd.DataFrame:
    db_path = "/home/sakshi/sams/data/raw/sams.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            barcode,
            aadhar_no,
            gender,
            district,
            module,
            highest_qualification,
            had_two_year_full_time_work_exp_after_tenth,
            year,      
            option_data
        FROM students
        WHERE option_data IS NOT NULL AND option_data != '[]' 
    """)
    rows = cursor.fetchall()
    conn.close()

    all_records = []
    for row in rows:
        barcode, aadhar_no, gender, district, module, qualification, work_exp, year, data = row
        try:
            options = json.loads(data)
            for opt in options:
                opt.update({
                    "barcode": barcode,
                    "aadhar_no": aadhar_no,
                    "gender": gender,
                    "district": district,
                    "module": module,
                    "qualification": qualification,
                    "work_exp": work_exp,
                    "year": year
                })
                all_records.append(opt)
        except json.JSONDecodeError:
            continue

    df = pd.DataFrame(all_records)
    df.rename(columns={
        'had_two_year_full_time_work_exp_after_tenth': 'work_exp',
        'highest_qualification': 'qualification'
    }, inplace=True)

    if not df.empty:
        cols = ['aadhar_no', 'barcode', 'year'] + [c for c in df.columns if c not in ['aadhar_no', 'barcode', 'year']]
        df = df[cols]

    return df

# Grouping Nodes
def by_trade(option_data: pd.DataFrame) -> pd.DataFrame:
    df = option_data.dropna(subset=['year', 'Trade', 'module'])
    df['year'] = df['year'].astype(int)
    return df

def by_year(option_data: pd.DataFrame) -> pd.DataFrame:
    df = option_data.dropna(subset=['year'])
    df['year'] = df['year'].astype(int)
    return df.groupby(['year', 'barcode']).size().reset_index(name='application_count')

def by_aadhar(option_data: pd.DataFrame) -> pd.DataFrame:
    df = option_data.copy()
    return df[df['module'].isin(['ITI', 'Diploma'])]

# Metric Nodes
def avg_applicants(by_year: pd.DataFrame) -> pd.DataFrame:
    return by_year.groupby('year')['application_count'].mean().reset_index(name='avg_apps')

def total_applicants(by_year: pd.DataFrame) -> pd.DataFrame:
    return by_year.groupby('year')['barcode'].nunique().reset_index(name='total_applications')

def demand(by_trade: pd.DataFrame) -> pd.DataFrame:
    yearly = by_trade.groupby('year').agg(
        total_applications=('Trade', 'size'),
        distinct_trades=('Trade', 'nunique')
    ).reset_index()

    trade_mod = by_trade.groupby(['year', 'Trade', 'module']).size().unstack(fill_value=0)
    trade_mod['total_count'] = trade_mod.sum(axis=1)
    trade_mod['demand_trade_module'] = trade_mod.idxmax(axis=1)

    top1 = trade_mod.reset_index().sort_values(['year', 'total_count'], ascending=[True, False])\
        .groupby('year', as_index=False).first()[['year', 'Trade', 'total_count', 'demand_trade_module']]\
        .rename(columns={'Trade': 'demand_trade', 'total_count': 'demand_trade_count'})

    return yearly.merge(top1, on='year')


def different_institute_allotment(total_applicants: pd.DataFrame) -> pd.DataFrame:
    def get_first_valid_reported(x):
        x = x[~x.isin(['', 'None'])].dropna()
        return x.iloc[0] if not x.empty else 'None'

    student_summary = option_data.groupby(['aadhar_no', 'year']).agg({
        'Institute_Name': 'first',
        'reported_institute': get_first_valid_reported
    }).reset_index()

    def classify_status(row):
        if row['reported_institute'] in ['None', '']:
            return 'No Allotment'
        elif row['Institute_Name'] == row['reported_institute']:
            return 'Same Institute'
        else:
            return 'Different Institute'

    student_summary['status'] = student_summary.apply(classify_status, axis=1)

    result = student_summary[student_summary['status'] == 'Different Institute']
    return result.groupby('year')['aadhar_no'].nunique().reset_index(name='different_institute_allotment')

def no_allotment(total_applicants: pd.DataFrame) -> pd.DataFrame:
    def get_first_valid_reported(x):
        x = x[~x.isin(['', 'None'])].dropna()
        return x.iloc[0] if not x.empty else 'None'

    student_summary = option_data.groupby(['aadhar_no', 'year']).agg({
        'reported_institute': get_first_valid_reported
    }).reset_index()

    no_allot = student_summary[student_summary['reported_institute'] == 'None']
    return no_allot.groupby('year')['aadhar_no'].nunique().reset_index(name='no_allotment')

def invalid_aadhar(by_aadhar: pd.DataFrame, counts_to_check: List[int] = [2,3,4,5]) -> Dict[int, pd.DataFrame]:
    df = by_aadhar.copy()
    total_counts = df.groupby(['module', 'year'])['aadhar_no'].nunique().reset_index(name='total_aadhar')
    barcode_counts = df.groupby(['module', 'year', 'aadhar_no'])['barcode'].nunique().reset_index(name='barcode_count')

    results = {}
    for count in counts_to_check:
        exact_match = barcode_counts[barcode_counts['barcode_count'] == count]
        exact_counts = exact_match.groupby(['module', 'year'])['aadhar_no'].nunique().reset_index(name='aadhar_count')
        merged = pd.merge(total_counts, exact_counts, on=['module', 'year'], how='left')
        merged['aadhar_count'] = merged['aadhar_count'].fillna(0).astype(int)
        merged['percent'] = (merged['aadhar_count'] / merged['total_aadhar'] * 100).round(2)
        results[count] = merged



def repeated_iti(by_aadhar: pd.DataFrame, min_barcodes: int = 2) -> pd.DataFrame:
    iti_data = by_aadhar[by_aadhar['module'] == 'ITI']
    barcode_counts = iti_data.groupby('aadhar_no')['barcode'].nunique().reset_index(name='barcode_count')
    suspicious = barcode_counts[barcode_counts['barcode_count'] >= min_barcodes]
    return suspicious


