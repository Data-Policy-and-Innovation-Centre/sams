import pandas as pd
import sqlite3
import json    
from typing import Dict, List


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
                opt["barcode"] = barcode
                opt["aadhar_no"] = aadhar_no
                opt["gender"] = gender
                opt["district"] = district
                opt["module"] = module
                opt["highest_qualification"] = qualification
                opt["had_two_year_full_time_work_exp_after_tenth"] = work_exp
                opt["year"] = year
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


# Intermediate grouping nodes

def by_year(option_data: pd.DataFrame) -> pd.DataFrame:
    """Group option_data by year and barcode for avg apps calculation."""
    df = option_data.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    return df.groupby(["year", "barcode"]).size().reset_index(name="application_count")


def by_trade(option_data: pd.DataFrame) -> pd.DataFrame:
    """Group option_data by year and Trade for demand calculation."""
    df = option_data.dropna(subset=['year', 'Trade', 'module'])
    df["year"] = df["year"].astype(int)
    return df


def by_aadhar(option_data: pd.DataFrame) -> pd.DataFrame:
    """Filtered data for Aadhar-related invalid checks."""
    df = option_data.copy()
    return df[df['module'].isin(['ITI', 'Diploma'])]


# Metric nodes
def total_applicants(by_year: pd.DataFrame) -> pd.DataFrame:
    """Total applications by year (direct from option_data)."""
    df = option_data.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    return df.groupby("year").size().reset_index(name="total_applications")

def avg_applicants(by_year: pd.DataFrame) -> pd.DataFrame:
    """Average applications per student by year."""
    return by_year.groupby("year")["application_count"].mean().reset_index(name="avg_apps")


def demand(by_trade: pd.DataFrame) -> pd.DataFrame:
    """Demand by year and trade."""
    yearly = by_trade.groupby('year').agg(
        total_applications=('Trade', 'size'),
        distinct_trades=('Trade', 'nunique')
    ).reset_index()

    trade_mod = by_trade.groupby(['year', 'Trade', 'module']).size().unstack(fill_value=0)
    trade_mod['total_count'] = trade_mod.sum(axis=1)
    trade_mod['demand_trade_module'] = trade_mod.idxmax(axis=1)

    top1 = (trade_mod
        .reset_index()
        .sort_values(['year', 'total_count'], ascending=[True, False])
        .groupby('year', as_index=False)
        .first()[['year', 'Trade', 'total_count', 'demand_trade_module']]
        .rename(columns={'Trade': 'demand_trade', 'total_count': 'demand_trade_count'})
    )

    return yearly.merge(top1, on='year')


def invalid_aadhar(by_aadhar: pd.DataFrame, counts_to_check: List[int] = [2,3,4,5,3052,8405,10970]) -> Dict[int, pd.DataFrame]:
    """Calculate invalid Aadhar counts by year for given counts."""
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
        summary = merged.pivot(index='year', columns='module', values=['aadhar_count', 'total_aadhar', 'percent']).fillna(0)
        summary.columns = [f"{mod}_{metric}" for metric, mod in summary.columns]
        summary = summary.reset_index()
        filtered = summary[(summary['ITI_aadhar_count'] > 0) | (summary['Diploma_aadhar_count'] > 0)]
        results[count] = filtered

    return results


def repeated_iti(by_aadhar: pd.DataFrame, min_barcodes: int = 2) -> pd.DataFrame:
    """Identify suspicious ITI candidates with multiple barcodes."""
    iti_data = by_aadhar[by_aadhar['module'] == 'ITI']
    barcode_counts = iti_data.groupby('aadhar_no')['barcode'].nunique().reset_index(name='barcode_count')
    suspicious = barcode_counts[barcode_counts['barcode_count'] >= min_barcodes]
    detailed = iti_data[iti_data['aadhar_no'].isin(suspicious['aadhar_no'])]

    def safe_unique(x):
        return sorted(x.unique()) if 'Institute_Name' in iti_data.columns else 'N/A'

    summary = detailed.groupby(['aadhar_no']).agg(
        total_barcodes=('barcode', pd.Series.nunique),
        total_years=('year', pd.Series.nunique),
        sample_years=('year', lambda x: sorted(x.unique())),
        total_rows=('barcode', 'count'),
        institutions=('Institute_Name', safe_unique)
    ).reset_index()

    return summary.sort_values(by='total_barcodes', ascending=False)
