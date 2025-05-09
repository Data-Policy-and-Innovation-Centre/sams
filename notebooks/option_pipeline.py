import pandas as pd
import sqlite3
import json

def option_data() -> pd.DataFrame:
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

    #Flatten option_data and enrich with student details
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


###DAG


# Calculate total applications by year


def total_applications_by_year(option_data: pd.DataFrame) -> pd.DataFrame:
    option_data = option_data.dropna(subset=["year"])
    option_data["year"] = option_data["year"].astype(int)
    return option_data.groupby("year").size().reset_index(name="total_applications_by_year")


def average_applications_per_student(option_data: pd.DataFrame) -> pd.DataFrame:
    df = option_data.groupby(["year", "barcode"]).size().reset_index(name="application_count")
    return (
        df.groupby("year")["application_count"]
        .mean()
        .reset_index()
        .rename(columns={"application_count": "average_applications_per_student"})
    )



def demand_by_year(option_data: pd.DataFrame) -> pd.DataFrame:
    # 1. Clean & ensure year is int
    df = (
        option_data
        .dropna(subset=['year','Trade','module'])
        .assign(year=lambda d: d.year.astype(int))
    )

    # 2. Yearly totals & distinct trades
    yearly = (
        df.groupby('year')
          .agg(
              total_applications_by_year=('Trade','size'),
              distinct_trades_by_year=('Trade','nunique')
          )
          .reset_index()
    )

    # 3. Per‐trade, per‐module counts → wide
    trade_mod = (
        df.groupby(['year','Trade','module'])
          .size()
          .unstack(fill_value=0)
    )
    # 4. Add total_count and identify top module
    trade_mod['total_count'] = trade_mod.sum(axis=1)
    trade_mod['demand_trade_module'] = trade_mod[['ITI','Diploma','PDIS']].idxmax(axis=1)

    # 5. Pick the top trade each year
    top1 = (
        trade_mod
        .reset_index()
        .sort_values(['year','total_count'], ascending=[True,False])
        .groupby('year', as_index=False)
        .first()[['year','Trade','total_count','demand_trade_module']]
        .rename(columns={
            'Trade': 'demand_trade',
            'total_count': 'demand_trade_count'
        })
    )

    # 6. Merge and return
    return yearly.merge(top1, on='year')