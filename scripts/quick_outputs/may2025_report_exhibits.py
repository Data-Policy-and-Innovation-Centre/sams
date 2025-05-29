# %%
import pandas as pd
from sams.config import datasets, SCTEVT_DIR, TABLES_DIR
import glob
import os
from loguru import logger
import re
from sams.utils import load_data
import sqlite3 as sqlite
from sams.config import SAMS_DB
from sams.analysis.utils import save_table_excel
 


# %% [markdown]
# # Data Preparation 

# %%
# List all xlsx files in the SCTEVT_DIR
xlsx_files = glob.glob(os.path.join(SCTEVT_DIR / "ITI_admission_and_results", "*.xlsx"))
sorted(xlsx_files)

# %%

# Convert all xlsx files in a directory to csv files
def convert_xlsx_to_csv(directory=SCTEVT_DIR / "ITI_admission_and_results"):
    xlsx_files = glob.glob(str(directory / "*.xlsx"))
    for xlsx_file in xlsx_files:
        csv_file = os.path.splitext(xlsx_file)[0] + ".csv"
        if not os.path.exists(csv_file):
            df = pd.read_excel(xlsx_file)
            logger.info(f"Converting {xlsx_file} to {csv_file}")
            df.to_csv(csv_file, index=False)
        

convert_xlsx_to_csv()



# %%


def load_admitted_trainee_csv(directory=SCTEVT_DIR / "ITI_admission_and_results"):
    files = glob.glob(str(directory / "AdmittedTrainee*.csv"))
    dfs = []
    for file in files:

        basename = file.split('/')[-1]
        year = basename.split("_")[0][-4:]
        if "_" not in basename:
            year = basename.split(".")[0][-4:]
        df = pd.read_csv(file)
        df['year'] = int(year)
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

admitted_trainees = load_admitted_trainee_csv(SCTEVT_DIR / "ITI_admission_and_results")

# %%

def load_exam_results_csv(directory=SCTEVT_DIR / "ITI_admission_and_results"):
    files = glob.glob(str(directory / "ExamResultSheet*.csv"))
    dfs = []
    for file in files:
        basename = file.split('/')[-1]
        year = basename.split("_")[1][:4]
        match = re.search(r'Year(\d{1})', basename)
        exam_year = int(match.group(1)) if match else None
        df = pd.read_csv(file)
        df['year'] = int(year)
        df['exam_year'] = int(exam_year)
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()
    
exam_result_sheet = load_exam_results_csv(SCTEVT_DIR / "ITI_admission_and_results")

# %%
def clean_exam_result_sheet(df: pd.DataFrame, path = SCTEVT_DIR / "ITI_admission_and_results/ODISHA-Complete Result of CTS of Session 2022& 2023.csv") -> pd.DataFrame:
    df = df[["Roll No", "Overall Result", "year", "exam_year"]].rename(columns={"Roll No": "roll_num", "Overall Result": "overall_result"})
    df['roll_num'] = df['roll_num'].astype(str).str.strip("'")
    df['overall_result'] = df['overall_result'].apply(lambda x: x.split(" ")[0] if isinstance(x, str) else x)
    df['overall_result'] = df['overall_result'].str.lower()

    results_2022_23 = pd.read_csv(path)
    results_2022_23 = results_2022_23[["prnnumber", "status", "admission_year"]].rename(columns={"prnnumber": "roll_num", "status": "overall_result", "admission_year": "year"})
    results_2022_23["exam_year"] = 1
    results_2022_23["exam_year"] = results_2022_23.apply(lambda x: 2 if x["year"] == 2022 else 1, axis=1)
    results_2022_23["roll_num"] = "00" + results_2022_23["roll_num"].astype(str).str[1:]

    df = pd.concat([df, results_2022_23], ignore_index=True)
    return df

def clean_admitted_trainee_sheet(df: pd.DataFrame) -> pd.DataFrame:
    df = df[["year", "Roll_Num", "ITI_Code", "Trade_Name", "Gender"]].rename(columns={"Roll_Num": "roll_num", "ITI_Code": "iti_code", "Trade_Name": "trade", "Gender":"gender"})
    df['roll_num'] = df['roll_num'].astype(str).str.strip("'")
    df.loc[df["year"] == 2018, "trade"] = df.loc[df["year"] == 2018, "trade"].astype(str) + " (NSQF)"
    return df


    


# %%
admitted_trainees_cleaned = clean_admitted_trainee_sheet(admitted_trainees)
exam_result_sheet_cleaned = clean_exam_result_sheet(exam_result_sheet)

# %%


# %%
admitted_trainees_cleaned[admitted_trainees_cleaned["year"] == 2022]["roll_num"].nunique()



# %%


# %%
def combine_admitted_and_exam_results(admitted_df: pd.DataFrame, exam_df: pd.DataFrame) -> pd.DataFrame:
    combined_df = pd.merge(admitted_df, exam_df, on=["roll_num", "year"], how="left")
    combined_df["matched"] = combined_df["overall_result"].notnull()
    # pivoted = combined_df.pivot_table(
    #     index=["year", "roll_num", "iti_code", "trade", "gender"],
    #     columns="exam_year",
    #     values="overall_result",
    #     aggfunc="first"
    # ).reset_index()

    # pivoted = pivoted.rename(
    #     columns={1.0: "overall_result_y1", 2.0: "overall_result_y2"}
    # )

    # return combined_df


    pivoted = combined_df.pivot_table(
        index=["year", "roll_num", "iti_code", "trade", "gender", "matched"],
        columns="exam_year",
        values="overall_result",
        aggfunc="first"
    ).reset_index()

    pivoted = pivoted.rename(
        columns={1.0: "overall_result_y1", 2.0: "overall_result_y2"}
    )

    # Merge back to admitted_df to retain all roll_num
    result = pd.merge(
        admitted_df,
        pivoted[["year", "roll_num", "overall_result_y1", "overall_result_y2"]],
        on=["year", "roll_num"],
        how="left"
    )

    return result

sctevt_df = combine_admitted_and_exam_results(admitted_trainees_cleaned, exam_result_sheet_cleaned)

    

# %%
sctevt_df[sctevt_df["year"] == 2022]["overall_result_y2"].count()

# %%
# Get institute level information and trade information from SAMS
sams_iti_enrollments = load_data(datasets["iti_enrollments"])
trades = sams_iti_enrollments[["reported_branch_or_trade", "course_period"]].rename(columns={"reported_branch_or_trade": "trade"}).drop_duplicates()
conn = sqlite.connect(SAMS_DB)
query = "SELECT * FROM institutes WHERE module = 'ITI'"
institutes = pd.read_sql_query(query, conn)[["ncvtmis_code", "type_of_institute"]].rename(columns={"ncvtmis_code":"iti_code"}).drop_duplicates()

# %%
# Combine to get institute and trade information
sctevt_df = pd.merge(sctevt_df, institutes, on="iti_code", how="left")
sctevt_df = pd.merge(sctevt_df, trades, on="trade", how="left")
sctevt_df

# %%
# Dropouts
sctevt_df["dropout"] = False
sctevt_df["dropout"] = sctevt_df.apply(lambda x: True if (pd.isna(x["overall_result_y1"]) and x["course_period"] == "1 Year") or (pd.isna(x["overall_result_y2"]) and x["course_period"] == "2 Years") else x["dropout"], axis=1) 

# %% [markdown]
# # Deck exhibits

# %%
def pretty_pivot(pivoted_df: pd.DataFrame) -> pd.DataFrame:
    pivoted_df = pivoted_df.copy()
    pivoted_df["Total"] = pivoted_df.sum(axis=1)
    cols = ["Total"] + [col for col in pivoted_df.columns if col != "Total"]
    pivoted_df = pivoted_df[cols]
    pivoted_df = pivoted_df.astype(int)
    return pivoted_df

# %%
# SAMS Students admitted ITI
students_admitted_over_time = pd.pivot_table(
    sams_iti_enrollments,
    index="year",
    columns="type_of_institute",
    values="aadhar_no",
    aggfunc="nunique"
)
students_admitted_over_time = students_admitted_over_time[students_admitted_over_time.index > 2017]
sams_students_admitted_over_time = pretty_pivot(students_admitted_over_time)
sams_students_admitted_over_time



# %%
# SAMS Students admitted female
students_admitted_over_time = pd.pivot_table(
    sams_iti_enrollments[sams_iti_enrollments["gender"] == "Female"],
    index="year",
    columns="type_of_institute",
    values="aadhar_no",
    aggfunc="nunique"
)
students_admitted_over_time = students_admitted_over_time[students_admitted_over_time.index > 2017]
sams_students_admitted_over_time_female = pretty_pivot(students_admitted_over_time)

# %%
# SCTEVT dropout all 
sctevt_dropout_over_time = pd.pivot_table(
    sctevt_df,
    index="year",
    columns="type_of_institute",
    values="dropout",
    aggfunc="sum"
)
sctevt_dropout_over_time = pretty_pivot(sctevt_dropout_over_time)
sctevt_dropout_over_time

# %%
# SCTEVT enrollment
sctevt_admitted_over_time = pd.pivot_table(
    sctevt_df,
    index="year",
    columns="type_of_institute",
    values="roll_num",
    aggfunc="nunique"
)
sctevt_admitted_over_time = pretty_pivot(sctevt_admitted_over_time)
sctevt_admitted_over_time

# %%
# SCTEVT enrollment female
sctevt_admitted_female = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Female"],
    index="year",
    columns="type_of_institute",
    values="roll_num",
    aggfunc="nunique",
)
sctevt_admitted_female = pretty_pivot(sctevt_admitted_female)
sctevt_admitted_female

# %%
# SCTEVT pass  total / government / private
sctevt_pass = pd.pivot_table(
    sctevt_df[(sctevt_df["overall_result_y1"] == "pass") & (sctevt_df["course_period"] == "1 Year") | 
              (sctevt_df["overall_result_y2"] == "pass") & (sctevt_df["course_period"] == "2 Years")],
    index="year",
    columns="type_of_institute",
    values="roll_num",
    aggfunc="nunique",
)
sctevt_pass = pretty_pivot(sctevt_pass )
sctevt_pass

# %%
# SCTEVT pass female 
sctevt_pass_female = sctevt_df[sctevt_df["gender"] == "Female"]
sctevt_pass_female = pd.pivot_table(
    sctevt_pass_female[(sctevt_df["overall_result_y1"] == "pass") & (sctevt_df["course_period"] == "1 Year") | 
              (sctevt_df["overall_result_y2"] == "pass") & (sctevt_df["course_period"] == "2 Years")],
    index="year",
    columns="type_of_institute",
    values="roll_num",
    aggfunc="nunique",
)
sctevt_pass_female = pretty_pivot(sctevt_pass_female)
sctevt_pass_female


# %%
# SCTEVT enrollment male
sctevt_admitted_male = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Male"],
    index="year",
    columns="type_of_institute",
    values="roll_num",
    aggfunc="nunique",
)
sctevt_admitted_male= pretty_pivot(sctevt_admitted_male)
sctevt_admitted_male

# %%
# SCTEVT dropout female
sctevt_dropout_over_time_female = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Female"],
    index="year",
    columns="type_of_institute",
    values="dropout",
    aggfunc="sum"
)
sctevt_dropout_over_time_female = pretty_pivot(sctevt_dropout_over_time_female)
sctevt_dropout_over_time_female

# %%
sctevt_df["retained"] = sctevt_df["dropout"].apply(lambda x: not x)
# SCTEVT retained all
sctevt_retained_over_time = pd.pivot_table(
    sctevt_df,
    index="year",
    columns="type_of_institute",
    values="retained",
    aggfunc="sum"
)
sctevt_retained_over_time = pretty_pivot(sctevt_retained_over_time)
sctevt_retained_over_time

# %%
# SCTEVT retained female
sctevt_retained_over_time_female = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Female"],
    index="year",
    columns="type_of_institute",
    values="retained",
    aggfunc="sum"
)
sctevt_retained_over_time_female = pretty_pivot(sctevt_retained_over_time_female)
sctevt_retained_over_time_female

# %%
# SCTEVT dropout male
sctevt_dropout_over_time_male = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Male"],
    index="year",
    columns="type_of_institute",
    values="dropout",
    aggfunc="sum"
)
sctevt_dropout_over_time_male = pretty_pivot(sctevt_dropout_over_time_male)
sctevt_dropout_over_time_male

# %%
# SCTEVT retained male
sctevt_retained_over_time_male = pd.pivot_table(
    sctevt_df[sctevt_df["gender"] == "Male"],
    index="year",
    columns="type_of_institute",
    values="retained",
    aggfunc="sum"
)
sctevt_retained_over_time_male = pretty_pivot(sctevt_retained_over_time_male)
sctevt_retained_over_time_male

# %%
# Export to Excel
dfs = [
    sams_students_admitted_over_time,
    sams_students_admitted_over_time_female,
    sctevt_admitted_over_time,
    sctevt_admitted_female,
    sctevt_admitted_male,
    sctevt_pass,
    sctevt_pass_female,
    sctevt_dropout_over_time,
    sctevt_dropout_over_time_female,
    sctevt_retained_over_time,
    sctevt_retained_over_time_female,
    sctevt_dropout_over_time_male,
    sctevt_retained_over_time_male
]
sheet_names = [
    "SAMS Students Admitted",
    "SAMS Students Admitted (Female)",
    "SCTEVT Students Admitted",
    "SCTEVT Students Admitted (Female)",
    "SCTEVT Students Admitted (Male)",
    "SCTEVT Pass",
    "SCTEVT Pass (Female)",
    "SCTEVT Dropout",
    "SCTEVT Dropout (Female)",
    "SCTEVT Retained",
    "SCTEVT Retained (Female)",
    "SCTEVT Dropout (Male)",
    "SCTEVT Retained (Male)"
]
save_table_excel(
    dfs=dfs,
    sheet_names=sheet_names,
    outfile=TABLES_DIR / "sctevt_iti_admission_and_results.xlsx",
    index=[True]*len(dfs)
)



