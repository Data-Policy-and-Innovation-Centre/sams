import pandas as pd
from hamilton.function_modifiers import parameterize, value, source, datasaver
from sams.config import datasets, exhibits
from sams.utils import load_data
from sams.analysis.utils import (
    pivot_table,
    save_table_excel
)
from loguru import logger

# ========== Datasets ============
@parameterize(
    iti_students_enrollments=dict(module=value("ITI")),
    diploma_students_enrollments=dict(module=value("Diploma")),
)
def students_enrollments(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_enrollments"])

@parameterize(
    iti_students_marks=dict(module=value("ITI")),
    diploma_students_marks=dict(module=value("Diploma")),
)
def students_marks(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_marks"])

@parameterize(
    iti_institutes_cutoffs=dict(module=value("ITI")),   
    diploma_institutes_cutoffs=dict(module=value("Diploma")),
)
def institutes_cutoffs(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_institutes_cutoffs"])

@parameterize(
    iti_institutes_strength=dict(module=value("ITI")),   
    diploma_institutes_strength=dict(module=value("Diploma")),
)
def institutes_strength(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_institutes_strength"])

@parameterize(
    iti_institutes_enrollments=dict(module=value("ITI")),   
    diploma_institutes_enrollments=dict(module=value("Diploma")),
)
def institutes_enrollments(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_institutes_enrollments"])

@parameterize(
    iti_marks_and_cutoffs=dict(module=value("ITI")),   
    diploma_marks_and_cutoffs=dict(module=value("Diploma")),
)
def marks_and_cutoffs(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_marks_and_cutoffs"])

@parameterize(
    iti_vacancies=dict(module=value("ITI")),   
    diploma_vacancies=dict(module=value("Diploma")),
)
def vacancies(module: str) -> pd.DataFrame:
    return load_data(datasets[f"{module.lower()}_vacancies"])

@parameterize(
    iti_students_enrollments_2023=dict(student_enrollments=source("iti_students_enrollments")),
    diploma_students_enrollments_2023=dict(student_enrollments=source("diploma_students_enrollments")),
)
def student_enrollments_2023(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    return student_enrollments[student_enrollments["year"] == 2023]

@parameterize(
    iti_students_marks_2023=dict(student_marks=source("iti_students_marks")),
    diploma_students_marks_2023=dict(student_marks=source("diploma_students_marks")),
)
def student_marks_2023(student_marks: pd.DataFrame) -> pd.DataFrame:
    return student_marks[student_marks["year"] == 2023]

@parameterize(
    iti_institutes_cutoffs_2023=dict(institutes_cutoffs=source("iti_institutes_cutoffs")),  
    diploma_institutes_cutoffs_2023=dict(institutes_cutoffs=source("diploma_institutes_cutoffs")),
)
def institutes_cutoffs_2023(institutes_cutoffs: pd.DataFrame) -> pd.DataFrame:
    return institutes_cutoffs[institutes_cutoffs["year"] == 2023]

# ========== Exhibits ============
@parameterize(
    iti_enrollments_over_time=dict(student_enrollments=source("iti_students_enrollments")),
    diploma_enrollments_over_time=dict(student_enrollments=source("diploma_students_enrollments")),
)
def enrollments_over_time(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    enrollments_over_time = pivot_table(
        student_enrollments,
        index="academic_year",
        values="aadhar_no",
        aggfunc="nunique",
        index_label="Year",
        value_label="Num. students",
        round=0

    )
    return enrollments_over_time

def combined_enrollments_over_time(iti_enrollments_over_time: pd.DataFrame, diploma_enrollments_over_time: pd.DataFrame) -> pd.DataFrame:
    iti_enrollments_over_time.rename(columns={"Num. students": "ITI"}, inplace=True)
    diploma_enrollments_over_time.rename(columns={"Num. students": "Diploma"}, inplace=True)
    combined_enrollments_over_time = pd.merge(iti_enrollments_over_time, diploma_enrollments_over_time, how="outer", on="Year")
    combined_enrollments_over_time = combined_enrollments_over_time[combined_enrollments_over_time["Year"] > 2017]
    combined_enrollments_over_time = combined_enrollments_over_time.astype("int")
    return combined_enrollments_over_time

def _get_pct(df: pd.DataFrame, vars: list[str], total_label: str, var_labels: list[str], round: list[int], drop: bool = True) -> pd.DataFrame:
    if len(vars) != len(var_labels):
        raise ValueError("The number of variables must be equal to the number of variable labels")

    df[total_label] = df[vars].sum(axis=1)
    for var, var_label, round_val in zip(vars, var_labels, round):
        df[var_label] = df[var] / df[total_label] * 100
        df[var_label] = df[var_label].round(round_val)

    if drop:
        df.drop(vars, axis=1, inplace=True)
    df = df[[var for var in df.columns if var != total_label] + [total_label]]
    return df

@parameterize(
    iti_enrollments_over_time_by_type=dict(student_enrollments=source("iti_students_enrollments")),
    diploma_enrollments_over_time_by_type=dict(student_enrollments=source("diploma_students_enrollments")),
)
def enrollments_over_time_by_type(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    enrollments_over_time_by_type = student_enrollments.groupby(["academic_year", "type_of_institute"]).agg({"aadhar_no": "nunique"}).reset_index()
    enrollments_over_time_by_type =  enrollments_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="aadhar_no")
    enrollments_over_time_by_type = enrollments_over_time_by_type[enrollments_over_time_by_type.index> 2017]
    enrollments_over_time_by_type = enrollments_over_time_by_type.astype("int")
    enrollments_over_time_by_type = _get_pct(enrollments_over_time_by_type, 
                                             ["Pvt.", "Govt."], "Num. students", ["Pvt (%)", "Govt (%)"], [1, 1],
                                             drop=True)
    enrollments_over_time_by_type.index.name = "Year"
    return enrollments_over_time_by_type



@parameterize(
    iti_institutes_over_time=dict(institutes_strength=source("iti_institutes_strength")),
    diploma_institutes_over_time=dict(institutes_strength=source("diploma_institutes_strength")),
)
def institutes_over_time(institutes_strength: pd.DataFrame) -> pd.DataFrame:
    institutes_over_time = pivot_table(
        institutes_strength,
        index="academic_year",
        values="sams_code",
        aggfunc="nunique",
        index_label="Year",
        value_label="Num. institutes",
        round=0
    )
    return institutes_over_time

def combined_institutes_over_time(iti_institutes_over_time: pd.DataFrame, diploma_institutes_over_time: pd.DataFrame) -> pd.DataFrame:
    itis = iti_institutes_over_time.rename(columns={"Num. institutes": "ITI"})
    diplomas = diploma_institutes_over_time.rename(columns={"Num. institutes": "Diploma"})
    combined_institutes_over_time = pd.merge(itis, diplomas, how="outer", on="Year")
    combined_institutes_over_time = combined_institutes_over_time[combined_institutes_over_time["Year"] > 2017]
    # combined_institutes_over_time = combined_institutes_over_time.astype("int")
    return combined_institutes_over_time

@parameterize(
    iti_institutes_over_time_by_type=dict(institutes_strength=source("iti_institutes_strength"), student_enrollments=source("iti_students_enrollments")),
    diploma_institutes_over_time_by_type=dict(institutes_strength=source("diploma_institutes_strength"), student_enrollments=source("diploma_students_enrollments")),
)
def institutes_over_time_by_type(institutes_strength: pd.DataFrame, student_enrollments: pd.DataFrame) -> pd.DataFrame:
    student_enrollments = student_enrollments[["sams_code", "type_of_institute"]].drop_duplicates()
    institutes_over_time_by_type = pd.merge(institutes_strength, student_enrollments, how="left", on="sams_code")
    institutes_over_time_by_type = institutes_over_time_by_type.groupby(["academic_year", "type_of_institute"]).agg({"sams_code": "nunique"}).reset_index()
    institutes_over_time_by_type = institutes_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="sams_code")
    # institutes_over_time_by_type = institutes_over_time_by_type.astype("int")
    institutes_over_time_by_type = _get_pct(institutes_over_time_by_type, 
                                             ["Pvt.", "Govt."], "Num. institutes", ["Pvt (%)", "Govt (%)"], [1, 1],
                                             drop=True)
    institutes_over_time_by_type.index.name = "Year"
    return institutes_over_time_by_type


@parameterize(
    top_10_iti_institutes_by_enrollment_2023=dict(students_enrollments_2023=source("iti_students_enrollments_2023")),
    top_10_diploma_institutes_by_enrollment_2023=dict(students_enrollments_2023=source("diploma_students_enrollments_2023")),
)
def top_10_institutes_by_enrollment_2023(students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    top_10_institutes_by_enrollment_2023 = students_enrollments_2023.groupby(["reported_institute", "type_of_institute"]).agg({"aadhar_no": "nunique"}).reset_index()
    top_10_institutes_by_enrollment_2023 = top_10_institutes_by_enrollment_2023.sort_values("aadhar_no", ascending=False).head(10)
    top_10_institutes_by_enrollment_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "aadhar_no": "Num. students"}, inplace=True)
    return top_10_institutes_by_enrollment_2023

def trades_over_time(iti_institutes_strength: pd.DataFrame) -> pd.DataFrame:
    trades_over_time = iti_institutes_strength.groupby(["academic_year"]).agg({"trade": "nunique"}).reset_index()
    trades_over_time.rename(columns={"trade": "Num. trades", "academic_year": "Year"}, inplace=True)
    return trades_over_time

def branches_over_time(diploma_institutes_strength: pd.DataFrame) -> pd.DataFrame:
    branches_over_time = diploma_institutes_strength.groupby(["academic_year"]).agg({"branch": "nunique"}).reset_index()
    branches_over_time.rename(columns={"branch": "Num. branches", "academic_year": "Year"}, inplace=True)
    return branches_over_time

@parameterize(
    top_10_trades_by_enrollment_2023=dict(students_enrollments_2023=source("iti_students_enrollments_2023")),
    top_10_branches_by_enrollment_2023=dict(students_enrollments_2023=source("diploma_students_enrollments_2023")),
)
def top_10_by_enrollment_2023(students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    top_10_by_enrollment_2023 = students_enrollments_2023.groupby(["reported_branch_or_trade"]).agg({"aadhar_no": "nunique"}).reset_index()
    top_10_by_enrollment_2023 = top_10_by_enrollment_2023.sort_values("aadhar_no", ascending=False).head(10)
    if students_enrollments_2023["module"].iloc[0] == "ITI":
        top_10_by_enrollment_2023.rename(columns={"reported_branch_or_trade": "Trade", "aadhar_no": "Num. students"}, inplace=True)
    else:
        top_10_by_enrollment_2023.rename(columns={"reported_branch_or_trade": "Branch", "aadhar_no": "Num. students"}, inplace=True)
    return top_10_by_enrollment_2023

def top_10_itis_by_num_trades_2023(iti_institutes_strength: pd.DataFrame, iti_students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    iti_institutes_strength_2023 = iti_institutes_strength[iti_institutes_strength["academic_year"] == 2023]
    iti_names = iti_students_enrollments_2023[["sams_code","reported_institute","type_of_institute"]].drop_duplicates()
    iti_institutes_strength_2023 = iti_institutes_strength_2023.merge(iti_names, how="left", on="sams_code")
    top_10_itis_by_num_trades_2023 = iti_institutes_strength_2023.groupby(["sams_code","reported_institute", "type_of_institute"]).agg({"trade": "nunique"}).reset_index()
    top_10_itis_by_num_trades_2023 = top_10_itis_by_num_trades_2023.sort_values("trade", ascending=False).head(10)
    top_10_itis_by_num_trades_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "trade": "Num. trades"}, inplace=True)
    top_10_itis_by_num_trades_2023.drop("sams_code", axis=1, inplace=True)
    return top_10_itis_by_num_trades_2023

def top_10_diplomas_by_num_branches_2023(diploma_institutes_strength: pd.DataFrame, diploma_students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    diploma_institutes_strength_2023 = diploma_institutes_strength[diploma_institutes_strength["academic_year"] == 2023]
    diploma_names = diploma_students_enrollments_2023[["sams_code","reported_institute","type_of_institute"]].drop_duplicates()
    diploma_institutes_strength_2023 = diploma_institutes_strength_2023.merge(diploma_names, how="left", on="sams_code")
    top_10_diplomas_by_num_branches_2023 = diploma_institutes_strength_2023.groupby(["sams_code","reported_institute", "type_of_institute"]).agg({"branch": "nunique"}).reset_index()
    top_10_diplomas_by_num_branches_2023 = top_10_diplomas_by_num_branches_2023.sort_values("branch", ascending=False).head(10)
    top_10_diplomas_by_num_branches_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "branch": "Num. branches"}, inplace=True)
    top_10_diplomas_by_num_branches_2023.drop("sams_code", axis=1, inplace=True)
    return top_10_diplomas_by_num_branches_2023

# ========== Save exhibits ============
@datasaver()
def students_enrollments_basics(combined_enrollments_over_time: pd.DataFrame, 
                         iti_enrollments_over_time_by_type: pd.DataFrame, 
                         diploma_enrollments_over_time_by_type: pd.DataFrame) -> dict:
    
    tables = [combined_enrollments_over_time, iti_enrollments_over_time_by_type, diploma_enrollments_over_time_by_type]
    sheet_names = ["Enrollments over time", "ITI Enrollments over time by type (%)", "Diploma Enrollments over time by type (%)"]
    file_path = exhibits["students_enrollment_basics"]["path"]
    metadata = {"path": file_path, "type": "excel"}
    save_table_excel(tables, sheet_names, index=[False, True, True], outfile=file_path)
    logger.info(f"Student enrollment summary exhibits saved at: {file_path}")
    return metadata

@datasaver()
def institutes_basics(iti_institutes_over_time: pd.DataFrame, diploma_institutes_over_time: pd.DataFrame, combined_institutes_over_time: pd.DataFrame,
                        iti_institutes_over_time_by_type: pd.DataFrame, diploma_institutes_over_time_by_type: pd.DataFrame,
                        top_10_iti_institutes_by_enrollment_2023: pd.DataFrame, top_10_diploma_institutes_by_enrollment_2023: pd.DataFrame) -> dict:
    
    tables = [iti_institutes_over_time, 
              diploma_institutes_over_time, 
              combined_institutes_over_time, 
              iti_institutes_over_time_by_type, 
              diploma_institutes_over_time_by_type,
              top_10_iti_institutes_by_enrollment_2023,
              top_10_diploma_institutes_by_enrollment_2023]
    sheet_names = ["ITI institutes over time", 
                   "Diploma institutes over time", 
                   "Institutes over time", 
                   "ITI Institutes over time by type (%)", 
                   "Diploma Institutes over time by type (%)",
                   "Top 10 ITI institutes by enrollment in 2023",
                   "Top 10 Diploma institutes by enrollment in 2023"]
    file_path = exhibits["institutes_basics"]["path"]
    metadata = {"path": file_path, "type": "excel"}
    save_table_excel(tables, sheet_names, index=[False, False, False, True, True, False, False], outfile=file_path)
    logger.info(f"Institutes summary exhibits saved at: {file_path}")
    return metadata

@datasaver()
def trades_and_branches_basics(trades_over_time: pd.DataFrame, branches_over_time: pd.DataFrame,
                                top_10_itis_by_num_trades_2023: pd.DataFrame, top_10_diplomas_by_num_branches_2023: pd.DataFrame,
                                top_10_trades_by_enrollment_2023: pd.DataFrame, top_10_branches_by_enrollment_2023: pd.DataFrame) -> dict:
    
    tables = [trades_over_time,
               branches_over_time, 
               top_10_itis_by_num_trades_2023, 
               top_10_diplomas_by_num_branches_2023, 
               top_10_trades_by_enrollment_2023,
               top_10_branches_by_enrollment_2023]
    sheet_names = ["Trades over time", 
                   "Branches over time", 
                   "Top 10 ITI institutes by number of trades in 2023",
                   "Top 10 Diploma institutes by number of branches in 2023",
                   "Top 10 trades by enrollment in 2023",
                   "Top 10 branches by enrollment in 2023"]
    file_path = exhibits["trades_and_branches_basics"]["path"]
    metadata = {"path": file_path, "type": "excel"}
    save_table_excel(tables, sheet_names, index=[False, False, False, False, False, False], outfile=file_path)
    logger.info(f"Trades and branches summary exhibits saved at: {file_path}")
    return metadata
    
    













