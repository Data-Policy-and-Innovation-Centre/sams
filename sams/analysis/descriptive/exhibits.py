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
    diploma_students_enrollments_2023=dict(student_enrollments=source("diploma__students_enrollments")),
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
        index="year",
        values="aadhar_no",
        aggfunc="count",
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
    return enrollments_over_time_by_type



@parameterize(
    iti_institutes_over_time=dict(institutes_strength=source("iti_institutes_strength")),
    diploma_institutes_over_time=dict(institutes_strength=source("diploma_institutes_strength")),
)
def institutes_over_time(institutes_strength: pd.DataFrame) -> pd.DataFrame:
    pass


# ========== Save exhibits ============
@datasaver()
def enrollments_exhibits(combined_enrollments_over_time: pd.DataFrame, 
                         iti_enrollments_over_time_by_type: pd.DataFrame, 
                         diploma_enrollments_over_time_by_type: pd.DataFrame) -> dict:
    
    tables = [combined_enrollments_over_time, iti_enrollments_over_time_by_type, diploma_enrollments_over_time_by_type]
    sheet_names = ["Enrollments over time", "ITI Enrollments over time by type (%)", "Diploma Enrollments over time by type (%)"]
    file_path = exhibits["students_enrollments"]["path"]
    metadata = {"path": file_path, "type": "excel"}
    save_table_excel(tables, sheet_names, index=[True, True, True], outfile=file_path)
    logger.info(f"Student enrollment exhibits saved at: {file_path}")
    return metadata
    
    













