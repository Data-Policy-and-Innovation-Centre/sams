import pandas as pd
from hamilton.function_modifiers import parameterize, value, source, datasaver
from sams.config import datasets, exhibits, FIGURES_DIR, TABLES_DIR
from sams.utils import load_data
from sams.analysis.utils import (
    pivot_table,
    save_table_excel
)
from loguru import logger
from shapely.geometry import Point
import geopandas as gpd
import matplotlib.pyplot as plt
from plotnine import ggplot, aes, geom_histogram, labs, theme, theme_classic, ggsave


# ========== Datasets ============

def pipeline_raw() -> pd.DataFrame:
    return pd.read_excel(exhibits["pipeline"]["input_path"], sheet_name="pipeline")

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

def geocodes() -> pd.DataFrame:
    return load_data(datasets["geocodes"])

def block_shapefiles() -> gpd.GeoDataFrame:
    return load_data(datasets["block_shapefiles"])

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
    return student_enrollments[student_enrollments["academic_year"] == 2023]

@parameterize(
    iti_students_marks_2023=dict(student_marks=source("iti_students_marks")),
    diploma_students_marks_2023=dict(student_marks=source("diploma_students_marks")),
)
def student_marks_2023(student_marks: pd.DataFrame) -> pd.DataFrame:
    return student_marks[student_marks["academic_year"] == 2023]

@parameterize(
    iti_institutes_cutoffs_2023=dict(institutes_cutoffs=source("iti_institutes_cutoffs")),  
    diploma_institutes_cutoffs_2023=dict(institutes_cutoffs=source("diploma_institutes_cutoffs")),
)
def institutes_cutoffs_2023(institutes_cutoffs: pd.DataFrame) -> pd.DataFrame:
    return institutes_cutoffs[institutes_cutoffs["academic_year"] == 2023]

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
    iti_enrollments_over_time = iti_enrollments_over_time.rename(columns={"Num. students": "ITI"})
    iti_enrollments_over_time = diploma_enrollments_over_time.rename(columns={"Num. students": "Diploma"})
    combined_enrollments_over_time = pd.merge(iti_enrollments_over_time, diploma_enrollments_over_time, how="outer", on="Year")
    combined_enrollments_over_time = combined_enrollments_over_time[combined_enrollments_over_time["Year"] > 2017]
    combined_enrollments_over_time = combined_enrollments_over_time.astype("int")
    return combined_enrollments_over_time

def _get_pct(df: pd.DataFrame, vars: list[str], total_label: str, var_labels: list[str], round: list[int], drop: bool = True) -> pd.DataFrame:
    df = df.copy()

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
    logger.info(f"TABLE: {student_enrollments.module[0]} enrollments over time by type of institute")
    enrollments_over_time_by_type = student_enrollments.groupby(["academic_year", "type_of_institute"]).agg({"aadhar_no": "nunique"}).reset_index()
    enrollments_over_time_by_type =  enrollments_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="aadhar_no")
    enrollments_over_time_by_type = enrollments_over_time_by_type[enrollments_over_time_by_type.index> 2017]
    enrollments_over_time_by_type = enrollments_over_time_by_type.astype("int")
    enrollments_over_time_by_type = _get_pct(enrollments_over_time_by_type, 
                                             ["Pvt.", "Govt."], "Num. students", ["Pvt (%)", "Govt (%)"], [1, 1],
                                             drop=True)
    enrollments_over_time_by_type.index.name = "Year"
    return enrollments_over_time_by_type


def pipeline_pct(pipeline_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("TABLE: Pipeline of 10th std. students")
    pipeline = _get_pct(pipeline_raw,pipeline_raw.columns[-4:], "Total 10th std. students",
 ["11th std. admitted (%)", "ITI admitted (%)", "Diploma admitted (%)", "Assumed Dropouts (%)"], [1, 1, 1, 1])
    pipeline.drop("Total 10th std. students", axis=1, inplace=True)
    return pipeline


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


@parameterize(
        iti_enrollment_institutes_over_time=dict(students_enrollment=source("iti_students_enrollments"), institutes_strength=source("iti_institutes_strength")),
        diploma_enrollment_institutes_over_time=dict(students_enrollment=source("diploma_students_enrollments"), institutes_strength=source("diploma_institutes_strength")),
)
def enrollment_institutes_over_time(students_enrollment: pd.DataFrame, institutes_strength: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: {students_enrollment.module[0]} enrollments and institutes over time")
    # Enrollments
    enrollments_over_time_by_type = students_enrollment.groupby(["academic_year", "type_of_institute"]).agg({"aadhar_no": "nunique"}).reset_index()
    enrollments_over_time_by_type = enrollments_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="aadhar_no")
    enrollments_over_time_by_type["Total"] = enrollments_over_time_by_type.sum(axis=1)
    enrollments_over_time_by_type = enrollments_over_time_by_type.reset_index().melt(id_vars=["academic_year"], var_name="type_of_institute", value_name="Num. students")

    # Institutes
    institutes_over_time_by_type = institutes_strength.merge(students_enrollment[["sams_code", "type_of_institute"]], how="left", on="sams_code").groupby(["academic_year", "type_of_institute"]).agg({"sams_code": "nunique"}).reset_index()
    institutes_over_time_by_type = institutes_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="sams_code")
    institutes_over_time_by_type["Total"] = institutes_over_time_by_type.sum(axis=1)
    institutes_over_time_by_type = institutes_over_time_by_type.reset_index().melt(id_vars=["academic_year"], var_name="type_of_institute", value_name="Num. institutes")

    # Merge
    enrollments_institutes_over_time = pd.merge(enrollments_over_time_by_type, institutes_over_time_by_type, how="outer", on=["academic_year", "type_of_institute"])
    enrollments_institutes_over_time[["Num. students", "Num. institutes"]] = enrollments_institutes_over_time[["Num. students", "Num. institutes"]].fillna(0).astype(int)

    # Pivot
    enrollments_institutes_over_time = enrollments_institutes_over_time.pivot_table(
        index="academic_year",
        columns="type_of_institute",
        values=["Num. students", "Num. institutes"],
        aggfunc="sum",
        fill_value=0
    )

    # Relabel multi-indices
    enrollments_institutes_over_time = enrollments_institutes_over_time.swaplevel(axis=1).sort_index(axis=1)
    enrollments_institutes_over_time = enrollments_institutes_over_time.astype(str)
    enrollments_institutes_over_time.replace({"0":"-"}, inplace=True)
    enrollments_institutes_over_time.index.name = "Year"
    enrollments_institutes_over_time.columns.names = ["Type", ""]

    return enrollments_institutes_over_time

def gap_between_10th_graduation_and_enrollment_iti(iti_students_enrollments: pd.DataFrame, iti_students_marks: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: Gap between 10th grad and enrollment for ITI students")
    iti_marks_enrollments = iti_students_enrollments.merge(iti_students_marks, on=['aadhar_no', 'academic_year'])
    iti_marks_enrollments['gap_years'] =  iti_marks_enrollments['date_of_application'].dt.year - iti_marks_enrollments['year_of_passing'].apply(lambda x: int(x)) 
    iti_marks_enrollments['gap_category'] = iti_marks_enrollments['gap_years'].apply(lambda x: 'Fresh graduate' if x == 0 else '1-3 years' if x <= 3 else '> 3 years')
    gaps_binned = iti_marks_enrollments['gap_category'].value_counts().sort_index()
    gaps_binned.index.name = "Years since graduation"
    gaps_binned = gaps_binned.rename("Num. students")
    return gaps_binned

def _top_5_trades_gender_over_time(df: pd.DataFrame) -> pd.DataFrame:
    df.drop("gender", axis=1, inplace=True)
    df = df.sort_values(["academic_year", "aadhar_no"], ascending=[True, False])
    df = df.groupby(["academic_year"]).head(5).reset_index(drop=True)
    df.rename(columns={"reported_branch_or_trade": "Trade", "academic_year": "Year", "aadhar_no": "Num. students", "share": "Share"}, inplace=True)
    df = df.pivot_table(index="Year", columns="Trade", values=["Num. students", "Share"]).swaplevel(axis=1).sort_index(axis=1)
    return df

@parameterize(
        top_5_trades_male_over_time=dict(iti_students_enrollments=source("iti_students_enrollments"), gender=value("Male")),
        top_5_trades_female_over_time=dict(iti_students_enrollments=source("iti_students_enrollments"), gender=value("Female"))
)
def top_5_trades_by_gender_over_time(iti_students_enrollments: pd.DataFrame, gender: str) -> pd.DataFrame:
    logger.info(f"TABLE: Top 5 trades for {gender} students over time")
    top_5_trades_by_gender_over_time = iti_students_enrollments.groupby(["academic_year", "gender", "reported_branch_or_trade"]).agg({"aadhar_no": "nunique"}).reset_index()
    top_5_trades_by_gender_over_time['share'] = top_5_trades_by_gender_over_time.groupby(["academic_year", "gender"])['aadhar_no'].transform(lambda x: x/x.sum())
    top_5_trades_by_gender_over_time = _top_5_trades_gender_over_time(top_5_trades_by_gender_over_time[top_5_trades_by_gender_over_time["gender"] == gender])
    return top_5_trades_by_gender_over_time

@parameterize(
        top_5_trades_male_2023=dict(iti_students_enrollments_2023=source("iti_students_enrollments_2023"),gender=value("Male")),
        top_5_trades_female_2023=dict(iti_students_enrollments_2023=source("iti_students_enrollments_2023"),gender=value("Female"))
)
def top_5_trades_by_gender_2023(iti_students_enrollments_2023: pd.DataFrame, gender: str) -> pd.DataFrame:
    logger.info(f"TABLE: Top 5 Trades for {gender} students (2023)")
    top_5_trades_by_gender_2023 = iti_students_enrollments_2023[iti_students_enrollments_2023["gender"] == gender].groupby(["reported_branch_or_trade"]).agg({"aadhar_no":"nunique"}).reset_index()
    top_5_trades_by_gender_2023['share'] = top_5_trades_by_gender_2023["aadhar_no"].transform(lambda x: x/x.sum()).round(2)
    top_5_trades_by_gender_2023 = top_5_trades_by_gender_2023.sort_values("share",ascending=False).head(5).reset_index(drop=True)
    top_5_trades_by_gender_2023.rename(columns={"reported_branch_or_trade": "Trade", "aadhar_no": "Num. students", "share": "Share"}, inplace=True)
    return top_5_trades_by_gender_2023
   
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
    logger.info(f"TABLE: {institutes_strength.module[0]} Institutes Over Time By Type (Pvt / Govt)")
    student_enrollments = student_enrollments[["sams_code", "type_of_institute"]].drop_duplicates()
    institutes_over_time_by_type = pd.merge(institutes_strength, student_enrollments, how="left", on="sams_code")
    institutes_over_time_by_type = institutes_over_time_by_type.groupby(["academic_year", "type_of_institute"]).agg({"sams_code": "nunique"}).reset_index()
    institutes_over_time_by_type = institutes_over_time_by_type.pivot(index="academic_year", columns="type_of_institute", values="sams_code")
    # institutes_over_time_by_type = institutes_over_time_by_type.astype("int")
    institutes_over_time_by_type = _get_pct(institutes_over_time_by_type, 
                                             ["Pvt.", "Govt."], "Num. institutes", ["Pvt (%)", "Govt (%)"], [1, 1],
                                             drop=False)
    institutes_over_time_by_type.index.name = "Year"
    return institutes_over_time_by_type

@parameterize(
    top_10_iti_institutes_by_enrollment_2023=dict(students_enrollments_2023=source("iti_students_enrollments_2023")),
    top_10_diploma_institutes_by_enrollment_2023=dict(students_enrollments_2023=source("diploma_students_enrollments_2023")),
)
def top_10_institutes_by_enrollment_2023(students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: Top 10 {students_enrollments_2023.reset_index().module[0]} institutes by enrollment in 2023")
    top_10_institutes_by_enrollment_2023 = students_enrollments_2023.groupby(["reported_institute", "type_of_institute"]).agg({"aadhar_no": "nunique"}).reset_index()
    top_10_institutes_by_enrollment_2023 = top_10_institutes_by_enrollment_2023.sort_values("aadhar_no", ascending=False).head(10)
    top_10_institutes_by_enrollment_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "aadhar_no": "Num. students"}, inplace=True)
    return top_10_institutes_by_enrollment_2023

def trades_over_time(iti_institutes_strength: pd.DataFrame) -> pd.DataFrame:
    logger.info("TABLE: Number of ITI Trades over time")
    trades_over_time = iti_institutes_strength.groupby(["academic_year"]).agg({"trade": "nunique"}).reset_index()
    trades_over_time.rename(columns={"trade": "Num. trades", "academic_year": "Year"}, inplace=True)
    return trades_over_time

def branches_over_time(diploma_institutes_strength: pd.DataFrame) -> pd.DataFrame:
    logger.info("TABLE: Number of Diploma Branches over time")
    branches_over_time = diploma_institutes_strength.groupby(["academic_year"]).agg({"branch": "nunique"}).reset_index()
    branches_over_time.rename(columns={"branch": "Num. branches", "academic_year": "Year"}, inplace=True)
    return branches_over_time

@parameterize(
    top_10_trades_by_enrollment_2023=dict(students_enrollments_2023=source("iti_students_enrollments_2023")),
    top_10_branches_by_enrollment_2023=dict(students_enrollments_2023=source("diploma_students_enrollments_2023")),
)
def top_10_by_enrollment_2023(students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: Top 10 {students_enrollments_2023.reset_index().module[0]} by enrollment in 2023")
    top_10_by_enrollment_2023 = students_enrollments_2023.groupby(["reported_branch_or_trade"]).agg({"aadhar_no": "nunique"}).reset_index()
    top_10_by_enrollment_2023["share"] = top_10_by_enrollment_2023["aadhar_no"].transform(lambda x: x/x.sum()).round(2)
    top_10_by_enrollment_2023 = top_10_by_enrollment_2023.sort_values("aadhar_no", ascending=False).head(10)
    if students_enrollments_2023["module"].iloc[0] == "ITI":
        top_10_by_enrollment_2023.rename(columns={"reported_branch_or_trade": "Trade", "aadhar_no": "Num. students", "share":"Share"}, inplace=True)
    else:
        top_10_by_enrollment_2023.rename(columns={"reported_branch_or_trade": "Branch", "aadhar_no": "Num. students", "share":"Share"}, inplace=True)
    return top_10_by_enrollment_2023
  
def top_10_itis_by_num_trades_2023(iti_institutes_strength: pd.DataFrame, iti_students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    logger.info("TABLE: Top 10 ITIs by number of trades (2023)")
    iti_institutes_strength_2023 = iti_institutes_strength[iti_institutes_strength["academic_year"] == 2023]
    iti_names = iti_students_enrollments_2023[["sams_code","reported_institute","type_of_institute"]].drop_duplicates()
    iti_institutes_strength_2023 = iti_institutes_strength_2023.merge(iti_names, how="left", on="sams_code")
    top_10_itis_by_num_trades_2023 = iti_institutes_strength_2023.groupby(["sams_code","reported_institute", "type_of_institute"]).agg({"trade": "nunique"}).reset_index()
    top_10_itis_by_num_trades_2023 = top_10_itis_by_num_trades_2023.sort_values("trade", ascending=False).head(10)
    top_10_itis_by_num_trades_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "trade": "Num. trades"}, inplace=True)
    top_10_itis_by_num_trades_2023.drop("sams_code", axis=1, inplace=True)
    return top_10_itis_by_num_trades_2023

def top_10_diplomas_by_num_branches_2023(diploma_institutes_strength: pd.DataFrame, diploma_students_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    logger.info("TABLE: Top 10 Diploma Institutes by Number of Branches (2023)")
    diploma_institutes_strength_2023 = diploma_institutes_strength[diploma_institutes_strength["academic_year"] == 2023]
    diploma_names = diploma_students_enrollments_2023[["sams_code","reported_institute","type_of_institute"]].drop_duplicates()
    diploma_institutes_strength_2023 = diploma_institutes_strength_2023.merge(diploma_names, how="left", on="sams_code")
    top_10_diplomas_by_num_branches_2023 = diploma_institutes_strength_2023.groupby(["sams_code","reported_institute", "type_of_institute"]).agg({"branch": "nunique"}).reset_index()
    top_10_diplomas_by_num_branches_2023 = top_10_diplomas_by_num_branches_2023.sort_values("branch", ascending=False).head(10)
    top_10_diplomas_by_num_branches_2023.rename(columns={"reported_institute": "Institute", "type_of_institute": "Type", "branch": "Num. branches"}, inplace=True)
    top_10_diplomas_by_num_branches_2023.drop("sams_code", axis=1, inplace=True)
    return top_10_diplomas_by_num_branches_2023

def _num_students_in_blocks_geom(student_enrollments_2023: pd.DataFrame, block_shapefiles: gpd.GeoDataFrame) -> pd.DataFrame:
    students_by_location = student_enrollments_2023.groupby(["student_long", "student_lat"]).agg({"aadhar_no": "nunique"}).reset_index()
    students_by_location = students_by_location.rename(columns={"aadhar_no": "Num. students"})
    geometry = [Point(xy) for xy in zip(students_by_location["student_long"], students_by_location["student_lat"])]
    students_by_location = gpd.GeoDataFrame(students_by_location, crs="EPSG:4326", geometry=geometry)
    block_shapefiles = block_shapefiles.to_crs("EPSG:4326")
    return students_by_location, block_shapefiles

@parameterize(
    map_iti_students_enrolled_2023=dict(student_enrollments_2023=source("iti_students_enrollments_2023"), block_shapefiles=source("block_shapefiles")),
    map_diploma_students_enrolled_2023=dict(student_enrollments_2023=source("diploma_students_enrollments_2023"), block_shapefiles=source("block_shapefiles")),
)
def map_students_enrolled_2023(student_enrollments_2023: pd.DataFrame, block_shapefiles: gpd.GeoDataFrame) -> plt.Figure:
    students_by_location, blocks = _num_students_in_blocks_geom(student_enrollments_2023, block_shapefiles)

    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot the blocks (base layer)
    blocks.plot(ax=ax, color="lightgrey", edgecolor="black")

    # Plot the student locations as dots
    # students_by_location.plot(ax=ax, color="red", markersize=students_by_location['Num. students'], label="Students")

    # Add a legend
    plt.legend()

    # Add a title and axis labels
    plt.title("Student Locations on Block Map")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    fig.savefig(FIGURES_DIR / "map_students_enrolled_2023.pdf")

    return fig

def map_itis_by_type_2023(itis_by_type_2023: pd.DataFrame) -> plt.Figure:
    pass
 

def fig_marks_2023(iti_students_marks_2023: pd.DataFrame, diploma_students_marks_2023: pd.DataFrame) -> tuple[ggplot,ggplot,ggplot]:
    
    # Prep data
    iti_students_marks_2023["module"] = "ITI"
    diploma_students_marks_2023["module"] = "Diploma"
    marks = pd.concat([iti_students_marks_2023, diploma_students_marks_2023])

    # Plot data
    logger.info("FIGURE: Distribution of 10th class marks for ITI (2023)")
    iti_plot = (ggplot(iti_students_marks_2023, aes(x='percentage'))
        + geom_histogram(binwidth=1, alpha=0.7, color="black", fill="lightblue")
        + labs(title='Distribution of 10th class marks for ITI (2023)', x='Marks (%)', y='Num. students')
        + theme(figure_size=(8, 6))
        + theme_classic()
       )
    
    logger.info("FIGURE: Distribution of 10th class marks for Polytechnic (2023)")
    diploma_plot = (ggplot(diploma_students_marks_2023, aes(x='percentage'))
        + geom_histogram(binwidth=1, alpha=0.7, color="black", fill="maroon")
        + labs(title='Distribution of 10th class marks for Polytechnic (2023)', x='Marks (%)', y='Num. students')
        + theme(figure_size=(8, 6)) 
        + theme_classic()
       )    

    logger.info("FIGURE: Histogram of Marks for All")
    all_plot = (ggplot(marks, aes(x='percentage', fill="module"))
        + geom_histogram(binwidth=1, alpha=0.7, color="black", position="dodge")
        + labs(title='Histogram of Marks for All', x='Marks (%)', y='Frequency', fill="Type")
        + theme(figure_size=(8, 6))
        + theme_classic()
       )
    return iti_plot, diploma_plot, all_plot

@parameterize(
        iti_marks_by_gender_2023=dict(student_marks_2023=source("iti_students_marks_2023"), student_enrollments_2023=source("iti_students_enrollments_2023")),
        diploma_marks_by_gender_2023=dict(student_marks_2023=source("diploma_students_marks_2023"), student_enrollments_2023=source("diploma_students_enrollments_2023")),
)
def marks_by_gender_2023(student_marks_2023: pd.DataFrame, student_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    student_marks_2023 = pd.concat([student_marks_2023, student_enrollments_2023[["gender"]]], axis=1)
    marks_by_gender_2023 = student_marks_2023.groupby(["gender"]).agg({"percentage":["mean","std"]}).reset_index()
    #marks_by_gender_2023.rename(columns={"mean": "Avg. marks",}, inplace=True)
    return marks_by_gender_2023
    

@parameterize(
        iti_locality_by_gender_2023 = dict(student_enrollments_2023=source("iti_students_enrollments_2023")),
        diploma_locality_by_gender_2023 = dict(student_enrollments_2023=source("diploma_students_enrollments_2023"))
)
def locality_by_gender_2023(student_enrollments_2023: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: {student_enrollments_2023.reset_index().module[0]} Locality by Gender (2023)")
    locality_by_gender_2023 = student_enrollments_2023.groupby(["gender", "local"]).agg({"aadhar_no":"nunique"}).reset_index()
    locality_by_gender_2023 = locality_by_gender_2023.pivot(index="gender", columns="local", values="aadhar_no").reset_index()
    return locality_by_gender_2023

@parameterize(
        iti_locality_by_gender_2018 = dict(student_enrollments=source("iti_students_enrollments")),
        diploma_locality_by_gender_2018 = dict(student_enrollments=source("diploma_students_enrollments"))
)       
def locality_by_gender_2018(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: {student_enrollments.module[0]} Locality by Gender (2018)")
    student_enrollments_2018 = student_enrollments[student_enrollments["academic_year"] == 2018]
    locality_by_gender_2018 = student_enrollments_2018.groupby(["gender", "local"]).agg({"aadhar_no":"nunique"}).reset_index()
    locality_by_gender_2018 = locality_by_gender_2018.pivot(index="gender", columns="local", values="aadhar_no").reset_index()
    return locality_by_gender_2018

@parameterize(
        iti_annual_income_over_time = dict(student_enrollments=source("iti_students_enrollments")),
        diploma_annual_income_over_time = dict(student_enrollments=source("diploma_students_enrollments")),
)
def annual_income_over_time(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: {student_enrollments.module[0]} Annual Income Over Time")
    income_over_time = student_enrollments.groupby(["academic_year","annual_income"]).agg({"aadhar_no":"nunique"}).reset_index()
    income_over_time = income_over_time.pivot_table(index="academic_year", columns="annual_income", values="aadhar_no").fillna(0).astype(int).reset_index()
    return income_over_time

@parameterize(
        iti_social_category_over_time = dict(student_enrollments=source("iti_students_enrollments")),
        diploma_social_category_over_time = dict(student_enrollments=source("diploma_students_enrollments")),
)
def social_category_over_time(student_enrollments: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"TABLE: {student_enrollments.module[0]} Social Category Over Time")
    social_category_over_time = student_enrollments.groupby(["academic_year","social_category"]).agg({"aadhar_no":"nunique"}).reset_index()
    social_category_over_time = social_category_over_time.pivot_table(index="academic_year", columns="social_category", values="aadhar_no").fillna(0).astype(int).reset_index()
    return social_category_over_time

@parameterize(
        iti_top_5_boards_2023 = dict(marks_2023=source("iti_students_marks_2023"), module=value("ITI")),
        diploma_top_5_boards_2023 = dict(marks_2023=source("diploma_students_marks_2023"), module=value("Diploma")),
)
def top_5_boards_2023(marks_2023: pd.DataFrame, module: str) -> pd.DataFrame:
    logger.info(f"TABLE: {module} Top Boards in 2023")
    top_boards_in_2023 = marks_2023.groupby(["highest_qualification_exam_board"]).agg({"aadhar_no":"nunique"}).reset_index()
    top_boards_in_2023 = top_boards_in_2023.pivot_table(index="highest_qualification_exam_board", values="aadhar_no").fillna(0).astype(int).reset_index()
    top_boards_in_2023 = top_boards_in_2023.sort_values(by="aadhar_no", ascending=False)
    top_boards_in_2023["percentage"] = top_boards_in_2023["aadhar_no"] / top_boards_in_2023["aadhar_no"].sum() * 100
    top_boards_in_2023["percentage"] = top_boards_in_2023["percentage"].round(1)
    top_boards_in_2023 = top_boards_in_2023.rename(columns={"highest_qualification_exam_board": "Board", 
                                                            "aadhar_no": "Num. students", "percentage": "Share (%)"})
    return top_boards_in_2023.head(5)

@parameterize(
    iti_highest_qualification_by_gender_2023 = dict(enrollments_2023 = source("iti_students_enrollments_2023")),
    diploma_highest_qualification_by_gender_2023 = dict(enrollments_2023 = source("diploma_students_enrollments_2023")),
)
def highest_qualification_by_gender_2023(enrollments_2023: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"TABLE: {enrollments_2023.reset_index().module[0]} Highest Qualification by Gender (2023)")
    enrollments_2023 = enrollments_2023.copy()
    enrollments_2023["highest_qualification"] = enrollments_2023["highest_qualification"].apply(lambda x: "Unknown" if pd.isna(x) else x)
    highest_qualification_by_gender_2023_levels = enrollments_2023.groupby(["gender", "highest_qualification"]).agg({"aadhar_no":"nunique"}).reset_index()
    highest_qualification_by_gender_2023_levels = highest_qualification_by_gender_2023_levels.pivot_table(index="gender", columns="highest_qualification", values="aadhar_no").fillna(0).astype(int)

    # Percentage
    highest_qualification_by_gender_2023_pct = _get_pct(
        highest_qualification_by_gender_2023_levels,
        vars=highest_qualification_by_gender_2023_levels.columns.to_list(),
        total_label="Num. students",
        var_labels=[f"{var} (%)" for var in highest_qualification_by_gender_2023_levels.columns.to_list()],
        round=[1]*len(highest_qualification_by_gender_2023_levels.columns.to_list()),
        drop=True
    )

    return highest_qualification_by_gender_2023_levels, highest_qualification_by_gender_2023_pct

@parameterize(
    iti_pass_by_gender_2023 = dict(enrollments_2023 = source("iti_students_enrollments_2023"), marks_2023 = source("iti_students_marks_2023"))
)
def pass_by_gender_2023(enrollments_2023: pd.DataFrame, marks_2023: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    module = enrollments_2023.reset_index().module[0]
    logger.info(f"TABLE: {module} Pass by Gender (2023)")
    pass_by_gender_2023 = pd.merge(
        enrollments_2023, marks_2023, on=["aadhar_no", "academic_year"], how="left"
    )

    pass_by_gender_2023 = pass_by_gender_2023.groupby(["gender", "exam_name"]).agg({"aadhar_no":"nunique"}).reset_index()
    pass_by_gender_2023 = pass_by_gender_2023.pivot_table(index="gender", columns="exam_name", values="aadhar_no").fillna(0).astype(int)

    # Percentage
    pass_by_gender_2023_pct = _get_pct(
        pass_by_gender_2023,
        vars=pass_by_gender_2023.columns.to_list(),
        total_label="Num. students",
        var_labels=[f"{var} (%)" for var in pass_by_gender_2023.columns.to_list()],
        round=[1]*len(pass_by_gender_2023.columns.to_list()),
        drop=True
    )

    return pass_by_gender_2023, pass_by_gender_2023_pct

@parameterize(
    iti_berhampur_cutoffs_2023 = dict(iti_institutes_cutoffs_2023 = source("iti_institutes_cutoffs_2023"), institute_name = value("ITI Berhampur")),
    iti_cuttack_cutoffs_2023 = dict(iti_institutes_cutoffs_2023 = source("iti_institutes_cutoffs_2023"), institute_name = value("ITI Cuttack"))
)
def iti_cutoffs_by_institute_2023(iti_institutes_cutoffs_2023: pd.DataFrame, institute_name: str) -> pd.DataFrame:
    logger.info(f"TABLE(s): {institute_name} Cutoffs (2023)")
    cutoffs = iti_institutes_cutoffs_2023[iti_institutes_cutoffs_2023["institute_name"].str.contains(institute_name)]
    cutoffs = cutoffs[cutoffs["qual"] == "10th Pass"]
    cutoffs = cutoffs[cutoffs["social_category"].isin(["UR", "SC", "ST"])]
    cutoffs = cutoffs[cutoffs["trade"].isin(["Electrician (NSQF)", "Fitter (NSQF)"])]
    cutoffs = cutoffs[~cutoffs["applicant_type"].str.contains("OMC")]
    cutoffs = cutoffs.groupby(["social_category", "gender", "trade"]).agg({"cutoff":"mean"}).reset_index()
    cutoffs = cutoffs.round(1)
    cutoffs = cutoffs.pivot_table(index=["trade","gender"], columns="social_category", values="cutoff")
    cutoffs.index.names = ["Trade", "Gender"]
    cutoffs.columns.name = "Social Category"
    return cutoffs




        

# ========== Save exhibits ============

@datasaver()
def uc_exhibits(pipeline_pct: pd.DataFrame, 
                iti_enrollment_institutes_over_time: pd.DataFrame,
                iti_enrollments_over_time_by_type: pd.DataFrame,
                top_10_itis_by_num_trades_2023: pd.DataFrame,
                top_10_iti_institutes_by_enrollment_2023: pd.DataFrame,
                top_10_trades_by_enrollment_2023: pd.DataFrame,
                iti_annual_income_over_time: pd.DataFrame,
                diploma_annual_income_over_time: pd.DataFrame,
                iti_social_category_over_time: pd.DataFrame,
                diploma_social_category_over_time: pd.DataFrame,
                fig_marks_2023: tuple[ggplot, ggplot, ggplot]) -> dict:
    
    # Prepare tables
    tables = [pipeline_pct, 
              iti_enrollment_institutes_over_time, 
              iti_enrollments_over_time_by_type, 
              top_10_itis_by_num_trades_2023, 
              top_10_iti_institutes_by_enrollment_2023, 
              top_10_trades_by_enrollment_2023,
              iti_annual_income_over_time,
              diploma_annual_income_over_time,
              iti_social_category_over_time,
              diploma_social_category_over_time]
    sheet_names = ["Pipeline (%)", 
                   "ITI institutes and enrollments over time (%)", 
                   "ITI enrollment shares of Govt. and Pvt. (%)",
                   "Top 10 ITI institutes by number of trades in 2023",
                   "Top 10 ITI institutes by enrollment in 2023",
                   "Top 10 trades by enrollment in 2023",
                   "ITI annual income over time",
                   "Diploma annual income over time",
                   "ITI social category over time",
                   "Diploma social category over time"]
    file_path = exhibits["uc_exhibits"]["path"]
    save_table_excel(tables, sheet_names, index=[False, True, True, False, False, False, False, False, False, False], outfile=file_path)
    logger.info(f"UC exhibits saved at: {file_path}")

    # Save figures
    ggsave(fig_marks_2023[0], FIGURES_DIR / "hist_marks_iti_2023.pdf")
    logger.info(f"Saved histogram of distribution of ITI marks to {FIGURES_DIR / 'hist_marks_iti_2023.pdf'}")

    ggsave(fig_marks_2023[1], FIGURES_DIR / "hist_marks_diploma_2023.pdf")
    logger.info(f"Saved histogram of distribution of Diploma marks to {FIGURES_DIR / 'hist_marks_diploma_2023.pdf'} ")

    metadata = {"tables":{"path": file_path, "type": "excel"},
                "iti_marks": {"path": FIGURES_DIR / "hist_marks_iti_2023.pdf", "type": "pdf"},
                "diploma_marks": {"path": FIGURES_DIR / "hist_marks_diploma_2023.pdf", "type": "pdf"}}
    return metadata

@datasaver()
def students_enrollments_basics(combined_enrollments_over_time: pd.DataFrame, 
                         iti_enrollments_over_time_by_type: pd.DataFrame, 
                         diploma_enrollments_over_time_by_type: pd.DataFrame) -> dict:
    
    tables = [combined_enrollments_over_time, 
            iti_enrollments_over_time_by_type,
            diploma_enrollments_over_time_by_type]
    sheet_names = ["Enrollments over time", 
                   "ITI Enrollments over time by type (%)", 
                   "Diploma Enrollments over time by type (%)"]
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


    
    













