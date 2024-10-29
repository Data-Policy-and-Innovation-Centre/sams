from sams.preprocessing import nodes
import pandas as pd
import sqlite3
import sams.config as datasets
from loguru import logger

# Load raw data
logger.info("Loading raw data from SQLite database")
con = sqlite3.connect(datasets.get_path("sams"))
df_iti = pd.read_sql_query("SELECT * FROM students where module = 'ITI';", con)
df_diploma = pd.read_sql_query("SELECT * FROM students WHERE module = 'Diploma';", con)

# Preprocess geocodes
logger.info("Preprocessing geocodes using pin codes")
df_geocodes = nodes.preprocess_geocodes([df_iti, df_diploma])
df_geocodes.to_parquet(datasets.get_path("geocodes"), index=False)

# Preprocess ITI students enrollment
logger.info("Preprocessing ITI students enrollment")
df_iti_enrollment = nodes.preprocess_iti_students_enrollment_data(df_iti, geocode=False)
df_iti_enrollment.to_parquet(datasets.get_path("iti_enrollments"), index=False)

# Preprocess ITI students marks
logger.info("Preprocessing ITI students marks")
df_iti_marks = nodes.preprocess_students_marks_data(df_iti_enrollment)
df_iti_marks.to_parquet(datasets.get_path("iti_marks"), index=False)

# Preprocess Diploma students enrollment
logger.info("Preprocessing Diploma students enrollment")
df_diploma_enrollment = nodes.preprocess_diploma_students_enrollment_data(
    df_diploma, geocode=False
)
df_diploma_enrollment.to_parquet(datasets.get_path("diploma_enrollments"), index=False)

# Preprocess Diploma students marks
logger.info("Preprocessing Diploma students marks")
df_diploma_marks = nodes.preprocess_students_marks_data(df_diploma)
df_diploma_marks.to_parquet(datasets.get_path("diploma_marks"), index=False)
