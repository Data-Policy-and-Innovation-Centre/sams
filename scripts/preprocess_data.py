from sams.preprocessing import nodes
import pandas as pd
import sqlite3
from sams.config import datasets
from loguru import logger

# Load raw data
logger.info("Loading raw data from SQLite database")
con = sqlite3.connect(datasets["sams"]["path"])
df_iti = pd.read_sql_query("SELECT * FROM students where module = 'ITI';", con)
df_diploma = pd.read_sql_query("SELECT * FROM students WHERE module = 'Diploma';", con)

# Preprocess geocodes
logger.info("Preprocessing geocodes using pin codes")
df_geocodes = nodes.preprocess_geocodes([df_iti, df_diploma])
df_geocodes.to_parquet(datasets["geocodes"]["path"], index=False)

# Preprocess ITI students enrollment
logger.info("Preprocessing ITI students enrollment")
df_iti_enrollment = nodes.preprocess_iti_students_enrollment_data(df_iti)


# Preprocess ITI students marks
logger.info("Preprocessing ITI students marks")
df_iti_marks = nodes.preprocess_students_marks_data(df_iti_enrollment)


# Preprocess Diploma students enrollment
logger.info("Preprocessing Diploma students enrollment")
df_diploma_enrollment = nodes.preprocess_diploma_students_enrollment_data(df_diploma)


# Preprocess Diploma students marks
logger.info("Preprocessing Diploma students marks")
df_diploma_marks = nodes.preprocess_students_marks_data(df_diploma)
