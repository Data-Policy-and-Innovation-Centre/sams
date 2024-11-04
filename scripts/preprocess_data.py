from sams.preprocessing import nodes
import pandas as pd
import sqlite3
from sams.config import datasets
from loguru import logger

# Load raw data
logger.info("Loading raw data from SQLite database")
con = sqlite3.connect(datasets["sams"]["path"])
df_diploma = pd.read_sql_query("SELECT * FROM students WHERE module = 'Diploma';", con)

# Preprocess Diploma students enrollment
logger.info("Preprocessing Diploma students enrollment")
df_diploma_enrollment = nodes.preprocess_diploma_students_enrollment_data(df_diploma)


# Preprocess Diploma students marks
logger.info("Preprocessing Diploma students marks")
df_diploma_marks = nodes.preprocess_students_marks_data(df_diploma)
