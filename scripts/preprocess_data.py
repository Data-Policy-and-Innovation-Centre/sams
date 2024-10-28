from sams.preprocessing import nodes
import pandas as pd
import sqlite3
import sams.config as config

# Preprocess ITI students enrollment
con = sqlite3.connect(config.get_path("sams"))
df_iti_enrollment = pd.read_sql_query(
    "SELECT * FROM students WHERE module = 'ITI';", con
)
df_iti_enrollment = nodes.preprocess_iti_students_enrollment_data(df_iti_enrollment)
df_iti_enrollment.to_parquet(config.get_path("iti_enrollments"), index=False)

# Preprocess ITI students marks
df_iti_marks = nodes.preprocess_students_marks_data(df_iti_enrollment)
df_iti_marks.to_parquet(config.get_path("iti_marks"), index=False)

# Preprocess Diploma students enrollment
df_diploma_enrollment = pd.read_sql_query(
    "SELECT * FROM students WHERE module = 'Diploma';", con
)
df_diploma_enrollment = nodes.preprocess_diploma_students_enrollment_data(
    df_diploma_enrollment
)
df_diploma_enrollment.to_parquet(config.get_path("diploma_enrollments"), index=False)

# Preprocess Diploma students marks
df_diploma_marks = nodes.preprocess_students_marks_data(df_diploma_enrollment)
df_diploma_marks.to_parquet(config.get_path("diploma_marks"), index=False)
