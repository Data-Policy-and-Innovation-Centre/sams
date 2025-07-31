import pandas as pd

# df = pd.read_sql_query(query, conn)
# conn.close()

# df.head()

print("========== [1] DATASET OVERVIEW ==========\n")

# Shape and year range
total_rows = df.shape[0]
academic_years = df['academic_year'].dropna().sort_values().unique()
academic_range = f"{academic_years[0]} – {academic_years[-1]}" if len(academic_years) > 0 else "Unknown"

# Unique students per year and overall (based on aadhar_no)
students_per_year = df.groupby('year')['aadhar_no'].nunique().reset_index(name='No. of Students')
total_unique_students = df['aadhar_no'].nunique()

print(f"\nTotal No. of Students: {total_unique_students}")
print(f"Total Applications: {total_rows}")
print(f"Academic Years Covered: {academic_range}\n")

print("\n No. of Students by Year:")
print(students_per_year.to_string(index=False))
print()

# Missing data summary
missing_summary = df.isnull().sum()
missing_summary = missing_summary[missing_summary > 0]
missing_percent = (missing_summary / total_rows * 100).round(2)
missing_df = pd.DataFrame({'Missing Count': missing_summary, 'Missing (%)': missing_percent})
print("Missing Fields (count and %):")
print(missing_df.sort_values(by='Missing Count', ascending=False).to_string())
print()

print("========== [2] STUDENT-LEVEL APPLICATION COUNT ==========\n")
# Group by student, count application rows 
student_option_counts = df.groupby('aadhar_no').size()

# Describe this distribution and drop 'max'
student_summary = student_option_counts.describe(percentiles=[0.25, 0.5, 0.75])

# Keep only selected rows
student_summary = student_summary.loc[['count', 'mean', 'std', 'min', '25%', '50%', '75%']]

# Rename for display
student_summary = student_summary.rename({
    'count': 'No. of Students',
    'mean': 'Avg Application per Student',
    'std': 'Std Dev',
    'min': 'Min Options',
    '25%': '25%',
    '50%': 'Median',
    '75%': '75%'
}).round(2)

print(student_summary.to_frame().T.to_string(index=False))
print()

# ========== [3] CATEGORICAL SUMMARY ==========
print("========== [3] CATEGORICAL SUMMARY ==========\n")

categorical_vars = [
    'gender', 'state', 'district', 'social_category', 'highest_qualification',
    'annual_income', 'course_period', 'reported_institute', 'Phase', 'Status'
]

cat_summary = []
for col in [col for col in categorical_vars if col in df.columns]:
    counts = df[col].value_counts(dropna=True)
    if not counts.empty:
        top_value = counts.index[0]
        top_count = counts.iloc[0]
        total = df[col].notnull().sum()
        top_percent = round((top_count / total) * 100, 2)
        cat_summary.append({
            'Variable': col,
            'Most Frequent': str(top_value),
            'Frequency (%)': top_percent,
            'No. of Observations': total
        })

summary_df = pd.DataFrame(cat_summary)
print(summary_df.to_string(index=False))
