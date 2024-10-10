from loguru import logger
import pandas as pd
from sams.config import MISSING_VALUES
from pathlib import Path

def count_null_values(data: list, table_name: str = "students") -> None:

    """
    Counts the number of null values in each column of the given data and writes it to a log file.

    Parameters
    ----------
    data : list
        A list of dictionaries where each dictionary represents a row in the data.
    table_name : str, optional
        The name of the table to be validated. It can be either "students" or "institutes".

    Raises
    ------
    ValueError
        If the table name is not valid.
    Exception
        If not all values of module and year are constant in the data.
        If not all values of admission_type are constant in the data when module is Diploma and table name is institutes.

    Notes
    -----
    This function does not return anything. It just writes the counts of missing values to a file.
    """

    if table_name not in ["students", "institutes"]:
        raise ValueError(f"Invalid table name: {table_name}")

    df = pd.DataFrame(data)

    # Check if all values of module and year are constant
    if not (df["module"].nunique() == 1 and df["academic_year"].nunique() == 1):
        raise Exception(f"All values of module and academic_year must be constant.")

    # Check if admission_type is constant if module is Diploma and table name is institutes
    if (df["module"].iloc[0] == "Diploma" and table_name == "institutes" and
            df["admission_type"].nunique() != 1):
        raise Exception(f"All values of admission_type must be constant.")
    
    # Count nulls
    null_counts = df.isnull().sum()
    null_counts += (df == "").sum()
    null_counts += (df == " ").sum()
    null_counts += (df == "NA").sum()

    # Write null counts to a file
    log_file = Path(MISSING_VALUES / f"missing_values_{table_name}_{df['module'].iloc[0]}_{df['academic_year'].iloc[0]}.log")
    if not log_file.exists():
        log_file.touch()
    with open(log_file, "w") as f:
        f.write(f"Metadata: {table_name}, {df['module'].iloc[0]}, {df['academic_year'].iloc[0]}\n")
        if table_name == "institutes":
            f.write(f"Admission Type: {df['admission_type'].iloc[0]}\n")
        f.write(f"Total Records: {len(df)}\n")
        f.write(f"Missing Values:\n")
        for var, count in null_counts[null_counts > 0].items():
            f.write(f"{var}: {count}\n")
        f.write("\n\n\n")

def validate(data: list, table_name: str = "students") -> None:

    count_null_values(data, table_name)
    

        

def check_null_values(row: dict, 
                      varlist: list = ['Barcode','module','academic_year',
                                       'AppliedStatus','EnrollmentStatus','AdmissionStatus','Phase','Year']) -> bool:
    """Check if any of the given variables in the row are null or empty.
    
    Parameters
    ----------
    row : dict
        A dictionary containing the row data.
    varlist : list
        A list of variable names to check.
    
    Returns
    -------
    bool
        False if all variables are not null or empty, otherwise False.
    """
    for var in varlist:
        if row[var] is None or row[var] in ["", " ", "NA"]:
            return True

    return False

