import pandas as pd

def summary_stats_table(df: pd.DataFrame, summary_var: str, 
                        grouping_label: str = None, 
                        grouping_var: str = None ) -> pd.DataFrame:
    """
    Calculate summary statistics for a given variable in a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to calculate summary statistics from
    summary_var : str
        Variable to calculate summary statistics for
    grouping_label : str, optional
        Label for a grouping variable, by default None
    grouping_var : str, optional
        Grouping variable to group summary statistics by, by default None

    Returns
    -------
    pd.DataFrame
        DataFrame with summary statistics, including mean, standard deviation, 25th and 75th percentiles, median, and count.
    """
    if grouping_var:
        summary = df.groupby(grouping_var)[summary_var].agg(
            mean='mean',
            std_dev='std',
            percentile_25=lambda x: x.quantile(0.25),
            median='median',
            percentile_75=lambda x: x.quantile(0.75),
            count = "count"
        ).reset_index()
    else:
        summary = df[summary_var].agg(
            mean='mean',
            std_dev='std',
            percentile_25=lambda x: x.quantile(0.25),
            median='median',
            percentile_75=lambda x: x.quantile(0.75),
            count = "count"
        )
    if grouping_label:
        summary.columns = [grouping_label, "Mean", "Std", "25th", "Median", "75th", "Count"]
    else:
        if grouping_var:
            summary.columns = [grouping_var, "Mean", "Std", "25th", "Median", "75th", "Count"]
        else:
            summary = pd.DataFrame(summary).transpose()
            summary.columns = ["Mean", "Std", "25th", "Median", "75th", "Count"]
    summary["Count"] = summary["Count"].astype(int)
    summary[["Mean", "Std", "25th", "Median", "75th"]] = summary[["Mean", "Std", "25th", "Median", "75th"]].round(1)
    summary = summary.sort_values("Count", ascending=False)
    return summary

def pivot_table(df: pd.DataFrame, index:str , values:str, aggfunc:str, round: int = None,index_label: str = None, value_label: str = None):
    """
    Perform a pivot table operation and round the result.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to perform pivot table on
    index : str
        Column to use as the index
    values : str
        Column to use for the values
    aggfunc : str
        Function to use for aggregation
    round : int, optional
        Number of decimal places to round the result to, by default None
    index_label : str, optional
        Label to use for the index column, by default None
    value_label : str, optional
        Label to use for the values column, by default None

    Returns
    -------
    pd.DataFrame
        DataFrame with the result of the pivot table
    """
    out = df.pivot_table( index=index, values=values, aggfunc=aggfunc)
    if round:
        out = out.round(round)
    out = out.reset_index()
    if index_label is not None:
        out = out.rename(columns={index: index_label})
    if value_label is not None:
        out = out.rename(columns={values: value_label})
    return out

def save_table_excel(dfs: list[pd.DataFrame], sheet_names: list[str], index: list[bool], outfile: str):
    """
    Save multiple DataFrames to an Excel file with specified sheet names.

    Parameters
    ----------
    dfs : list[pd.DataFrame]
        List of DataFrames to save to Excel.
    sheet_names : list[str]
        List of sheet names corresponding to each DataFrame.
    index: list[bool]
        List of booleans indicating whether to include the index column in each DataFrame.
    outfile : str
        Path to the output Excel file.

    Raises
    ------
    ValueError
        If the number of DataFrames does not match the number of sheet names.

    """
    if len(dfs) != len(sheet_names):
        raise ValueError("The number of input DataFrames must be equal to the number of sheet names")

    if len(dfs) != len(index):
        raise ValueError("The number of input DataFrames must be equal to the number of index bool values")

    with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
        for df, sheet_name, index in zip(dfs, sheet_names, index):
            df.to_excel(writer, sheet_name=sheet_name, index=index)