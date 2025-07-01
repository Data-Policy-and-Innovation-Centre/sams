import pandas as pd
import pandas as pd
import json


def preprocess_option_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten option_data from students and classify as Govt., Pvt., or Mix.
    """
    import json

    govt_institutes = set([
        "ITI Balasore", "ITI Cuttack", "Govt. ITI, Paradeep"  # yet had to add full list from ["pvt"], checking how to do that, without hardcoding

    ])
    pvt_institutes = set([
        "Guru ITC", "OP Jindal Institute of Technology  & Skills ITC"  # yet had to add full list from ["pvt"], checking how to do that, without hardcoding

    ])

    all_options = []
    for aadhar, option_json, academic_year, module in df[["aadhar_no", "option_data", "academic_year", "module"]].values:
        try:
            options = json.loads(option_json) if option_json else []
            inst_names = {opt.get("Institute_Name", "").strip() for opt in options}
        except Exception:
            continue

        if inst_names.issubset(govt_institutes):
            inst_type = "Govt."
        elif inst_names.issubset(pvt_institutes):
            inst_type = "Pvt."
        else:
            inst_type = "Mix"

        for opt in options:
            record = {
                "aadhar_no": aadhar,
                "academic_year": academic_year,
                "module": module,
                "option_number": opt.get("option_number"),
                "institute_name": opt.get("Institute_Name"),
                "trade_name": opt.get("Trade_Name"),
                "type": inst_type,
                "status": opt.get("Status")
            }
            all_options.append(record)

    return pd.DataFrame(all_options)


def summary_option_data(option_data: pd.DataFrame) -> pd.DataFrame:
    """Summarizes application stats"""
    summary = (
        option_data.groupby(["academic_year", "module"])
        .agg(
            applications=("aadhar_no", "count"),
            unique_applicants=("aadhar_no", pd.Series.nunique),
            reported=("status", lambda x: (x == "Reported").sum())
        )
        .reset_index()
    )
    summary["apps_per_student"] = summary["applications"] / summary["unique_applicants"]
    return summary

