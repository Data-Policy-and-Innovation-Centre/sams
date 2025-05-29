# %%


from sams.config import datasets
import pandas as pd
from plotnine import ggplot, aes, geom_bar, theme_minimal, geom_histogram, labs, scale_x_continuous, geom_density
import numpy as np



# %%
def summary_stats_table(df: pd.DataFrame, summary_var: str, 
                        grouping_label: str = None, 
                        grouping_var: str = None ) -> pd.DataFrame:
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

# %%
# Students
iti_enrollments = pd.read_parquet(datasets["iti_enrollments"]["path"])
diploma_enrollments = pd.read_parquet(datasets["diploma_enrollments"]["path"])
iti_marks = pd.read_parquet(datasets["iti_marks"]["path"])
diploma_marks = pd.read_parquet(datasets["diploma_marks"]["path"])



# %%
# Institutes
iti_strength = pd.read_parquet(datasets["iti_institutes_strength"]["path"])
iti_cutoffs = pd.read_parquet(datasets["iti_institutes_cutoffs"]["path"])
iti_cutoffs_2023 = iti_cutoffs[iti_cutoffs["academic_year"] == 2023]
iti_inst_enrollments = pd.read_parquet(datasets["iti_institutes_enrollments"]["path"])

diploma_strength = pd.read_parquet(datasets["diploma_institutes_strength"]["path"])
diploma_inst_enrollments = pd.read_parquet(datasets["diploma_institutes_enrollments"]["path"])

# %%
iti_enrollments_2023 = iti_enrollments[iti_enrollments["academic_year"] == 2023]
diploma_enrollments_2023 = diploma_enrollments[diploma_enrollments["academic_year"] == 2023]
iti_marks_2023 = iti_marks[iti_marks["academic_year"] == 2023]
diploma_marks_2023 = diploma_marks[diploma_marks["academic_year"] == 2023]

# %%
# Processed data
iti_marks_and_cutoffs = pd.read_parquet(datasets["iti_marks_and_cutoffs"]["path"])
iti_vacancies = pd.read_parquet(datasets["iti_vacancies"]["path"])

# %% [markdown]
# # Trades x Qualifications

# %%
from sams.config import TABLES_DIR
outfile = TABLES_DIR / "trades_by_qualification.xlsx"

# %% [markdown]
# ## Trades by highest qualification

# %%
qual_trade = iti_enrollments_2023.pivot_table(index="reported_branch_or_trade",columns="highest_qualification", values="aadhar_no", aggfunc="count")
qual_trade = qual_trade.replace({np.nan: 0})
qual_trade = qual_trade.astype("int")
qual_trade = qual_trade.sort_values(by=["Graduate and above"], ascending=False)
qual_trade["Total"] = qual_trade.sum(axis=1)

with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
    pd.DataFrame(qual_trade).to_excel(writer, sheet_name="(Table) Trades by Qualification")


# %%
qual_trade_pct = qual_trade.div(qual_trade["Total"], axis=0).drop(["Total"], axis=1)
qual_trade_pct = qual_trade_pct * 100
qual_trade_pct = pd.concat([qual_trade_pct, qual_trade["Total"]], axis=1)
qual_trade_pct = qual_trade_pct.sort_values(by=["Graduate and above"], ascending=False)
qual_trade_pct = qual_trade_pct.applymap(lambda x: '{:.1f}'.format(x))
qual_trade_pct["Total"] = pd.to_numeric(qual_trade_pct["Total"], errors="coerce").astype("int")
qual_trade_pct

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(qual_trade_pct).to_excel(writer, sheet_name="(Table) Trades by Qualification Pct")


# %% [markdown]
# ## Trades by gender

# %%
gender_trade = iti_enrollments_2023.pivot_table(index="reported_branch_or_trade",columns="gender", values="aadhar_no", aggfunc="count")
gender_trade = gender_trade.replace({np.nan: 0})
gender_trade = gender_trade.astype("int")
gender_trade["Total"] = gender_trade.sum(axis=1)
gender_trade

# %%
gender_trade_pct = gender_trade[["Male","Female"]].div(gender_trade["Total"], axis=0)
gender_trade_pct = gender_trade_pct.sort_values(by=["Female"], ascending=False)
gender_trade_pct = gender_trade_pct * 100
gender_trade_pct = gender_trade_pct.applymap(lambda x: '{:.1f}'.format(x))
gender_trade_pct = pd.concat([gender_trade_pct, gender_trade["Total"]], axis=1)
#gender_trade_pct["Total"] = pd.to_numeric(gender_trade_pct["Total"], errors="coerce").astype("int")


with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(gender_trade_pct).to_excel(writer, sheet_name="(Table) Trades by Gender (%)")


# %% [markdown]
# ## Highest qualification by gender

# %%
gender_qual = iti_enrollments_2023.pivot_table(index="gender",columns="highest_qualification", values="aadhar_no", aggfunc="count")
gender_qual = gender_qual.replace({np.nan: 0})
gender_qual["Total"] = gender_qual.sum(axis=1)
gender_qual

# %%
gender_qual_pct = gender_qual.div(gender_qual["Total"], axis=0)
gender_qual_pct = gender_qual_pct.drop(["Total"], axis=1)
#gender_qual_pct = gender_qual_pct.sort_values(by=[""], ascending=False)
gender_qual_pct = gender_qual_pct * 100
gender_qual_pct = gender_qual_pct.applymap(lambda x: '{:.1f}'.format(x))
gender_qual_pct = pd.concat([gender_qual_pct, gender_qual["Total"]], axis=1)
gender_qual_pct

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(gender_qual_pct).to_excel(writer, sheet_name="(Table) Qualification by Gender (%)")

# %% [markdown]
# ## Trades by social category

# %%
category_trade = iti_enrollments_2023.pivot_table(index="reported_branch_or_trade", columns = "social_category", values="aadhar_no", aggfunc="count")
category_trade = category_trade.replace({np.nan: 0})

#category_trade = category_trade.applymap(lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"))
category_trade = category_trade.astype(int)
category_trade["Total"] = category_trade.sum(axis=1)
category_trade

# %%
category_trade_pct = category_trade.div(category_trade["Total"], axis=0)
category_trade_pct = category_trade_pct.drop(["Total"], axis=1)
category_trade_pct = category_trade_pct * 100
category_trade_pct = category_trade_pct.sort_values(by=["Schedule Caste (SC)", "Schedule Tribe (ST)"], ascending=False)
category_trade_pct = category_trade_pct.applymap(lambda x: '{:.1f}'.format(x))
category_trade_pct = pd.concat([category_trade_pct, category_trade["Total"]], axis=1)
category_trade_pct

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(category_trade_pct).to_excel(writer, sheet_name="(Table) Trades by Social Category (%)")


# %% [markdown]
# ## Highest qualification by social category

# %%
category_qual = iti_enrollments_2023.pivot_table(index="social_category",columns="highest_qualification", values="aadhar_no", aggfunc="count")
category_qual = category_qual.replace({np.nan: 0})
category_qual["Total"] = category_qual.sum(axis=1)
category_qual = category_qual.astype("int")

# %%
category_qual_pct = category_qual.div(category_qual["Total"], axis=0)
category_qual_pct = category_qual_pct.drop(["Total"], axis=1)
category_qual_pct = category_qual_pct * 100
category_qual_pct = category_qual_pct.applymap(lambda x: '{:.1f}'.format(x))
category_qual_pct = pd.concat([category_qual_pct, category_qual["Total"]], axis=1)
category_qual_pct["Total"] = pd.to_numeric(category_qual_pct["Total"], errors="coerce").astype("int")
category_qual_pct

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(category_qual_pct).to_excel(writer, sheet_name="(Table) Qualification by Social Category (%)")

#category_qual_pct = category_qual_pct.sort_values(by=["Schedule Caste (SC)", "Schedule Trib (ST)"], ascending=False)

# %% [markdown]
# # Boards, marks, and cutoffs

# %%
# Set up outputfile
outfile = TABLES_DIR / "marks_cutoffs.xlsx"

# %%
iti_marks_and_cutoffs_2023 = iti_marks_and_cutoffs[iti_marks_and_cutoffs["academic_year"] == 2023]

# %%
# Summarize marks by trade
marks_by_trade = summary_stats_table(iti_marks_and_cutoffs_2023, "percentage", grouping_label="Trade", grouping_var="trade")
marks_by_trade

with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
     pd.DataFrame(marks_by_trade).to_excel(writer, sheet_name="(Table) Marks by Trade", index=False)


# %%
# Average marks by demographics
marks_demographics = iti_marks_and_cutoffs_2023.pivot_table(index="gender",columns="social_category", values="percentage", aggfunc="mean")
marks_demographics = marks_demographics.round(1)
marks_demographics

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
     pd.DataFrame(marks_demographics).to_excel(writer, sheet_name="(Table) Marks by Demographics")

# %%
# Summarize marks by boards

boards_marks = summary_stats_table(iti_marks_and_cutoffs_2023, summary_var="percentage", grouping_label="Board", grouping_var="highest_qualification_exam_board")
boards_marks

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
     pd.DataFrame(boards_marks).to_excel(writer, sheet_name="(Table) Boards Marks", index=False)


# %%
institute_types = iti_enrollments_2023[["sams_code","type_of_institute"]].drop_duplicates()
iti_cutoffs_2023 = pd.merge(
    iti_cutoffs_2023,
    institute_types,
    how="left",
    on="sams_code"
)

def coarse_social_category(x):
    if x in ["SC" ,"ST", "UR"]:
        return x
    else:
        return "Other"


iti_cutoffs_2023["social_category"] = iti_cutoffs_2023["social_category"].apply(coarse_social_category)

# %%
def pivot_table(df: pd.DataFrame, index:str , columns:str, values:str, aggfunc:str, index_label: str = None):
    table = df.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc)
    table = table.round(1)
    table = table.reset_index()
    if index_label is not None:
        table = table.rename(columns={index: index_label})
    return table


# %%
# Average cutofs by trade and demographics
cutoff_trade_demographics = pivot_table(iti_cutoffs_2023, index="trade", columns="social_category", 
                                        values="cutoff", aggfunc="mean", index_label="Trade" )
cutoff_trade_gender = pivot_table(iti_cutoffs_2023, index="trade", columns="gender", 
                                        values="cutoff", aggfunc="mean", index_label="Trade" )

cutoff_trade_demographics = cutoff_trade_demographics.sort_values(by=["UR"], ascending=False)
cutoff_trade_gender = cutoff_trade_gender.sort_values(by=["Male"], ascending=False)

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
   pd.DataFrame(cutoff_trade_demographics).to_excel(writer, sheet_name="(Table) Cutoff by Trade and Demographics", index=False)
   pd.DataFrame(cutoff_trade_gender).to_excel(writer, sheet_name="(Table) Cutoff by Trade and Gender", index=False)


# %% [markdown]
# # Distance from home

# %%
outfile = TABLES_DIR / "distance.xlsx"

# %%
# Summarize distance by type of institute
distance_by_type = summary_stats_table(iti_enrollments_2023, summary_var="distance", grouping_label="Type", grouping_var="type_of_institute")
distance_all = summary_stats_table(iti_enrollments_2023, summary_var="distance")
distance_all["Type"] = "All"
distance_by_type = pd.concat([ distance_all, distance_by_type], axis=0)
distance_by_type = distance_by_type.reset_index()
distance_by_type.drop(["index"], axis=1, inplace=True)
distance_by_type = distance_by_type[["Type", "Mean", "Std", "25th", "Median", "75th", "Count"]]

with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
    pd.DataFrame(distance_by_type).to_excel(writer, sheet_name="(Table) Distance by Type", index=False)



# %%
# Summarize distance travelled by gender
distance_by_gender = summary_stats_table(iti_enrollments_2023, summary_var="distance", grouping_label="Gender", grouping_var="gender")
distance_by_gender

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(distance_by_gender).to_excel(writer, sheet_name="(Table) Distance by Gender", index=False)



# %% [markdown]
# # Over/undersubscription

# %%
outfile = TABLES_DIR / "vacancies.xlsx"

# %%
iti_vacancies_2023 = iti_vacancies[iti_vacancies["academic_year"] == 2023]


# %%

# Vacancies by trade by institute
vacancies_by_trade = summary_stats_table(iti_vacancies_2023, summary_var="vacancies", grouping_label="Trade", grouping_var="trade")
vacancy_ratio_by_trade = summary_stats_table(iti_vacancies_2023, summary_var="vacancy_ratio", grouping_label="Trade", grouping_var="trade")

with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
    pd.DataFrame(vacancies_by_trade).to_excel(writer, sheet_name="(Table) Vacancy by Trade", index=False)
    pd.DataFrame(vacancy_ratio_by_trade).to_excel(writer, sheet_name="(Table) Vacancy Ratio by Trade", index=False)



# %%
# Vacancies by institute
vacancies_institute = iti_vacancies_2023.groupby("sams_code").agg({"enrollment": "sum", "strength": "sum", "vacancies": "sum"})
vacancies_institute = pd.merge(iti_enrollments_2023[["sams_code", "reported_institute","institute_district","type_of_institute"]].drop_duplicates(), vacancies_institute, how="right", on="sams_code")
vacancies_institute["vacancy_ratio"] = vacancies_institute["vacancies"] / vacancies_institute["strength"]
vacancies_institute["vacancy_ratio"] = vacancies_institute["vacancy_ratio"].round(2)

# Top 10 institutes by vacancies
top_10_vacant_institutes = vacancies_institute.sort_values(by=["vacancies"], ascending=False).head(10)
top_10_vacant_institutes = top_10_vacant_institutes[["reported_institute", "institute_district","vacancies", "type_of_institute"]]
bottom_10_vacant_institutes = vacancies_institute.sort_values(by=["vacancies"], ascending=True).head(10)
bottom_10_vacant_institutes = bottom_10_vacant_institutes[["reported_institute", "institute_district","vacancies", "type_of_institute"]]

# Top 10 institutes by vacancy ratios
top_10_vacancy_ratio_institutes = vacancies_institute.sort_values(by=["vacancy_ratio"], ascending=False).head(10)
top_10_vacancy_ratio_institutes = top_10_vacancy_ratio_institutes[["reported_institute", "institute_district","vacancy_ratio", "type_of_institute"]]
bottom_10_vacancy_ratio_institutes = vacancies_institute.sort_values(by=["vacancy_ratio"], ascending=True).head(10)
bottom_10_vacancy_ratio_institutes = bottom_10_vacancy_ratio_institutes[["reported_institute", "institute_district","vacancy_ratio", "type_of_institute"]]

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(top_10_vacant_institutes).to_excel(writer, sheet_name="(Table) Top 10 Institutes by Vacancy", index=False)
    pd.DataFrame(bottom_10_vacant_institutes).to_excel(writer, sheet_name="(Table) Bottom 10 Institutes by Vacancy", index=False)
    pd.DataFrame(top_10_vacancy_ratio_institutes).to_excel(writer, sheet_name="(Table) Top 10 Institutes by Vacancy Ratio", index=False)
    pd.DataFrame(bottom_10_vacancy_ratio_institutes).to_excel(writer, sheet_name="(Table) Bottom 10 Institutes by Vacancy Ratio", index=False)


# %%
# Vacancies by district
vacancies_district = vacancies_institute.groupby("institute_district").agg({"enrollment": "sum", "strength": "sum", "vacancies": "sum"})
vacancies_district["vacancy_ratio"] = vacancies_district["vacancies"] / vacancies_district["strength"]
vacancies_district["vacancy_ratio"] = vacancies_district["vacancy_ratio"].round(2)

# Top 10 districts by vacancies
top_10_vacancies_district = vacancies_district.sort_values(by=["vacancies"], ascending=False).head(10)
bottom_10_vacancies_district = vacancies_district.sort_values(by=["vacancies"], ascending=True).head(10)

# Top 10 districts by vacnacy ratios
top_10_vacancy_ratio_district = vacancies_district.sort_values(by=["vacancy_ratio"], ascending=False).head(10)
bottom_10_vacancy_ratio_district = vacancies_district.sort_values(by=["vacancy_ratio"], ascending=True).head(10)

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(top_10_vacancies_district).to_excel(writer, sheet_name="(Table) Top 10 Districts by Vacancy", index=True)
    pd.DataFrame(bottom_10_vacancies_district).to_excel(writer, sheet_name="(Table) Bottom 10 Districts by Vacancy", index=True)
    pd.DataFrame(top_10_vacancy_ratio_district).to_excel(writer, sheet_name="(Table) Top 10 Districts by Vacancy Ratio", index=True)
    pd.DataFrame(bottom_10_vacancy_ratio_district).to_excel(writer, sheet_name="(Table) Bottom 10 Districts by Vacancy Ratio", index=True)



# %%
# Vacancies by trade
vacancies_trade = iti_vacancies_2023.groupby("trade").agg({"enrollment": "sum", "strength": "sum", "vacancies": "sum"})
vacancies_trade["vacancy_ratio"] = vacancies_trade["vacancies"] / vacancies_trade["strength"]
vacancies_trade["vacancy_ratio"] = vacancies_trade["vacancy_ratio"].round(2)

# Top 10 trades by vacancy
top_10_vacancies_trade = vacancies_trade.sort_values(by=["vacancies"], ascending=False).head(10)
bottom_10_vacancies_trade = vacancies_trade.sort_values(by=["vacancies"], ascending=True).head(10)

# Top 10 trades by vacnacy ratios
top_10_vacancy_ratio_trade = vacancies_trade.sort_values(by=["vacancy_ratio"], ascending=False).head(10)
bottom_10_vacancy_ratio_trade = vacancies_trade.sort_values(by=["vacancy_ratio"], ascending=True).head(10)

with pd.ExcelWriter(outfile, engine='openpyxl', mode='a') as writer:
    pd.DataFrame(top_10_vacancies_trade).to_excel(writer, sheet_name="(Table) Top 10 Trades by Vacancy", index=True)
    pd.DataFrame(bottom_10_vacancies_trade).to_excel(writer, sheet_name="(Table) Bottom 10 Trades by Vacancy", index=True)
    pd.DataFrame(top_10_vacancy_ratio_trade).to_excel(writer, sheet_name="(Table) Top 10 Trades by Vacancy Ratio", index=True)
    pd.DataFrame(bottom_10_vacancy_ratio_trade).to_excel(writer, sheet_name="(Table) Bottom 10 Trades by Vacancy Ratio", index=True)

# %%
top_10_vacancies_district

# %% [markdown]
# # Geography

# %%
outfile = TABLES_DIR / "geography.xlsx"

# %%
iti_female_shares_by_district = iti_enrollments_2023.pivot_table(
    index="district", columns="gender", values="aadhar_no", aggfunc="count"
)
iti_female_shares_by_district["avg_female_share"] = iti_female_shares_by_district["Female"] / (
    iti_female_shares_by_district["Female"] + iti_female_shares_by_district["Male"]
)
iti_female_shares_by_district = iti_female_shares_by_district.sort_values(by=["avg_female_share"], ascending=False)
iti_female_shares_by_district.rename(columns={"avg_female_share": "Female share"},inplace=True)
iti_female_shares_by_district["Female share"] = iti_female_shares_by_district["Female share"].round(2)

with pd.ExcelWriter(outfile, engine='openpyxl', mode='w') as writer:
    pd.DataFrame(iti_female_shares_by_district).to_excel(writer, sheet_name="(Table) Female Shares by District")




