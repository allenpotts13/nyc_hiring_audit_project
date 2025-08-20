import pandas as pd
from utils.minio_client import get_minio_client
from utils.logger import setup_logger
from dotenv import load_dotenv
from minio import Minio
from dotenv import load_dotenv
import os
from io import BytesIO
from rapidfuzz import fuzz as rapid_fuzz
from datetime import datetime, timedelta
import duckdb

load_dotenv()


def calculate_fuzzy_match(target_string, candidate_list_of_strings, use_rapidfuzz=True):
    best_match = None
    best_ratio = -1

    for candidate in candidate_list_of_strings:
        ratio = rapid_fuzz.token_set_ratio(target_string, candidate)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = candidate

    return best_match, best_ratio

def process_job_postings_gold(duckdb_path, nrows=1000):
    con = duckdb.connect(duckdb_path)

    payroll_lookup_df = con.execute("SELECT * FROM citywide_payroll_silver").df()
    job_postings_df = con.execute("SELECT * FROM jobs_postings_silver").df()

    job_postings_df["posting_date"] = pd.to_datetime(job_postings_df["posting_date"], errors="coerce")
    job_postings_df = job_postings_df[job_postings_df["posting_date"].dt.year.isin([2024, 2025])].head(nrows)

    job_postings_df["post_until"] = pd.to_datetime(job_postings_df["post_until"], errors="coerce")
    default_duration = 30
    def calc_duration(row):
        if pd.notnull(row["post_until"]):
            return (row["post_until"] - row["posting_date"]).days
        elif pd.notnull(row["posting_date"]):
            return default_duration
        else:
            return None
    job_postings_df["posting_duration_days"] = job_postings_df.apply(calc_duration, axis=1)

    payroll_strings = payroll_lookup_df["fuzzy_compare"].tolist()
    payroll_salaries = payroll_lookup_df.set_index("fuzzy_compare")["payroll_salary_annual"].to_dict()

    matched_titles = []
    matched_salaries = []
    match_ratios = []

    for idx, row in job_postings_df.iterrows():
        business_title = str(row["business_title"])
        best_match, best_ratio = calculate_fuzzy_match(business_title, payroll_strings)
        if best_ratio >= 85:
            matched_titles.append(best_match)
            matched_salaries.append(payroll_salaries.get(best_match, None))
            match_ratios.append(best_ratio)
        else:
            matched_titles.append(None)
            matched_salaries.append(None)
            match_ratios.append(best_ratio)

    job_postings_df["Payroll Fuzzy Match"] = matched_titles
    job_postings_df["Payroll Salary Annual"] = matched_salaries
    job_postings_df["Match Ratio"] = match_ratios

    gold_df = job_postings_df[job_postings_df["Match Ratio"] >= 85][[
        "job_id",
        "business_title",
        "agency",
        "posting_date",
        "post_until",
        "posting_duration_days",
        "Payroll Fuzzy Match",
        "Payroll Salary Annual",
        "Match Ratio"
    ]]

    con.execute("CREATE OR REPLACE TABLE nyc_job_postings_gold AS SELECT * FROM gold_df")
    con.close()

    return gold_df

def main():
    duckdb_path = "duckdb/nyc_hiring.duckdb"
    process_job_postings_gold(duckdb_path)

if __name__ == "__main__":
    main()
