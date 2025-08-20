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
from functools import reduce

load_dotenv()

logger = setup_logger(__name__, "logs/data_processors.log")


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

    gold_df = job_postings_df[[
        "job_id",
        "business_title",
        "agency",
        "posting_date",
        "post_until",
        "posting_duration_days",
        "salary_range_from", 
        "salary_range_to",    
        "Payroll Fuzzy Match",
        "Payroll Salary Annual",
        "Match Ratio"
    ]].copy()

    mask = gold_df["Match Ratio"] < 85
    gold_df.loc[mask, ["Payroll Fuzzy Match", "Payroll Salary Annual", "Match Ratio"]] = pd.NA

    con.execute("CREATE OR REPLACE TABLE nyc_job_postings_gold AS SELECT * FROM gold_df")
    con.close()

    return gold_df

def generate_posting_duration_dataset(duckdb_path, nrows=1000):
    con = duckdb.connect(duckdb_path)

    jobs_df = con.execute("SELECT * FROM jobs_postings_silver").df()
    jobs_df["posting_date"] = pd.to_datetime(jobs_df["posting_date"], errors="coerce")
    jobs_df = jobs_df[jobs_df["posting_date"].dt.year.isin([2024, 2025])].head(nrows)


    lightcast_tables = [
        "lightcast_top_posted_job_titles_silver",
        "lightcast_top_posted_occupations_silver",
        "lightcast_top_posted_occupations_onet_silver",
        "lightcast_top_posted_occupations_soc_silver"
    ]

    for table in lightcast_tables:
        lightcast_df = con.execute(f"SELECT * FROM {table}").df()
        lightcast_titles = lightcast_df["job_title"].astype(str).tolist() if "job_title" in lightcast_df.columns else []

        matched_titles = []
        match_ratios = []
        for idx, row in jobs_df.iterrows():
            business_title = str(row["business_title"])
            best_match, best_ratio = calculate_fuzzy_match(business_title, lightcast_titles)
            if best_ratio >= 75:
                matched_titles.append(best_match)
                match_ratios.append(best_ratio)
            else:
                matched_titles.append(None)
                match_ratios.append(best_ratio)

        result_df = jobs_df.copy()
        result_df[f"{table}_match_title"] = matched_titles
        result_df[f"{table}_match_ratio"] = match_ratios

        final_columns = [
            "job_id", "business_title", "agency", "posting_date", "post_until", "posting_duration_days",
            f"{table}_match_title", f"{table}_match_ratio"
        ]

        table_suffix = table.replace('lightcast_top_posted_', '').replace('_silver', '')
        gold_table_name = f"nyc_posting_duration_gold_{table_suffix}"

        con.register("temp_result_df", result_df)
        select_cols = ', '.join([f'"{col}"' for col in final_columns])
        con.execute(f"CREATE OR REPLACE TABLE {gold_table_name} AS SELECT {select_cols} FROM temp_result_df")
        con.unregister("temp_result_df")
    con.close()

def main():
    duckdb_path = "duckdb/nyc_hiring.duckdb"
    process_job_postings_gold(duckdb_path)
    generate_posting_duration_dataset(duckdb_path)

if __name__ == "__main__":
    main()
