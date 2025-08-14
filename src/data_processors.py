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


def process_payroll_data(nrows=1000):
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    payroll_parquet_path = os.getenv("RAW_PAYROLL_PARQUET_PATH")
    if not bucket_name or not payroll_parquet_path:
        raise ValueError("MINIO_BUCKET_NAME or RAW_PAYROLL_PARQUET_PATH environment variable not set.")

    minio_client = get_minio_client()
    response = minio_client.get_object(bucket_name, payroll_parquet_path)
    parquet_bytes = BytesIO(response.read())
    df = pd.read_parquet(parquet_bytes)

    df = df.head(nrows)
    cols = [
        "Agency Name",
        "Job Title",
        "Annual Salary",
        "Hourly Rate",
        "Salary Frequency"
    ]
    df = df[cols]

    def compute_annual(row):
        if pd.notnull(row["Annual Salary"]):
            return row["Annual Salary"]
        elif row["Salary Frequency"] == "Hourly" and pd.notnull(row["Hourly Rate"]):
            return row["Hourly Rate"] * 2080  
        else:
            return None

    df["Payroll Salary Annual"] = df.apply(compute_annual, axis=1)

    df["fuzzy_compare"] = df["Agency Name"].astype(str) + " " + df["Job Title"].astype(str)

    return df[["Agency Name", "Job Title", "Payroll Salary Annual", "fuzzy_compare"]]

def calculate_fuzzy_match(target_string, candidate_list_of_strings, use_rapidfuzz=True):
    best_match = None
    best_ratio = -1

    for candidate in candidate_list_of_strings:
        ratio = rapid_fuzz.token_set_ratio(target_string, candidate)
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = candidate

    return best_match, best_ratio

def process_job_postings_data(payroll_lookup_df, nrows=1000):
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    job_postings_parquet_path = os.getenv("RAW_JOBS_POSTINGS_PARQUET_PATH")
    if not bucket_name or not job_postings_parquet_path:
        raise ValueError("MINIO_BUCKET_NAME or RAW_JOBS_POSTINGS_PARQUET_PATH environment variable not set.")

    minio_client = get_minio_client()
    response = minio_client.get_object(bucket_name, job_postings_parquet_path)
    parquet_bytes = BytesIO(response.read())
    df = pd.read_parquet(parquet_bytes)

    df = df.head(nrows)
    df["Posting Date"] = pd.to_datetime(df["Posting Date"], errors="coerce")
    df = df[df["Posting Date"].dt.year.isin([2024, 2025])]

    df["Post Until"] = pd.to_datetime(df["Post Until"], errors="coerce")
    today = pd.Timestamp(datetime.today())
    default_duration = 30

    def calc_duration(row):
        if pd.notnull(row["Post Until"]):
            return (row["Post Until"] - row["Posting Date"]).days
        elif pd.notnull(row["Posting Date"]):
            return default_duration
        else:
            return None

    df["Posting Duration Days"] = df.apply(calc_duration, axis=1)

    payroll_strings = payroll_lookup_df["fuzzy_compare"].tolist()
    payroll_salaries = payroll_lookup_df.set_index("fuzzy_compare")["Payroll Salary Annual"].to_dict()

    matched_titles = []
    matched_salaries = []
    match_ratios = []

    for idx, row in df.iterrows():
        business_title = str(row["Business Title"])
        best_match, best_ratio = calculate_fuzzy_match(business_title, payroll_strings, use_rapidfuzz=True)
        if best_ratio >= 85:
            matched_titles.append(best_match)
            matched_salaries.append(payroll_salaries.get(best_match, None))
            match_ratios.append(best_ratio)
        else:
            matched_titles.append(None)
            matched_salaries.append(None)
            match_ratios.append(best_ratio)

    df["Payroll Fuzzy Match"] = matched_titles
    df["Payroll Salary Annual"] = matched_salaries
    df["Match Ratio"] = match_ratios


    final_cols = [
        "Job ID",
        "Business Title",
        "Agency",
        "Posting Date",
        "Post Until",
        "Posting Duration Days",
        "Payroll Fuzzy Match",
        "Payroll Salary Annual",
        "Match Ratio"
    ]
    return df[final_cols]

def process_lightcast_data(nrows=1000):
    """
    Reads the Lightcast Excel file from MinIO, cleans and standardizes columns.
    Returns the processed DataFrame.
    """
    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    lightcast_xlsx_path = os.getenv("RAW_LIGHTCAST_FILE")
    if not bucket_name or not lightcast_xlsx_path:
        raise ValueError("MINIO_BUCKET_NAME or RAW_LIGHTCAST_FILE environment variable not set.")

    minio_client = get_minio_client()
    response = minio_client.get_object(bucket_name, lightcast_xlsx_path)
    xlsx_bytes = BytesIO(response.read())
    df = pd.read_excel(xlsx_bytes)
    df = df.head(nrows)
    df = df.rename(columns={
        "Job Title": "Lightcast Job Title",
        "Total Postings": "Lightcast Total Postings",
        "Median Posting Duration": "Lightcast Median Posting Duration"
    })

    cols = [
        "Lightcast Job Title",
        "Lightcast Total Postings",
        "Lightcast Median Posting Duration"
    ]
    df = df[cols]

    return df