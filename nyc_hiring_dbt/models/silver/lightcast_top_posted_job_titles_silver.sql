{{ config(
    pre_hook=[
        "SET s3_access_key_id='admin'",
        "SET s3_secret_access_key='password'",
        "SET s3_endpoint='host.docker.internal:9000'",
        "SET s3_url_style='path'",
        "SET s3_use_ssl=false"
    ]
) }}
SELECT
    "Job Title" AS job_title,
    CAST(REPLACE("Total Postings (Jan 2024 - Jun 2025)", ',', '') AS INTEGER) AS total_postings,
    CAST(REPLACE("Unique Postings (Jan 2024 - Jun 2025)", ',', '') AS INTEGER) AS unique_postings,
        "Median Posting Duration" AS median_posting_duration
FROM lightcast_top_posted_job_titles_bronze
