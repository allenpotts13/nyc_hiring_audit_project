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
    CAST(job_id AS VARCHAR) AS job_id,
    TRIM(agency) AS agency,
    TRIM(business_title) AS business_title,
    TRIM(civil_service_title) AS civil_service_title,
    TRIM(job_category) AS job_category,
    TRIM(full_time_part_time_indicator) AS full_time_part_time_indicator,
    TRIM(career_level) AS career_level,
    CAST(salary_range_from AS DOUBLE) AS salary_range_from,
    CAST(salary_range_to AS DOUBLE) AS salary_range_to,
    TRIM(salary_frequency) AS salary_frequency,
    TRIM(work_location) AS work_location,
    TRIM(division_work_unit) AS division_work_unit,

    TRY_CAST(posting_date AS TIMESTAMP) AS posting_date,
    TRY_CAST(post_until AS TIMESTAMP) AS post_until,

    CASE
        WHEN TRY_CAST(post_until AS TIMESTAMP) IS NOT NULL AND TRY_CAST(posting_date AS TIMESTAMP) IS NOT NULL
            THEN DATE_DIFF('day', TRY_CAST(posting_date AS TIMESTAMP), TRY_CAST(post_until AS TIMESTAMP))
        ELSE NULL
    END AS posting_duration_days,

    CONCAT(TRIM(agency), ' ', TRIM(business_title)) AS fuzzy_compare
FROM jobs_postings_bronze
WHERE job_id IS NOT NULL
  AND agency IS NOT NULL
  AND business_title IS NOT NULL