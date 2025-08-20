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
    CAST("fiscal_year" AS INTEGER) AS fiscal_year,
    TRIM("agency_name") AS agency_name,
    TRIM("title_description") AS job_title,
    TRIM("work_location_borough") AS borough,
    TRIM("leave_status_as_of_june_30") AS leave_status,
    CASE
        WHEN LOWER("pay_basis") = 'per annum' AND "base_salary" IS NOT NULL
            THEN CAST("base_salary" AS DOUBLE)
        ELSE NULL
    END AS payroll_salary_annual,
    CAST("regular_hours" AS DOUBLE) AS regular_hours,
    CAST("regular_gross_paid" AS DOUBLE) AS regular_gross_paid,
    CAST("ot_hours" AS DOUBLE) AS ot_hours,
    CAST("total_ot_paid" AS DOUBLE) AS total_ot_paid,
    CAST("total_other_pay" AS DOUBLE) AS total_other_pay,
    CONCAT(TRIM("agency_name"), ' ', TRIM("title_description")) AS fuzzy_compare
FROM citywide_payroll_bronze
WHERE "agency_name" IS NOT NULL
  AND "title_description" IS NOT NULL
  AND "base_salary" IS NOT NULL
  AND LOWER("pay_basis") = 'per annum'
