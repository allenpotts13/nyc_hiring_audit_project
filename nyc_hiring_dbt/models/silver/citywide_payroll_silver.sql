CREATE OR REPLACE TABLE payroll_silver AS
SELECT
    CAST("Fiscal Year" AS INTEGER) AS fiscal_year,
    TRIM("Agency Name") AS agency_name,
    TRIM("Title Description") AS job_title,
    TRIM("Work Location Borough") AS borough,
    TRIM("Leave Status as of June 30") AS leave_status,
    CASE
        WHEN LOWER("Pay Basis") = 'per annum' AND "Base Salary" IS NOT NULL
            THEN CAST("Base Salary" AS DOUBLE)
        ELSE NULL
    END AS payroll_salary_annual,
    CAST("Regular Hours" AS DOUBLE) AS regular_hours,
    CAST("Regular Gross Paid" AS DOUBLE) AS regular_gross_paid,
    CAST("OT Hours" AS DOUBLE) AS ot_hours,
    CAST("Total OT Paid" AS DOUBLE) AS total_ot_paid,
    CAST("Total Other Pay" AS DOUBLE) AS total_other_pay,
    CONCAT(TRIM("Agency Name"), ' ', TRIM("Title Description")) AS fuzzy_compare
FROM payroll_bronze
WHERE "Agency Name" IS NOT NULL
  AND "Title Description" IS NOT NULL
  AND "Base Salary" IS NOT NULL
  AND LOWER("Pay Basis") = 'per annum';

COPY payroll_silver TO 's3://project4-silver/payroll_silver.parquet' (FORMAT PARQUET);