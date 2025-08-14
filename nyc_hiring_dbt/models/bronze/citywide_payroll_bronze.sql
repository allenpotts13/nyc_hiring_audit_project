CREATE OR REPLACE TABLE payroll_bronze AS
SELECT * FROM read_parquet('s3://project4-bronze/raw/Citywide_Payroll_Data.parquet');