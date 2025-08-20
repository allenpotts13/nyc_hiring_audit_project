{{ config(
    pre_hook=[
        "SET s3_access_key_id='admin'",
        "SET s3_secret_access_key='password'",
        "SET s3_endpoint='host.docker.internal:9000'",
        "SET s3_url_style='path'",
        "SET s3_use_ssl=false"
    ]
) }}
SELECT * FROM read_parquet('s3://project4-bronze/raw/Citywide_Payroll_Data.parquet')