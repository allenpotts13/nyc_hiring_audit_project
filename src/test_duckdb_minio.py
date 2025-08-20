import duckdb

# Set DuckDB S3/MinIO configuration
con = duckdb.connect(database='/workspaces/nyc_hiring_audit_project/duckdb/nyc_hiring.duckdb')
con.execute("SET s3_access_key_id='admin'")
con.execute("SET s3_secret_access_key='password'")
con.execute("SET s3_endpoint='host.docker.internal:9000'")
con.execute("SET s3_url_style='path'" )  # MinIO usually needs path style
con.execute("SET s3_use_ssl=false")

# Test reading the Parquet file from MinIO
try:
    result = con.execute("SELECT * FROM read_parquet('s3://project4-bronze/raw/Citywide_Payroll_Data.parquet') LIMIT 5").fetchdf()
    print(result)
except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()
