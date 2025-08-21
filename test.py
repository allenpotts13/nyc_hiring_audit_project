import duckdb
con = duckdb.connect('/workspaces/nyc_hiring_audit_project/duckdb/nyc_hiring.duckdb')
print(con.execute("SELECT * FROM information_schema.tables").fetchdf())  # List all tables