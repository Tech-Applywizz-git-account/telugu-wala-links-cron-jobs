import os
from sqlalchemy import create_engine, text
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

# Connect to PostgreSQL
try:
    conn_str = os.environ.get("PSQL_KEY")
    if not conn_str:
        raise ValueError("PSQL_KEY environment variable is missing.")

    # Create SQLAlchemy engine
    engine = create_engine(conn_str)
    print("Successfully connected to PostgreSQL!")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# Connect to Supabase
try:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Supabase URL or KEY is missing.")

    supabase = create_client(url, key)
    print("Successfully connected to Supabase!")
except Exception as e:
    print(f"Error connecting to Supabase: {e}")
    exit()

# Exception handling for fetching max upload date
try:
    response = (
        supabase.table("job_jobrole_all")
        .select("upload_date")
        .not_.is_("upload_date", None)
        .order("upload_date", desc=True)
        .limit(1)
        .execute()
    )

    if not response.data:
        print("No existing data found. Starting fresh sync from beginning...")
        # Use a very old date to sync all jobs
        max_upload_date_str = '1970-01-01 00:00:00'
        end_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        max_upload_date = response.data[0]["upload_date"]
        print(f"Max upload date: {max_upload_date}")
        
        # Convert max_upload_date to datetime object
        if isinstance(max_upload_date, str):
            # Handle ISO format string (with or without timezone)
            if 'T' in max_upload_date:
                max_upload_date_dt = datetime.fromisoformat(max_upload_date.replace('Z', '+00:00'))
            else:
                max_upload_date_dt = datetime.fromisoformat(max_upload_date)
        elif hasattr(max_upload_date, 'isoformat'):
            max_upload_date_dt = max_upload_date
        else:
            max_upload_date_dt = datetime.fromisoformat(str(max_upload_date))
        
        # Calculate end date (max_upload_date + 1 day)
        end_date_dt = max_upload_date_dt + timedelta(days=1)
        
        # Format dates for SQL query
        max_upload_date_str = max_upload_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date_dt.strftime('%Y-%m-%d %H:%M:%S')
    
except Exception as e:
    print(f"Error fetching max upload date: {e}")
    # Start from scratch if error occurs
    max_upload_date_str = '1970-01-01 00:00:00'
    end_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Fetch jobs from the database
sql_query = f"""
SELECT 
    j.id AS job_id,
    jr.id AS job_role_id,
    jr.name AS job_role_name,
    j.source,
    j.title,
    j.company,
    j.location,
    j.url,
    j.description,
    j."rawText" AS raw_text,
    j."datePosted" AS date_posted,
    j."hoursBackPosted" AS hours_back_posted,
    j."yearsExpRequired" AS years_exp_required,
    j."uploadDate" AS upload_date,
    j."ingestedAt" AS ingested_at
FROM "karmafy_job" j
LEFT JOIN "karmafy_jobrole" jr
       ON j."roleId"::bigint = jr.id
WHERE j."uploadDate" > '{max_upload_date_str}'
  AND j."uploadDate" <= CURRENT_TIMESTAMP
ORDER BY j."uploadDate" DESC;
"""

try:
    print("Fetching data from the PostgreSQL database...")
    print(f"Date range: {max_upload_date_str} to {end_date_str}")
    print("-" * 80)
    
    # Use text() to properly handle the SQL query
    df = pd.read_sql(text(sql_query), engine)
    print(f"Fetched {len(df):,} rows from PostgreSQL database.\n")
except Exception as e:
    print(f"Error fetching data from PostgreSQL: {e}")
    import traceback
    traceback.print_exc()
    exit()

if len(df) == 0:
    print("No new jobs to sync. Exiting...")
    exit()

# Process jobs
print("=" * 80)
print("PROCESSING JOBS FOR SUPABASE SYNC")
print("=" * 80)
print()

# Prepare data for Supabase insertion
jobs_data = []

for idx, row in df.iterrows():
    # Prepare data for Supabase matching the schema
    job_data = {
        "job_id": int(row.get("job_id")) if pd.notna(row.get("job_id")) else None,
        "job_role_id": int(row.get("job_role_id")) if pd.notna(row.get("job_role_id")) else None,
        "job_role_name": row.get("job_role_name") if pd.notna(row.get("job_role_name")) else None,
        "source": row.get("source") if pd.notna(row.get("source")) else "",
        "title": row.get("title") if pd.notna(row.get("title")) else "",
        "company": row.get("company") if pd.notna(row.get("company")) else "",
        "location": row.get("location") if pd.notna(row.get("location")) else "",
        "url": row.get("url") if pd.notna(row.get("url")) else "",
        "description": row.get("description") if pd.notna(row.get("description")) else "",
        "raw_text": row.get("raw_text") if pd.notna(row.get("raw_text")) else "",
        "date_posted": row.get("date_posted").isoformat() if pd.notna(row.get("date_posted")) else None,
        "hours_back_posted": int(row.get("hours_back_posted")) if pd.notna(row.get("hours_back_posted")) else 0,
        "years_exp_required": row.get("years_exp_required") if pd.notna(row.get("years_exp_required")) else None,
        "upload_date": row.get("upload_date").isoformat() if pd.notna(row.get("upload_date")) else None,
        "ingested_at": row.get("ingested_at").isoformat() if pd.notna(row.get("ingested_at")) else None,
        "country": "United States of America"
    }
    
    jobs_data.append(job_data)
    
    # Print progress
    if (idx + 1) % 100 == 0 or idx + 1 == len(df):
        print(f"Prepared {idx + 1}/{len(df)} jobs for sync...")

print()
print("=" * 80)
print("DATA PREPARATION COMPLETE")
print("=" * 80)
print(f"Total jobs prepared: {len(jobs_data)}")

if len(jobs_data) > 0:
    print()
    print("=" * 80)
    print("INSERTING JOBS INTO SUPABASE")
    print("=" * 80)
    
    # Replace NaN/None values with None (Supabase-friendly)
    for record in jobs_data:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
    
    # Insert data in batches (Supabase has limits)
    table_name = "job_jobrole_all"
    batch_size = 1000
    total_inserted = 0
    total_errors = 0
    
    print(f"\nInserting {len(jobs_data)} jobs into Supabase...")
    print(f"Using batch size: {batch_size}")
    print("-" * 80)
    
    try:
        for i in range(0, len(jobs_data), batch_size):
            batch = jobs_data[i:i + batch_size]
            if batch:  # Ensure batch is not empty
                try:
                    response = supabase.table(table_name).insert(batch).execute()
                    batch_inserted = len(response.data) if response.data else 0
                    total_inserted += batch_inserted
                    print(f"✓ Batch {i // batch_size + 1}: Inserted {batch_inserted}/{len(batch)} rows")
                except Exception as batch_error:
                    print(f"✗ Batch {i // batch_size + 1}: Error - {batch_error}")
                    total_errors += len(batch)
        
        print()
        print(f"{'='*80}")
        print(f"SUPABASE SYNC COMPLETE")
        print(f"{'='*80}")
        print(f"Total rows inserted successfully: {total_inserted}")
        if total_errors > 0:
            print(f"Total rows with errors: {total_errors}")
        print(f"Success rate: {(total_inserted / len(jobs_data) * 100):.2f}%")
        
    except Exception as e:
        print(f"\nError inserting data to Supabase: {e}")
        print(f"Attempted to insert {len(jobs_data)} rows")
        print(f"Columns in data: {list(jobs_data[0].keys()) if jobs_data else 'N/A'}")
        import traceback
        traceback.print_exc()
else:
    print("\nNo jobs data prepared for insertion.")

print()
print("=" * 80)
print("SYNC SCRIPT COMPLETE")
print("=" * 80)
