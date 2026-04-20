import os
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# =========================
# CONFIG (NEW)
# =========================

WINDOW_SIZE = timedelta(days=1)   # ✅ process 1 day per run
MAX_RUNTIME_SECONDS = 540         # ✅ 9 mins max (safe for 10 min cron)

CHUNK_SIZE = 2000                # ✅ reduced for speed
SUPABASE_BATCH = 200             # ✅ reduced for speed

start_execution = time.time()

# =========================
# CONNECTIONS
# =========================

try:
    conn_str = os.environ.get("PSQL_KEY")
    if not conn_str:
        raise ValueError("PSQL_KEY environment variable is missing")

    engine = create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=300
    )

    print("Successfully connected to PostgreSQL!")

except Exception as e:
    print(f"PostgreSQL connection error: {e}")
    exit()


try:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY missing")

    supabase = create_client(url, key)

    print("Successfully connected to Supabase!")

except Exception as e:
    print(f"Supabase connection error: {e}")
    exit()


# =========================
# FETCH MAX UPLOAD DATE
# =========================

def get_max_upload_date():
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
            print("No existing records found → full sync")
            return datetime(1970, 1, 1)

        raw_date = response.data[0]["upload_date"]

        if isinstance(raw_date, str):
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        else:
            dt = raw_date

        print(f"Max upload date: {dt}")
        return dt

    except Exception as e:
        print(f"Error fetching max upload date: {e}")
        return datetime(1970, 1, 1)


start_time = get_max_upload_date()

# ✅ prevent future overflow
now = datetime.utcnow()
end_time = min(start_time + WINDOW_SIZE, now)

print("=" * 80)
print(f"Processing window: {start_time} → {end_time}")
print("=" * 80)

# =========================
# SQL QUERY (UPDATED)
# =========================

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
    j."applyType" AS apply_type,
    j."rawText" AS raw_text,
    j."datePosted" AS date_posted,
    j."hoursBackPosted" AS hours_back_posted,
    j."yearsExpRequired" AS years_exp_required,
    j."uploadDate" AS upload_date,
    j."ingestedAt" AS ingested_at

FROM "karmafy_job" j
LEFT JOIN "karmafy_jobrole" jr
    ON j."roleId"::bigint = jr.id

WHERE j."uploadDate" > '{start_time}'
AND j."uploadDate" <= '{end_time}'

ORDER BY j."uploadDate" ASC
"""


# =========================
# HELPERS
# =========================

def clean_value(v):
    if pd.isna(v):
        return None
    return v


def prepare_record(row):
    return {
        "job_id": int(row["job_id"]) if pd.notna(row["job_id"]) else None,
        "job_role_id": int(row["job_role_id"]) if pd.notna(row["job_role_id"]) else None,
        "job_role_name": clean_value(row["job_role_name"]),
        "source": clean_value(row["source"]) or "",
        "title": clean_value(row["title"]) or "",
        "company": clean_value(row["company"]) or "",
        "location": clean_value(row["location"]) or "",
        "url": clean_value(row["url"]) or "",
        "description": clean_value(row["description"]) or "",
        "apply_type": clean_value(row["apply_type"]),
        "raw_text": clean_value(row["raw_text"]) or "",
        "date_posted": row["date_posted"].isoformat() if pd.notna(row["date_posted"]) else None,
        "hours_back_posted": int(row["hours_back_posted"]) if pd.notna(row["hours_back_posted"]) else 0,
        "years_exp_required": clean_value(row["years_exp_required"]),
        "upload_date": row["upload_date"].isoformat() if pd.notna(row["upload_date"]) else None,
        "ingested_at": row["ingested_at"].isoformat() if pd.notna(row["ingested_at"]) else None,
        "country": "United States of America"
    }


# =========================
# CHUNKED PROCESSING
# =========================

total_inserted = 0
total_errors = 0

print("=" * 80)
print("STARTING CHUNKED SYNC")
print("=" * 80)

try:
    for chunk_number, chunk_df in enumerate(
        pd.read_sql(text(sql_query), engine, chunksize=CHUNK_SIZE),
        start=1
    ):

        print(f"\nProcessing chunk {chunk_number} → {len(chunk_df)} rows")

        records = [prepare_record(row) for _, row in chunk_df.iterrows()]

        for i in range(0, len(records), SUPABASE_BATCH):

            # ✅ STOP if runtime exceeds limit
            if time.time() - start_execution > MAX_RUNTIME_SECONDS:
                print("⏱️ Max runtime reached, stopping early")
                raise Exception("Stopping early to avoid long cron execution")

            batch = records[i:i + SUPABASE_BATCH]

            try:
                response = (
                    supabase.table("job_jobrole_all")
                    .insert(batch)
                    .execute()
                )

                inserted = len(response.data) if response.data else 0
                total_inserted += inserted

                print(
                    f"✓ Chunk {chunk_number} | Batch {i//SUPABASE_BATCH + 1} | Inserted {inserted}"
                )

                time.sleep(0.1)  # ✅ prevent rate limits

            except Exception as batch_error:
                total_errors += len(batch)
                print(
                    f"✗ Chunk {chunk_number} | Batch {i//SUPABASE_BATCH + 1} | Error: {batch_error}"
                )

except Exception as e:
    print(f"Stopped safely: {e}")


# =========================
# FINAL REPORT
# =========================

print("\n" + "=" * 80)
print("SYNC COMPLETE")
print("=" * 80)
print(f"Inserted: {total_inserted}")
print(f"Errors: {total_errors}")
