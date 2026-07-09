import os
import csv
import zipfile
import io
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

def safe_float(val):
    if not val or val.strip() == "":
        return 0.0
    try:
        return float(val.strip())
    except ValueError:
        return 0.0

def safe_int(val):
    if not val or val.strip() == "":
        return None
    try:
        return int(val.strip())
    except ValueError:
        return None

def safe_date(val):
    if not val or val.strip() == "":
        return None
    cleaned = val.strip().split(" ")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%y", "%m/%d/%y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

CALACCESS_EXPORT_URL = "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"

def download_calaccess_export():
    """
    Downloads the CAL-ACCESS raw data export directly from the California
    Secretary of State. No account or API key required — this is a public,
    unauthenticated URL that the state refreshes once a day:
    https://www.sos.ca.gov/campaign-lobbying/helpful-resources/raw-data-campaign-finance-and-lobbying-activity

    This replaces the previous Big Local News integration. BLN mirrors the
    same underlying data and is a legitimate source in its own right, but it
    requires an account with project-level access, which turned out to be a
    real ongoing failure point (an invalid token, then later a valid token
    on an account that wasn't a member of the project). Downloading straight
    from the state removes that entire dependency — there's no
    authentication step here that can expire, get revoked, or be scoped to
    the wrong project.
    """
    local_filename = "state_data_archive.zip"
    print("Step 1: Downloading CAL-ACCESS raw data directly from the CA Secretary of State...")
    with requests.get(CALACCESS_EXPORT_URL, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                f.write(chunk)
    print("✅ Transfer complete!")
    return local_filename

def process_file_data(the_zip, filename_keyword, row_parser_func, insert_sql, conn):
    """
    Loads one CalAccess extract into the database.

    IMPORTANT: this function used to insert with a single big execute_batch()
    call per 2000-row chunk. That meant a single bad row anywhere in a chunk
    (most commonly: a receipt/expenditure whose filer_id isn't present in
    calaccess_filers, which violates the foreign key constraint on that
    column) raised an exception that aborted the whole database transaction.
    Because that exception propagated straight out of run_daily_sync's
    already-committed TRUNCATE, every affected run ended with the tables
    permanently empty instead of reloaded.

    Fix: each chunk is now wrapped in its own SAVEPOINT. If the whole chunk
    inserts cleanly (the common case), it's fast, same as before. If the
    chunk fails, we roll back just that chunk and retry its rows one at a
    time inside their own savepoints, so only the actual bad/orphaned rows
    are skipped instead of discarding (or aborting the whole run over) 2000
    good ones.
    """
    target_filename = None
    for f in the_zip.namelist():
        if filename_keyword.lower() in f.lower():
            target_filename = f
            break

    if not target_filename:
        print(f"⚠️ Warning: File snippet matching '{filename_keyword}' was absent inside the archive.")
        return

    print(f"Processing and parsing record columns from internal sheet: {target_filename}...")

    inserted = 0
    skipped = 0

    with the_zip.open(target_filename) as raw_file:
        text_file = io.TextIOWrapper(raw_file, encoding='utf-8', errors='ignore')
        reader = csv.DictReader(text_file, delimiter='\t')

        batch = []
        batch_size = 2000

        with conn.cursor() as cur:

            def flush(rows):
                nonlocal inserted, skipped
                if not rows:
                    return
                cur.execute("SAVEPOINT chunk_sp;")
                try:
                    execute_batch(cur, insert_sql, rows)
                    cur.execute("RELEASE SAVEPOINT chunk_sp;")
                    inserted += len(rows)
                except Exception:
                    # Something in this chunk was bad. Roll back just the
                    # chunk (not the whole sync) and retry row by row so we
                    # only lose the actual offending rows.
                    cur.execute("ROLLBACK TO SAVEPOINT chunk_sp;")
                    cur.execute("RELEASE SAVEPOINT chunk_sp;")
                    for single_row in rows:
                        cur.execute("SAVEPOINT row_sp;")
                        try:
                            cur.execute(insert_sql, single_row)
                            cur.execute("RELEASE SAVEPOINT row_sp;")
                            inserted += 1
                        except Exception:
                            cur.execute("ROLLBACK TO SAVEPOINT row_sp;")
                            cur.execute("RELEASE SAVEPOINT row_sp;")
                            skipped += 1

            for row in reader:
                parsed_record = row_parser_func(row)
                if parsed_record:
                    batch.append(parsed_record)

                if len(batch) >= batch_size:
                    flush(batch)
                    batch = []

            flush(batch)

    print(f"  -> {target_filename}: {inserted} rows inserted, {skipped} rows skipped (bad or orphaned data).")

def parse_filer(row):
    first = row.get("FIRST_NAME", "").strip()
    last = row.get("LAST_NAME", "").strip()
    full_name = f"{first} {last}".strip() if first else last
    if not row.get("FILER_ID"):
        return None
    return (row.get("FILER_ID", "").strip(), row.get("FILER_TYPE", "").strip(), full_name, row.get("FILER_STATUS", "").strip(), first, last)

def parse_receipt(row):
    if not row.get("FILER_ID"):
        return None
    return (safe_int(row.get("FILING_ID")), row.get("FILER_ID", "").strip(), safe_float(row.get("AMOUNT")), safe_date(row.get("RCVD_DATE") or row.get("DATE_RCVD")), row.get("CTRIB_TYP", "").strip(), row.get("CTRIB_NAM_L", "").strip(), row.get("CTRIB_NAM_F", "").strip(), row.get("CTRIB_CITY", "").strip(), row.get("CTRIB_ST", "").strip(), row.get("CTRIB_ZIP4", "").strip(), row.get("CTRIB_EMP", "").strip(), row.get("CTRIB_OCC", "").strip(), safe_float(row.get("CUM_YTD")))

def parse_expenditure(row):
    if not row.get("FILER_ID"):
        return None
    return (safe_int(row.get("FILING_ID")), row.get("FILER_ID", "").strip(), safe_float(row.get("AMOUNT")), safe_date(row.get("EXPN_DATE")), row.get("PAYEE_NAM_L", "").strip(), row.get("PAYEE_NAM_F", "").strip(), row.get("PAYEE_CITY", "").strip(), row.get("PAYEE_ST", "").strip(), row.get("PAYEE_ZIP4", "").strip(), row.get("EXPN_CODE", "").strip(), row.get("EXPN_DSCR", "").strip(), row.get("CAND_NAM_L", "").strip(), row.get("BAL_NAME", "").strip(), row.get("SUP_OPP_CD", "").strip())

def run_daily_sync():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("❌ Core Abort: SUPABASE_DB_URL target path missing.")
        return

    local_zip_path = download_calaccess_export()

    if not os.path.exists(local_zip_path):
        print(f"❌ Abort: Local staging archive cache file '{local_zip_path}' could not be resolved.")
        return

    print("Step 2: Connecting directly to your cloud Supabase Warehouse cluster...")
    conn = psycopg2.connect(db_url)

    try:
        print("Step 3: Flushing legacy tables via TRUNCATE CASCADE...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE calaccess_receipts, calaccess_expenditures, calaccess_filers CASCADE;")
        # NOTE: no commit() here. The truncate above used to be committed
        # immediately and separately from the reload below. That meant any
        # failure during the reload left the tables permanently empty,
        # because rolling back only undid the not-yet-committed inserts, not
        # the already-committed truncate. Now the truncate is part of the
        # same transaction as the reload, so a genuinely fatal failure below
        # (e.g. a lost DB connection mid-load) rolls back the truncate too,
        # leaving the previous day's data intact instead of wiping it.

        print("Step 4: Extracting zip architecture streams and loading database chunks...")
        with zipfile.ZipFile(local_zip_path) as the_zip:

            # Load Filers first — receipts and expenditures both have a
            # foreign key on filer_id pointing at this table, so filers must
            # be loaded (and committed as part of this same transaction)
            # before any receipt/expenditure row referencing them will pass.
            process_file_data(
                the_zip,
                "filername_cd",
                parse_filer,
                "INSERT INTO calaccess_filers (filer_id, filer_type, filer_name, filer_status, first_name, last_name) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (filer_id) DO NOTHING;",
                conn
            )

            # Load Receipts
            process_file_data(
                the_zip,
                "rcpt_cd",
                parse_receipt,
                """INSERT INTO calaccess_receipts (filing_id, filer_id, amount, receipt_date, contributor_type,
                   contributor_last_name, contributor_first_name, contributor_city, contributor_state,
                   contributor_zip, contributor_employer, contributor_occupation, cumulative_ytd)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                conn
            )

            # Load Expenditures
            process_file_data(
                the_zip,
                "expn_cd",
                parse_expenditure,
                """INSERT INTO calaccess_expenditures (filing_id, filer_id, amount, expenditure_date, payee_last_name,
                   payee_first_name, payee_city, payee_state, payee_zip, expenditure_code, expenditure_description,
                   candidate_name, ballot_measure_name, support_oppose_code)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                conn
            )

        conn.commit()
        print("\n🎯 Success! The data pipeline completely refreshed your Swing Strategies Warehouse tables.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Data Extraction Pipeline Failure: {str(e)}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    run_daily_sync()
