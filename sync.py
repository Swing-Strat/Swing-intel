import os
import io
import zipfile
import csv
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# The official, direct link to California's daily raw database dumps
CAL_ACCESS_ZIP_URL = "https://campaignfinance.cdn.sos.ca.gov/rawdata/calaccess_raw_data.zip"

def safe_float(val):
    """Safely converts a string dollar value to a float number for database insertion."""
    if not val or val.strip() == "":
        return 0.0
    try:
        return float(val.strip())
    except ValueError:
        return 0.0

def safe_int(val):
    """Safely converts a string to an integer."""
    if not val or val.strip() == "":
        return None
    try:
        return int(val.strip())
    except ValueError:
        return None

def safe_date(val):
    """Normalizes various California date formatting strings into clean SQL YYYY-MM-DD."""
    if not val or val.strip() == "":
        return None
    cleaned = val.strip().split(" ")[0]  # strip off timestamps if present
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%y", "%m/%d/%y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def process_file_data(the_zip, filename_keyword, row_parser_func, insert_sql, conn):
    """Finds a file in the ZIP, reads it, transforms rows, and loads into database."""
    target_filename = None
    for f in the_zip.namelist():
        if filename_keyword.lower() in f.lower():
            target_filename = f
            break
            
    if not target_filename:
        print(f"⚠️ Warning: Could not locate a file matching '{filename_keyword}' in the zip package.")
        return

    print(f"Processing and cleaning entries from state file: {target_filename}...")
    
    with the_zip.open(target_filename) as raw_file:
        # Wrap file stream to safely skip corrupted accents or text formatting relics from legacy state records
        text_file = io.TextIOWrapper(raw_file, encoding='utf-8', errors='ignore')
        
        # State files are standard Tab-Delimited text formats
        reader = csv.DictReader(text_file, delimiter='\t')
        
        batch = []
        batch_size = 2000
        
        with conn.cursor() as cur:
            for row in reader:
                parsed_record = row_parser_func(row)
                if parsed_record:
                    batch.append(parsed_record)
                
                # Write to database in optimized chunks to protect network performance
                if len(batch) >= batch_size:
                    execute_batch(cur, insert_sql, batch)
                    batch = []
            
            # Flush any remaining rows
            if batch:
                execute_batch(cur, insert_sql, batch)

def parse_filer(row):
    # Combine first and last names if it's an individual candidate
    first = row.get("FIRST_NAME", "").strip()
    last = row.get("LAST_NAME", "").strip()
    full_name = f"{first} {last}".strip() if first else last
    
    if not row.get("FILER_ID"):
        return None
        
    return (
        row.get("FILER_ID", "").strip(),
        row.get("FILER_TYPE", "").strip(),
        full_name,
        row.get("FILER_STATUS", "").strip(),
        first,
        last
    )

def parse_receipt(row):
    if not row.get("FILER_ID"):
        return None
    return (
        safe_int(row.get("FILING_ID")),
        row.get("FILER_ID", "").strip(),
        safe_float(row.get("AMOUNT")),
        safe_date(row.get("RCVD_DATE") or row.get("DATE_RCVD")),
        row.get("CTRIB_TYP", "").strip(),
        row.get("CTRIB_NAM_L", "").strip(),
        row.get("CTRIB_NAM_F", "").strip(),
        row.get("CTRIB_CITY", "").strip(),
        row.get("CTRIB_ST", "").strip(),
        row.get("CTRIB_ZIP4", "").strip(),
        row.get("CTRIB_EMP", "").strip(),
        row.get("CTRIB_OCC", "").strip(),
        safe_float(row.get("CUM_YTD"))
    )

def parse_expenditure(row):
    if not row.get("FILER_ID"):
        return None
    return (
        safe_int(row.get("FILING_ID")),
        row.get("FILER_ID", "").strip(),
        safe_float(row.get("AMOUNT")),
        safe_date(row.get("EXPN_DATE")),
        row.get("PAYEE_NAM_L", "").strip(),
        row.get("PAYEE_NAM_F", "").strip(),
        row.get("PAYEE_CITY", "").strip(),
        row.get("PAYEE_ST", "").strip(),
        row.get("PAYEE_ZIP4", "").strip(),
        row.get("EXPN_CODE", "").strip(),
        row.get("EXPN_DSCR", "").strip(),
        row.get("CAND_NAM_L", "").strip(),
        row.get("BAL_NAME", "").strip(),
        row.get("SUP_OPP_CD", "").strip()
    )

def run_daily_sync():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("❌ Critical Abort: SUPABASE_DB_URL environment variable is missing.")
        return

    print("Step 1: Downloading compressed raw data matrix from CA Secretary of State...")
    response = requests.get(CAL_ACCESS_ZIP_URL, stream=True)
    response.raise_for_status()
    zip_buffer = io.BytesIO(response.content)
    
    print("Step 2: Connecting to your Supabase Data Warehouse cluster...")
    conn = psycopg2.connect(db_url)
    
    try:
        # Step 3: Clear yesterday's snapshots completely so we don't duplicate rows
        print("Step 3: Flushing out legacy data rows...")
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE calaccess_receipts, calaccess_expenditures, calaccess_filers CASCADE;")
        conn.commit()
        
        # Step 4: Parse files inside the zip architecture and process database saves
        print("Step 4: Executing ETL data parsing and structural streaming...")
        with zipfile.ZipFile(zip_buffer) as the_zip:
            
            # Load Filers
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
        print("🎯 Success! Your Swing Strategies Data Warehouse is completely populated and up to date.")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Pipeline Failure: {str(e)}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    run_daily_sync()
