import os
import csv
import zipfile
import io
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, date

# How far back to load receipts and expenditures. The state's export is a
# full historical dump going back decades; most of that is far more than a
# working research tool needs, and skipping it up front means much less data
# to parse, insert, and store. Filers are NOT filtered by this — every filer
# is kept regardless of age, since receipts/expenditures have a foreign key
# to calaccess_filers and a within-range transaction referencing an
# out-of-range filer would otherwise fail to insert. The filer registry is
# also comparatively small next to the transaction tables, so there isn't
# much to gain from filtering it too. Override with the SYNC_YEARS_BACK env
# var if you want a different window without editing code.
YEARS_BACK = int(os.environ.get("SYNC_YEARS_BACK", "5"))

def _cutoff_date():
    today = date.today()
    try:
        return today.replace(year=today.year - YEARS_BACK)
    except ValueError:
        # today is Feb 29 and (today.year - YEARS_BACK) isn't a leap year
        return today.replace(month=2, day=28, year=today.year - YEARS_BACK)

CUTOFF_DATE = _cutoff_date()

def _is_recent(iso_date_str):
    """True if iso_date_str (as returned by safe_date, 'YYYY-MM-DD' or None)
    falls on or after CUTOFF_DATE and not after today. Rows with a
    missing/unparseable date are treated as NOT recent and get skipped —
    we can't confirm they're in range, so the safer default is to leave
    them out rather than guess.

    The upper bound matters: this is 25 years of manually-entered filings,
    and a handful of rows have a data-entry-error date far in the future
    (e.g. a typo'd 4-digit year like 2119). Without a ceiling those pass
    the >= CUTOFF_DATE check and get miscounted as recent."""
    if not iso_date_str:
        return False
    try:
        parsed = datetime.strptime(iso_date_str, "%Y-%m-%d").date()
        return CUTOFF_DATE <= parsed <= date.today()
    except ValueError:
        return False

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

def _clean_tsv_lines(text_file):
    """Some CalAccess TSVs (CVR_CAMPAIGN_DISCLOSURE_CD.TSV in particular)
    contain embedded NUL bytes in free-text fields, which crashes Python's
    csv module ('line contains NUL') before a single row is read. Strip
    them at the line level so csv.DictReader never sees one."""
    for line in text_file:
        yield line.replace("\0", "")

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
        reader = csv.DictReader(_clean_tsv_lines(text_file), delimiter='\t')

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
    # The real FILERNAME_CD.TSV columns are NAML/NAMF/STATUS, not
    # FIRST_NAME/LAST_NAME/FILER_STATUS (those never existed in this file —
    # every previously-loaded filer had a blank filer_name, which made
    # search_filer's `ILIKE` lookup match nothing). Confirmed against the
    # real export's header, not assumed.
    first = row.get("NAMF", "").strip()
    last = row.get("NAML", "").strip()
    full_name = f"{first} {last}".strip() if first else last
    if not row.get("FILER_ID"):
        return None
    return (row.get("FILER_ID", "").strip(), row.get("FILER_TYPE", "").strip(), full_name, row.get("STATUS", "").strip(), first, last)

def load_filing_to_filer_map(the_zip):
    """
    RCPT_CD.TSV and EXPN_CD.TSV carry no FILER_ID column at all (only
    FILING_ID, plus a CMTE_ID that's populated on <10% of rows and means
    something different — an intermediary committee, not the primary
    filer). Every receipt/expenditure row used to fail the `if not
    row.get("FILER_ID")` check and get silently dropped, which is why the
    tables were always empty regardless of how the sync ran.

    The real FILING_ID -> FILER_ID mapping lives on each filing's cover
    sheet, CVR_CAMPAIGN_DISCLOSURE_CD.TSV (one row per filing/amendment,
    with a clean 1:1 FILING_ID -> FILER_ID pair). Load it once into memory
    so receipt/expenditure rows can resolve their actual filer.
    """
    target_filename = None
    for f in the_zip.namelist():
        name_lower = f.lower()
        if "cvr_campaign_disclosure_cd" in name_lower:
            target_filename = f
            break

    if not target_filename:
        print("⚠️ Warning: CVR_CAMPAIGN_DISCLOSURE_CD.TSV was absent inside the archive — receipts/expenditures cannot be linked to a filer and will all be skipped.")
        return {}

    print(f"Loading FILING_ID -> FILER_ID map from {target_filename}...")
    mapping = {}
    with the_zip.open(target_filename) as raw_file:
        text_file = io.TextIOWrapper(raw_file, encoding='utf-8', errors='ignore')
        reader = csv.DictReader(_clean_tsv_lines(text_file), delimiter='\t')
        for row in reader:
            filing_id = row.get("FILING_ID", "").strip()
            filer_id = row.get("FILER_ID", "").strip()
            if filing_id and filer_id:
                mapping[filing_id] = filer_id
    print(f"  -> Loaded {len(mapping)} filing->filer mappings.")
    return mapping

def parse_receipt(row, filing_to_filer):
    filer_id = filing_to_filer.get(row.get("FILING_ID", "").strip())
    if not filer_id:
        return None
    # Real column is RCPT_DATE — RCVD_DATE/DATE_RCVD never existed in this
    # file. CTRIB_TYP doesn't exist either; ENTITY_CD (IND/COM/OTH/PTY/SCC)
    # is the real column carrying the contributor's entity type. Contributor
    # name columns are CTRIB_NAML/CTRIB_NAMF (no underscore before L/F).
    receipt_date = safe_date(row.get("RCPT_DATE"))
    if not _is_recent(receipt_date):
        return None
    return (safe_int(row.get("FILING_ID")), filer_id, safe_float(row.get("AMOUNT")), receipt_date, row.get("ENTITY_CD", "").strip(), row.get("CTRIB_NAML", "").strip(), row.get("CTRIB_NAMF", "").strip(), row.get("CTRIB_CITY", "").strip(), row.get("CTRIB_ST", "").strip(), row.get("CTRIB_ZIP4", "").strip(), row.get("CTRIB_EMP", "").strip(), row.get("CTRIB_OCC", "").strip(), safe_float(row.get("CUM_YTD")))

def parse_expenditure(row, filing_to_filer):
    filer_id = filing_to_filer.get(row.get("FILING_ID", "").strip())
    if not filer_id:
        return None
    # Payee/candidate name columns are PAYEE_NAML/PAYEE_NAMF/CAND_NAML (no
    # underscore before L/F) — same naming-convention mismatch as receipts.
    expenditure_date = safe_date(row.get("EXPN_DATE"))
    if not _is_recent(expenditure_date):
        return None
    return (safe_int(row.get("FILING_ID")), filer_id, safe_float(row.get("AMOUNT")), expenditure_date, row.get("PAYEE_NAML", "").strip(), row.get("PAYEE_NAMF", "").strip(), row.get("PAYEE_CITY", "").strip(), row.get("PAYEE_ST", "").strip(), row.get("PAYEE_ZIP4", "").strip(), row.get("EXPN_CODE", "").strip(), row.get("EXPN_DSCR", "").strip(), row.get("CAND_NAML", "").strip(), row.get("BAL_NAME", "").strip(), row.get("SUP_OPP_CD", "").strip())

def run_daily_sync():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("❌ Core Abort: SUPABASE_DB_URL target path missing.")
        return

    print(f"Loading receipts and expenditures dated {CUTOFF_DATE.isoformat()} or later ({YEARS_BACK} year(s) back). All filers are loaded regardless of date.")

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

            # Receipts/expenditures don't carry FILER_ID directly — resolve
            # it via each row's FILING_ID against the cover-sheet map loaded
            # here (see load_filing_to_filer_map's docstring).
            filing_to_filer = load_filing_to_filer_map(the_zip)

            # Load Receipts
            process_file_data(
                the_zip,
                "rcpt_cd",
                lambda row: parse_receipt(row, filing_to_filer),
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
                lambda row: parse_expenditure(row, filing_to_filer),
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
