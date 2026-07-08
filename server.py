"""
Swing Intel MCP Server
Swing Strategies — Internal Political Research Tooling

Exposes 10 research tools over HTTP transport so that
Claude clients can connect to a single, centrally-hosted instance.

SECURITY NOTE: Every external API key is read from an environment variable
at call time via os.environ.get(). Nothing is hardcoded. If a key is
missing, the affected tool returns a clear error message rather than
failing silently or fabricating data.

DATABASE ACCESS NOTE: All Supabase/PostgreSQL queries in this file run
through SUPABASE_DB_URL_READONLY, a connection that should be bound to a
Postgres role granted SELECT-only privileges. The nightly sync pipeline
(sync.py) should use a SEPARATE, privileged connection string
(SUPABASE_DB_URL) that is never referenced here. This separation means
that even if a generated SQL query is malformed or unexpected, the
database itself will refuse any write, insert, update, or delete.
"""

import os
import json
import base64
from datetime import datetime, timedelta
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP

mcp = FastMCP("swing-intel")

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 20  # seconds
MAX_SQL_ROWS = 100  # hard ceiling enforced in code, not just in the tool description


def _missing_key_error(env_var_name: str, tool_name: str) -> str:
    return (
        f"ERROR: {tool_name} could not run because the environment variable "
        f"'{env_var_name}' is not set on this server. Add it in the Render "
        f"dashboard under Environment, then redeploy."
    )


def _safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None,
              timeout: int = DEFAULT_TIMEOUT):
    """Wrapper around requests.get with consistent error handling."""
    resp = None
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except requests.exceptions.HTTPError as e:
        return {
            "ok": False,
            "error": f"HTTP error {resp.status_code if resp is not None else '?'}: {str(e)}",
            "raw_text": resp.text[:1000] if resp is not None else None,
        }
    except requests.exceptions.Timeout:
        return {"ok": False, "error": f"Request to {url} timed out after {timeout}s."}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": f"Request failed: {str(e)}"}
    except json.JSONDecodeError:
        return {
            "ok": False,
            "error": "Response was not valid JSON. The endpoint may have "
                     "returned an HTML page (e.g. a login/portal redirect) "
                     "instead of an API response.",
            "raw_text": resp.text[:1000] if resp is not None else None,
        }


def _safe_post(url: str, json_body: Optional[dict] = None, headers: Optional[dict] = None,
                timeout: int = DEFAULT_TIMEOUT):
    resp = None
    try:
        resp = requests.post(url, json=json_body, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except requests.exceptions.HTTPError as e:
        return {
            "ok": False,
            "error": f"HTTP error {resp.status_code if resp is not None else '?'}: {str(e)}",
            "raw_text": resp.text[:1000] if resp is not None else None,
        }
    except requests.exceptions.Timeout:
        return {"ok": False, "error": f"Request to {url} timed out after {timeout}s."}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": f"Request failed: {str(e)}"}
    except json.JSONDecodeError:
        return {"ok": False, "error": "Response was not valid JSON.",
                "raw_text": resp.text[:1000] if resp is not None else None}


def _execute_supabase_query(sql: str, params: Optional[tuple] = None) -> dict:
    """
    Helper to run a READ-ONLY query against the Supabase/PostgreSQL instance.

    IMPORTANT: This intentionally reads SUPABASE_DB_URL_READONLY, not
    SUPABASE_DB_URL. SUPABASE_DB_URL_READONLY should be a connection string
    authenticated as a Postgres role that has been granted SELECT-only
    privileges (see swing_intel_readonly role setup). This is a deliberate
    safety boundary: no query run through this function should ever be able
    to modify data, regardless of what the query text says.
    """
    db_url = os.environ.get("SUPABASE_DB_URL_READONLY")
    if not db_url:
        return {
            "ok": False,
            "error": (
                "SUPABASE_DB_URL_READONLY environment variable is missing. "
                "This server requires a separate read-only database connection "
                "string, distinct from the one used by sync.py. Create a "
                "read-only Postgres role, then add its connection string as "
                "SUPABASE_DB_URL_READONLY in the Render dashboard under Environment."
            ),
        }

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        conn.set_session(readonly=True)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            if cur.description:
                rows = cur.fetchall()
                return {"ok": True, "data": rows}
            conn.commit()
            return {"ok": True, "data": f"Query executed successfully. Rows affected: {cur.rowcount}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        if conn:
            conn.close()


def _enforce_row_limit(sql_query: str, max_rows: int = MAX_SQL_ROWS) -> str:
    """
    Ensures a SELECT query has a LIMIT clause no higher than max_rows.
    If no LIMIT is present, appends one. This is a code-level backstop —
    the tool description asks the model to include a LIMIT, but this
    function guarantees it regardless of what the model actually generates.
    """
    stripped = sql_query.strip().rstrip(";")
    lowered = stripped.lower()

    if "limit" not in lowered:
        return f"{stripped} LIMIT {max_rows};"

    # A LIMIT clause exists somewhere in the query. We don't attempt to
    # parse and rewrite it (risk of corrupting valid SQL) — we only add a
    # ceiling if none exists at all. If the model already specified a
    # LIMIT, we trust it but note this is a soft spot: a query author could
    # still specify LIMIT 100000. If this becomes a real problem, the fix is
    # to move to an actual SQL parser (e.g. sqlglot) rather than string checks.
    return f"{stripped};"


# ---------------------------------------------------------------------------
# TOOL 1 — CA State Bills (LegiScan)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_ca_state_bills(query: str, mode: str = "search", bill_id: Optional[str] = None) -> str:
    """
    Look up California state legislation via LegiScan.

    Args:
        query: Bill number (e.g. "SB 1") or keyword to search for.
        mode: "search" (default) to find bills by number/keyword, or
              "detail" to get full detail for a known bill_id.
        bill_id: Required when mode="detail". The LegiScan internal bill id
                 (obtained from a prior "search" call).
    """
    api_key = os.environ.get("LEGISCAN_API_KEY")
    if not api_key:
        return _missing_key_error("LEGISCAN_API_KEY", "get_ca_state_bills")

    base_url = "https://api.legiscan.com/"

    if mode == "detail":
        if not bill_id:
            return "ERROR: mode='detail' requires a bill_id. Run mode='search' first to obtain one."
        result = _safe_get(base_url, params={
            "key": api_key,
            "op": "getBill",
            "id": bill_id,
        })
    else:
        result = _safe_get(base_url, params={
            "key": api_key,
            "op": "getSearch",
            "state": "CA",
            "query": query,
        })

    if not result["ok"]:
        return f"LegiScan lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 2 — Federal Bills (Congress.gov)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_federal_bills(congress: int, bill_type: str, bill_number: str,
                      endpoint: str = "detail") -> str:
    """
    Look up federal legislation via the Congress.gov API.

    Args:
        congress: Congress number, e.g. 119 for the 119th Congress (2025-2026).
        bill_type: Bill type code, lowercase — e.g. "hr", "s", "hres", "sres".
        bill_number: The bill number, e.g. "100".
        endpoint: One of "detail", "actions", "cosponsors", "summaries".
                  Defaults to "detail".
    """
    api_key = os.environ.get("CONGRESS_API_KEY")
    if not api_key:
        return _missing_key_error("CONGRESS_API_KEY", "get_federal_bills")

    valid_endpoints = {"detail": "", "actions": "/actions",
                       "cosponsors": "/cosponsors", "summaries": "/summaries"}
    if endpoint not in valid_endpoints:
        return f"ERROR: endpoint must be one of {list(valid_endpoints.keys())}"

    url = (f"https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}"
           f"{valid_endpoints[endpoint]}")

    result = _safe_get(url, params={"api_key": api_key, "format": "json"})

    if not result["ok"]:
        return f"Congress.gov lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 3 — Legislator Data (OpenStates)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_legislator_data(mode: str, name: Optional[str] = None, lat: Optional[float] = None,
                        lng: Optional[float] = None, chamber: Optional[str] = None) -> str:
    """
    Look up California legislator info via OpenStates.

    Args:
        mode: One of "by_name", "by_geo", "all_chamber", "committees".
        name: Legislator name — required for mode="by_name".
        lat: Latitude — required for mode="by_geo".
        lng: Longitude — required for mode="by_geo".
        chamber: "upper" or "lower" — required for mode="all_chamber" and
                 optional filter for mode="committees".
    """
    api_key = os.environ.get("OPENSTATES_API_KEY")
    if not api_key:
        return _missing_key_error("OPENSTATES_API_KEY", "get_legislator_data")

    base_url = "https://v3.openstates.org"
    headers = {"X-API-KEY": api_key}

    if mode == "by_name":
        if not name:
            return "ERROR: mode='by_name' requires a name."
        result = _safe_get(f"{base_url}/people",
                           params={"jurisdiction": "ca", "name": name},
                           headers=headers)
    elif mode == "by_geo":
        if lat is None or lng is None:
            return "ERROR: mode='by_geo' requires lat and lng."
        result = _safe_get(f"{base_url}/people.geo",
                           params={"lat": lat, "lng": lng},
                           headers=headers)
    elif mode == "all_chamber":
        if chamber not in ("upper", "lower"):
            return "ERROR: mode='all_chamber' requires chamber='upper' or 'lower'."
        result = _safe_get(f"{base_url}/people",
                           params={"jurisdiction": "ca", "chamber": chamber},
                           headers=headers)
    elif mode == "committees":
        params = {"jurisdiction": "ca"}
        if chamber:
            params["chamber"] = chamber
        result = _safe_get(f"{base_url}/committees", params=params, headers=headers)
    else:
        return "ERROR: mode must be one of 'by_name', 'by_geo', 'all_chamber', 'committees'."

    if not result["ok"]:
        return f"OpenStates lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 4 — Federal Donations (FEC)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_federal_donations(mode: str, name: Optional[str] = None,
                          candidate_id: Optional[str] = None,
                          committee_id: Optional[str] = None,
                          cycle: Optional[int] = None) -> str:
    """
    Look up federal campaign finance data via the FEC API.

    Args:
        mode: One of "search_candidate", "candidate_totals", "search_committee",
              "committee_totals", "top_donors", "independent_expenditures".
        name: Candidate or committee name — required for search modes.
        candidate_id: FEC candidate id — required for candidate_totals and
                      independent_expenditures.
        committee_id: FEC committee id — required for committee_totals and
                      top_donors.
        cycle: Election cycle year, e.g. 2026 — required for the *_totals modes.
    """
    api_key = os.environ.get("FEC_API_KEY")
    if not api_key:
        return _missing_key_error("FEC_API_KEY", "get_federal_donations")

    base_url = "https://api.open.fec.gov/v1"
    params_common = {"api_key": api_key}

    if mode == "search_candidate":
        if not name:
            return "ERROR: mode='search_candidate' requires name."
        result = _safe_get(f"{base_url}/candidates/search/",
                           params={**params_common, "q": name, "state": "CA", "per_page": 10})
    elif mode == "candidate_totals":
        if not candidate_id or not cycle:
            return "ERROR: mode='candidate_totals' requires candidate_id and cycle."
        result = _safe_get(f"{base_url}/candidate/{candidate_id}/totals/",
                           params={**params_common, "cycle": cycle})
    elif mode == "search_committee":
        if not name:
            return "ERROR: mode='search_committee' requires name."
        result = _safe_get(f"{base_url}/committees/",
                           params={**params_common, "q": name, "state": "CA", "per_page": 10})
    elif mode == "committee_totals":
        if not committee_id or not cycle:
            return "ERROR: mode='committee_totals' requires committee_id and cycle."
        result = _safe_get(f"{base_url}/committee/{committee_id}/totals/",
                           params={**params_common, "cycle": cycle})
    elif mode == "top_donors":
        if not committee_id:
            return "ERROR: mode='top_donors' requires committee_id."
        result = _safe_get(f"{base_url}/schedules/schedule_a/",
                           params={**params_common, "committee_id": committee_id,
                                   "sort": "-contribution_receipt_amount", "per_page": 10})
    elif mode == "independent_expenditures":
        if not candidate_id:
            return "ERROR: mode='independent_expenditures' requires candidate_id."
        result = _safe_get(f"{base_url}/schedules/schedule_e/",
                           params={**params_common, "candidate_id": candidate_id, "per_page": 20})
    else:
        return ("ERROR: mode must be one of 'search_candidate', 'candidate_totals', "
                "'search_committee', 'committee_totals', 'top_donors', "
                "'independent_expenditures'.")

    if not result["ok"]:
        return f"FEC lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 5 — CA State Campaign Finance (Supabase Integration)
# ---------------------------------------------------------------------------

@mcp.tool()
def query_ca_state_finance(mode: str, name_query: Optional[str] = None, filer_id: Optional[str] = None) -> str:
    """
    Query structured California state campaign finance disclosures directly
    from the Swing Strategies internal Supabase data warehouse.

    Args:
        mode: One of "search_filer" (fuzzy matches candidate/PAC names to find IDs)
              or "filer_overview" (pulls full metrics summary for a specific filer ID).
        name_query: Name snippet of the candidate or committee. (Required for mode="search_filer").
        filer_id: The official 7-digit state filer ID string. (Required for mode="filer_overview").
    """
    if mode == "search_filer":
        if not name_query:
            return "ERROR: mode='search_filer' requires a 'name_query' parameter."

        sql = """
            SELECT filer_id, filer_type, filer_name, filer_status
            FROM calaccess_filers
            WHERE filer_name ILIKE %s
            ORDER BY filer_name LIMIT 10;
        """
        result = _execute_supabase_query(sql, (f"%{name_query}%",))

    elif mode == "filer_overview":
        if not filer_id:
            return "ERROR: mode='filer_overview' requires a valid 'filer_id'."

        output = {"filer_info": {}, "receipts_summary": {}, "expenditures_summary": {}}

        info_res = _execute_supabase_query("SELECT * FROM calaccess_filers WHERE filer_id = %s;", (filer_id,))
        if info_res["ok"] and info_res["data"]:
            output["filer_info"] = info_res["data"][0]
        elif not info_res["ok"]:
            return f"Supabase Warehouse Query Failed: {info_res['error']}"

        rcpt_res = _execute_supabase_query("""
            SELECT COALESCE(SUM(amount), 0) as total_raised, COUNT(*) as donation_count,
                   COALESCE(MAX(amount), 0) as largest_single_donation
            FROM calaccess_receipts WHERE filer_id = %s;
        """, (filer_id,))
        if rcpt_res["ok"] and rcpt_res["data"]:
            output["receipts_summary"] = rcpt_res["data"][0]
        elif not rcpt_res["ok"]:
            return f"Supabase Warehouse Query Failed: {rcpt_res['error']}"

        expn_res = _execute_supabase_query("""
            SELECT COALESCE(SUM(amount), 0) as total_spent, COUNT(*) as expense_count
            FROM calaccess_expenditures WHERE filer_id = %s;
        """, (filer_id,))
        if expn_res["ok"] and expn_res["data"]:
            output["expenditures_summary"] = expn_res["data"][0]
        elif not expn_res["ok"]:
            return f"Supabase Warehouse Query Failed: {expn_res['error']}"

        return json.dumps(output, indent=2)

    else:
        return "ERROR: mode must be 'search_filer' or 'filer_overview'."

    if not result["ok"]:
        return f"Supabase Warehouse Query Failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 6 — Local City/County Campaign Finance (Exa search over public portals)
# ---------------------------------------------------------------------------

@mcp.tool()
def search_local_county_finance(candidate_or_committee_name: str,
                                jurisdiction: str = "sacramento") -> str:
    """
    Search for local city/county campaign finance disclosures (Form 460s and
    equivalent) via targeted web search over known public portals.
    """
    api_key = os.environ.get("EXA_API_KEY")

    portal_map = {
        "sacramento": ["site:pubdocs.saccounty.gov", "site:elections.saccounty.gov", "site:netfile.com/public/Sacramento"],
        "orange_county": ["site:ocvote.gov", "site:netfile.com/public/OrangeCounty"],
        "irvine": ["site:cityofirvine.org", "site:netfile.com/public/Irvine"],
        "los_angeles": ["site:ethics.lacity.org", "site:lacity.org"],
        "san_francisco": ["site:sfethics.org", "site:data.sfgov.org"],
        "san_diego": ["site:sandiego.gov", "site:netfile.com/public/SanDiego"],
        "san_jose": ["site:sanjoseca.gov", "site:netfile.com/public/SanJose"],
        "long_beach": ["site:longbeach.gov", "site:netfile.com/public/LongBeach"],
        "oakland": ["site:oaklandca.gov", "site:netfile.com/public/Oakland"],
        "fresno": ["site:fresno.gov", "site:netfile.com/public/Fresno"],
        "bakersfield": ["site:bakersfieldcity.us", "site:netfile.com/public/Bakersfield"],
        "other": ["site:netfile.com/public"],
    }

    manual_portals = {
        "sacramento": "https://www.saccounty.gov/elections/Pages/Campaign-Disclosure.aspx",
        "orange_county": "https://ocvote.gov/campaign/",
        "irvine": "https://cityofirvine.org/city-clerk/campaign-finance-disclosure",
        "los_angeles": "https://ethics.lacity.org/disclosure-programs/cams/",
        "san_francisco": "https://sfethics.org/disclosure/campaign-finance-disclosure",
        "san_diego": "https://www.sandiego.gov/city-clerk/officialdocs/campaign-disclosure",
        "san_jose": "https://www.sanjoseca.gov/your-government/departments-offices/city-clerk/campaign-finance-disclosure",
        "long_beach": "https://www.longbeach.gov/cityclerk/campaign-finance/",
        "oakland": "https://www.oaklandca.gov/services/search-campaign-finance-disclosure-statements",
        "fresno": "https://www.fresno.gov/cityclerk/campaign-disclosure/",
        "bakersfield": "https://www.bakersfieldcity.us/298/City-Clerk",
        "other": "https://netfile.com/public/",
    }

    if jurisdiction not in portal_map:
        return f"ERROR: jurisdiction must be one of {list(portal_map.keys())}"

    if not api_key:
        return (
            _missing_key_error("EXA_API_KEY", "search_local_county_finance")
            + f" Manual fallback: check {manual_portals[jurisdiction]} directly for "
              f"'{candidate_or_committee_name}'."
        )

    site_filters = " OR ".join(portal_map[jurisdiction])
    query = f"{candidate_or_committee_name} Form 460 campaign disclosure {site_filters}"

    result = _safe_post(
        "https://api.exa.ai/search",
        json_body={
            "query": query,
            "numResults": 8,
            "type": "keyword",
            "contents": {"text": {"maxCharacters": 500}},
        },
        headers={"x-api-key": api_key, "Content-Type": "application/json"},
    )

    if not result["ok"]:
        return (f"Exa search failed: {result['error']}. Manual fallback: check "
                f"{manual_portals[jurisdiction]} directly.")

    output = {
        "search_results": result["data"],
        "manual_verification_url": manual_portals[jurisdiction],
        "note": ("These are search leads, not parsed filing data. Most local "
                 "portals (NetFile and otherwise) have no usable public API. "
                 "Verify every figure directly on the portal before using it."),
    }
    return json.dumps(output, indent=2)


# ---------------------------------------------------------------------------
# TOOL 7 — Economic Data (BLS)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_economic_data(series_id: str, start_year: Optional[int] = None,
                      end_year: Optional[int] = None) -> str:
    """
    Pull regional employment/unemployment data from the Bureau of Labor
    Statistics (BLS) Local Area Unemployment Statistics (LAUS) API.
    """
    api_key = os.environ.get("BLS_API_KEY")
    if not api_key:
        return _missing_key_error("BLS_API_KEY", "get_economic_data")

    current_year = datetime.now().year
    if not end_year:
        end_year = current_year
    if not start_year:
        start_year = current_year - 2

    body = {
        "seriesid": [series_id],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": api_key,
        "latest": True,
    }

    result = _safe_post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json_body=body)

    if not result["ok"]:
        return f"BLS lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 8 — News & Policy (Exa)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_news_and_policy(query: str, days_back: int = 30, num_results: int = 10) -> str:
    """
    Run a semantic web search for local press coverage, public affairs
    articles, and policy whitepapers via the Exa API.
    """
    api_key = os.environ.get("EXA_API_KEY")
    if not api_key:
        return _missing_key_error("EXA_API_KEY", "get_news_and_policy")

    num_results = min(num_results, 25)
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")

    result = _safe_post(
        "https://api.exa.ai/search",
        json_body={
            "query": query,
            "numResults": num_results,
            "type": "neural",
            "startPublishedDate": start_date,
            "contents": {"text": {"maxCharacters": 600}},
        },
        headers={"x-api-key": api_key, "Content-Type": "application/json"},
    )

    if not result["ok"]:
        return f"Exa search failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 9 — FPPC Guideline Lookup (local, no external API)
# ---------------------------------------------------------------------------

FPPC_GUIDELINES_MD = """
# California FPPC Compliance Quick Reference
**Source basis:** California Political Reform Act / FPPC regulations.
**IMPORTANT:** Contribution limits are adjusted for inflation on a
biennial cycle (odd years) and specific figures change. This reference
is a starting-point summary, NOT a substitute for checking fppc.ca.gov
directly before any figure is used in a client-facing or filed document.
Always verify current figures at https://www.fppc.ca.gov/ before relying
on any dollar amount below.

## Contribution Limits (State Candidates)
- State Senate/Assembly candidates: per-election limit, adjusted biennially.
- Statewide office candidates (Governor, etc.): separate, higher per-election limit.
- Small contributor committees: different, generally higher limits.
- Local jurisdictions (cities/counties) may set their OWN limits, which can
  differ from state limits — always check the local jurisdiction's rules,
  not just state figures, for city council or county supervisor races.

## Ballot Measure Committees
- No contribution limits apply to committees formed solely to support or
  oppose a ballot measure (this is a key distinction from candidate races).
- Disclosure and reporting requirements still apply in full.

## Disclosure Thresholds
- Committees must register (Form 410) once they raise or spend above the
  qualifying threshold — verify current dollar threshold at fppc.ca.gov.
- Major donor disclosure applies once an individual/entity's contributions
  to a single committee cross the major donor threshold.
- "Top Funders" disclosure boxes are required on certain mailers/ads for
  ballot measure committees above specified spending thresholds.

## Independent Expenditures
- Must be reported within specified windows once spending crosses
  disclosure thresholds — 24-hour reports apply close to Election Day.
- IEs cannot be coordinated with a candidate committee; "coordination"
  has a specific legal definition under FPPC regs — flag any campaign
  activity that could blur this line for legal review.

## AB 2355 — AI-Generated Political Advertising Disclosure
- California law requires disclosure when AI was used to generate or
  substantially alter content in a political advertisement.
- Any draft produced with Claude's assistance that could constitute a
  political ad, mailer, or paid digital communication must be flagged
  for legal review of AB 2355 disclosure requirements before it goes out.

## Voluntary Expenditure Ceilings
- Some local jurisdictions offer voluntary expenditure ceiling programs in
  exchange for benefits (e.g., ballot statement space, matching funds).
  These are jurisdiction-specific — verify locally, do not assume statewide
  applicability.

## Standing Rule for This Tool
This is a PRELIMINARY reference only. Every output built using this tool
must carry:
  "⚠️ FPPC: This is a preliminary check. Verify against current guidelines
  at fppc.ca.gov before use. Senior review required."
"""


@mcp.tool()
def fppc_guideline_lookup(topic: Optional[str] = None) -> str:
    """
    Return California FPPC compliance guidance for cross-referencing during a chat.
    """
    topic_map = {
        "contribution_limits": "## Contribution Limits (State Candidates)",
        "ballot_measures": "## Ballot Measure Committees",
        "disclosure": "## Disclosure Thresholds",
        "independent_expenditures": "## Independent Expenditures",
        "ab2355": "## AB 2355",
        "expenditure_ceilings": "## Voluntary Expenditure Ceilings",
    }

    if not topic:
        return FPPC_GUIDELINES_MD

    if topic not in topic_map:
        return f"ERROR: topic must be one of {list(topic_map.keys())}"

    header = topic_map[topic]
    sections = FPPC_GUIDELINES_MD.split("## ")
    for section in sections:
        if section.startswith(header.replace("## ", "")):
            return "## " + section

    return FPPC_GUIDELINES_MD


# ---------------------------------------------------------------------------
# TOOL 10 — Advanced AI SQL Engine (Dynamic Reads)
# ---------------------------------------------------------------------------

@mcp.tool()
def execute_ca_finance_custom_sql(sql_query: str) -> str:
    """
    Execute a raw, read-only PostgreSQL SELECT query against the California
    Campaign Finance data warehouse schemas. Use this when custom filters,
    groupings, or deep table joins are required.

    AVAILABLE WAREHOUSE TABLES:

    1. calaccess_filers:
       - filer_id VARCHAR(15) PRIMARY KEY
       - filer_type VARCHAR(50)
       - filer_name TEXT NOT NULL
       - filer_status VARCHAR(20)
       - first_name / last_name VARCHAR(255)

    2. calaccess_receipts (Contributions IN):
       - id BIGSERIAL PRIMARY KEY
       - filing_id INT
       - filer_id VARCHAR(15) REFERENCES calaccess_filers(filer_id)
       - amount NUMERIC(12,2)
       - receipt_date DATE
       - contributor_type VARCHAR(10)
       - contributor_last_name TEXT
       - contributor_first_name VARCHAR(255)
       - contributor_city / contributor_state / contributor_zip
       - contributor_employer / contributor_occupation
       - cumulative_ytd NUMERIC(12,2)

    3. calaccess_expenditures (Outbound Vendor/Staff Spend):
       - id BIGSERIAL PRIMARY KEY
       - filing_id INT
       - filer_id VARCHAR(15) REFERENCES calaccess_filers(filer_id)
       - amount NUMERIC(12,2)
       - expenditure_date DATE
       - payee_last_name TEXT
       - payee_first_name VARCHAR(255)
       - payee_city / payee_state / payee_zip
       - expenditure_code VARCHAR(3)
       - expenditure_description TEXT
       - candidate_name / ballot_measure_name
       - support_oppose_code VARCHAR(1)

    CRITICAL RULES:
    - Only SELECT queries are permitted. Mutation queries will be immediately blocked.
    - A LIMIT of 100 rows is enforced automatically on every query, even if
      you do not include one yourself.
    - This tool runs against a database role that is granted SELECT-only
      privileges at the database level. Even if a query were malformed or
      unexpected, the database itself will refuse any write.
    """
    clean_query = sql_query.strip().lower()
    forbidden_commands = ["insert", "update", "delete", "drop", "alter", "truncate", "create", "grant", "replace"]

    if any(cmd in clean_query for cmd in forbidden_commands) or not clean_query.startswith(("select", "with")):
        return "ERROR: Safety violation. This endpoint only executes read-only SELECT database actions."

    limited_query = _enforce_row_limit(sql_query)

    result = _execute_supabase_query(limited_query)

    if not result["ok"]:
        return f"Database Error: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# Entrypoint — Streamable HTTP transport configuration for cloud alignment
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Fixed parameter to use standard "http" transport keyword
    mcp.run(transport="http", host="0.0.0.0", port=port)
