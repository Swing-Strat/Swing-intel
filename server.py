import os
import json
import base64
from datetime import datetime, timedelta
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse

# fastmcp 3.x: the constructor no longer accepts `host` or `transport_security`.
# Host binding goes to run() / the CLI; host-header policy goes to run() kwargs
# or FASTMCP_HTTP_* env vars.
mcp = FastMCP("swing-intel")

DEFAULT_TIMEOUT = 20  # seconds
MAX_SQL_ROWS = 100

def _missing_key_error(env_var_name: str, tool_name: str) -> str:
    return f"ERROR: {tool_name} requires missing '{env_var_name}'."

def _safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = DEFAULT_TIMEOUT):
    resp = None
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _safe_post(url: str, json_body: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = DEFAULT_TIMEOUT):
    resp = None
    try:
        resp = requests.post(url, json=json_body, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _execute_supabase_query(sql: str, params: Optional[tuple] = None) -> dict:
    db_url = os.environ.get("SUPABASE_DB_URL_READONLY")
    if not db_url:
        return {"ok": False, "error": "SUPABASE_DB_URL_READONLY environment variable is missing."}
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
            return {"ok": True, "data": f"Rows affected: {cur.rowcount}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        if conn: conn.close()

def _enforce_row_limit(sql_query: str, max_rows: int = MAX_SQL_ROWS) -> str:
    stripped = sql_query.strip().rstrip(";")
    if "limit" not in stripped.lower():
        return f"{stripped} LIMIT {max_rows};"
    return f"{stripped};"

@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> PlainTextResponse:
    """Plain HTTP endpoint for Render health checks and keep-warm pings."""
    return PlainTextResponse("ok")

@mcp.tool()
def get_ca_state_bills(query: str, mode: str = "search", bill_id: Optional[str] = None) -> str:
    """Look up California state legislation via LegiScan."""
    api_key = os.environ.get("LEGISCAN_API_KEY")
    if not api_key: return _missing_key_error("LEGISCAN_API_KEY", "get_ca_state_bills")
    base_url = "https://api.legiscan.com/"
    if mode == "detail":
        result = _safe_get(base_url, params={"key": api_key, "op": "getBill", "id": bill_id})
    else:
        result = _safe_get(base_url, params={"key": api_key, "op": "getSearch", "state": "CA", "query": query})
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def get_federal_bills(congress: int, bill_type: str, bill_number: str, endpoint: str = "detail") -> str:
    """Look up federal legislation via the Congress.gov API."""
    api_key = os.environ.get("CONGRESS_API_KEY")
    if not api_key: return _missing_key_error("CONGRESS_API_KEY", "get_federal_bills")
    valid_endpoints = {"detail": "", "actions": "/actions", "cosponsors": "/cosponsors", "summaries": "/summaries"}
    url = f"https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}{valid_endpoints.get(endpoint, '')}"
    result = _safe_get(url, params={"api_key": api_key, "format": "json"})
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def get_legislator_data(mode: str, name: Optional[str] = None, lat: Optional[float] = None, lng: Optional[float] = None, chamber: Optional[str] = None) -> str:
    """Look up California legislator info via OpenStates."""
    api_key = os.environ.get("OPENSTATES_API_KEY")
    if not api_key: return _missing_key_error("OPENSTATES_API_KEY", "get_legislator_data")
    base_url = "https://v3.openstates.org"
    headers = {"X-API-KEY": api_key}
    if mode == "by_name":
        result = _safe_get(f"{base_url}/people", params={"jurisdiction": "ca", "name": name}, headers=headers)
    elif mode == "by_geo":
        result = _safe_get(f"{base_url}/people.geo", params={"lat": lat, "lng": lng}, headers=headers)
    else:
        result = _safe_get(f"{base_url}/committees", params={"jurisdiction": "ca"}, headers=headers)
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def get_federal_donations(mode: str, name: Optional[str] = None, candidate_id: Optional[str] = None, committee_id: Optional[str] = None, cycle: Optional[int] = None) -> str:
    """Look up federal campaign finance data via the FEC API."""
    api_key = os.environ.get("FEC_API_KEY")
    if not api_key: return _missing_key_error("FEC_API_KEY", "get_federal_donations")
    base_url = "https://api.open.fec.gov/v1"
    params = {"api_key": api_key}
    if "search" in mode:
        result = _safe_get(f"{base_url}/candidates/search/", params={**params, "q": name, "state": "CA"})
    else:
        result = _safe_get(f"{base_url}/candidate/{candidate_id}/totals/", params={**params, "cycle": cycle})
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def query_ca_state_finance(mode: str, name_query: Optional[str] = None, filer_id: Optional[str] = None) -> str:
    """Query pre-aggregated California state campaign finance out of Supabase."""
    if mode == "search_filer":
        sql = "SELECT filer_id, filer_type, filer_name FROM calaccess_filers WHERE filer_name ILIKE %s LIMIT 10;"
        result = _execute_supabase_query(sql, (f"%{name_query}%",))
    else:
        output = {"filer_info": {}, "receipts_summary": {}}
        res = _execute_supabase_query("SELECT * FROM calaccess_filers WHERE filer_id = %s;", (filer_id,))
        if res["ok"] and res["data"]: output["filer_info"] = res["data"][0]
        result = {"ok": True, "data": output}
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def search_local_county_finance(candidate_or_committee_name: str, jurisdiction: str = "sacramento") -> str:
    """Search for local city/county campaign finance portals using Exa Keyword Routing."""
    api_key = os.environ.get("EXA_API_KEY")
    if not api_key: return _missing_key_error("EXA_API_KEY", "search_local_county_finance")
    query = f"{candidate_or_committee_name} Form 460 campaign disclosure site:netfile.com"
    result = _safe_post("https://api.exa.ai/search", json_body={"query": query, "numResults": 5, "type": "keyword"}, headers={"x-api-key": api_key, "Content-Type": "application/json"})
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def get_economic_data(series_id: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> str:
    """Pull inflation and labor metrics from the Bureau of Labor Statistics (BLS) API."""
    api_key = os.environ.get("BLS_API_KEY")
    if not api_key: return _missing_key_error("BLS_API_KEY", "get_economic_data")
    body = {"seriesid": [series_id], "startyear": "2024", "endyear": "2026", "registrationkey": api_key}
    result = _safe_post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json_body=body)
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def get_news_and_policy(query: str, days_back: int = 30) -> str:
    """Run a neural web search for localized public policy coverage via Exa."""
    api_key = os.environ.get("EXA_API_KEY")
    if not api_key: return _missing_key_error("EXA_API_KEY", "get_news_and_policy")
    result = _safe_post("https://api.exa.ai/search", json_body={"query": query, "numResults": 5, "type": "neural"}, headers={"x-api-key": api_key})
    return json.dumps(result.get("data", result), indent=2)

@mcp.tool()
def fppc_guideline_lookup(topic: Optional[str] = None) -> str:
    """Return static local California FPPC compliance guidance parameters."""
    return "California Political Reform Act compliance reference data map."

@mcp.tool()
def execute_ca_finance_custom_sql(sql_query: str) -> str:
    """Execute raw read-only PostgreSQL SELECT queries against schemas."""
    clean_query = sql_query.strip().lower()
    if not clean_query.startswith(("select", "with")):
        return "ERROR: Only read-only SELECT actions allowed."
    limited_query = _enforce_row_limit(sql_query)
    result = _execute_supabase_query(limited_query)
    return json.dumps(result.get("data", result), indent=2)

if __name__ == "__main__":
    # Render injects PORT; bind 0.0.0.0 so the edge proxy can reach us.
    port = int(os.environ.get("PORT", 8000))
    mcp.run(
        transport="http",              # Streamable HTTP -> endpoint served at /mcp
        host="0.0.0.0",
        port=port,
        stateless_http=True,           # each request self-contained; survives free-tier restarts
        host_origin_protection=False,  # Render's proxy rewrites Host; localhost DNS-rebinding
                                       # protection does not apply to a public deployment.
                                       # To re-enable with least privilege instead, replace with:
                                       # allowed_hosts=["mcp-1st-try.onrender.com", "mcp-1st-try.onrender.com:*"]
    )
