"""
Swing Intel MCP Server
Swing Strategies — Internal Political Research Tooling

Exposes 9 research tools over SSE (Server-Sent Events) transport so that
Claude Desktop clients can connect to a single, centrally-hosted instance
via a URL rather than each user running a local server.

SECURITY NOTE: Every external API key is read from an environment variable
at call time via os.environ.get(). Nothing is hardcoded. If a key is
missing, the affected tool returns a clear error message rather than
failing silently or fabricating data.
"""

import os
import json
import base64
from datetime import datetime, timedelta
from typing import Optional

import requests
from fastmcp import FastMCP

mcp = FastMCP("swing-intel")

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 20  # seconds


def _missing_key_error(env_var_name: str, tool_name: str) -> str:
    return (
        f"ERROR: {tool_name} could not run because the environment variable "
        f"'{env_var_name}' is not set on this server. Add it in the Render "
        f"dashboard under Environment, then redeploy."
    )


def _safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None,
              timeout: int = DEFAULT_TIMEOUT):
    """Wrapper around requests.get with consistent error handling."""
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except requests.exceptions.HTTPError as e:
        return {
            "ok": False,
            "error": f"HTTP error {resp.status_code}: {str(e)}",
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
    try:
        resp = requests.post(url, json=json_body, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return {"ok": True, "status_code": resp.status_code, "data": resp.json()}
    except requests.exceptions.HTTPError as e:
        return {
            "ok": False,
            "error": f"HTTP error {resp.status_code}: {str(e)}",
            "raw_text": resp.text[:1000] if resp is not None else None,
        }
    except requests.exceptions.Timeout:
        return {"ok": False, "error": f"Request to {url} timed out after {timeout}s."}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": f"Request failed: {str(e)}"}
    except json.JSONDecodeError:
        return {"ok": False, "error": "Response was not valid JSON.",
                "raw_text": resp.text[:1000] if resp is not None else None}


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

    Returns a JSON string with bill status, sponsors, and history.
    Never substitute training data for this — LegiScan status changes daily.
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

    Returns a JSON string with bill status, sponsor, and (depending on
    endpoint) actions, cosponsors, or CRS summaries.
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

    Returns a JSON string with name, party, district, and committee
    assignments as available.
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

    Returns a JSON string. Always check 'coverage_end_date' in results —
    FEC filings lag actual contributions by weeks.
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
        result = _safe_get(f"{base_url}/candidates/{candidate_id}/totals/",
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
# TOOL 5 — CA State Campaign Finance (FollowTheMoney)
# ---------------------------------------------------------------------------

@mcp.tool()
def search_ca_state_finance(mode: str, filer_name: Optional[str] = None,
                             donor_name: Optional[str] = None,
                             year: Optional[int] = None) -> str:
    """
    Look up California state-level campaign finance via FollowTheMoney.

    Args:
        mode: One of "by_filer" (candidate/committee cash totals and IEs)
              or "by_donor" (contributions from a specific donor).
        filer_name: Candidate, committee, or ballot measure committee name —
                    required for mode="by_filer".
        donor_name: Donor name — required for mode="by_donor".
        year: Election year, e.g. 2026. Defaults to the current year if omitted.

    Returns a JSON string. Always supplement with CAL-ACCESS
    (https://cal-access.sos.ca.gov/) for the most current filings, since
    FollowTheMoney data can lag official state filings.

    NOTE: This tool queries the FollowTheMoney API. It does not query
    OpenSecrets, which covers federal — not California state — money and
    requires a separate key/registration.
    """
    api_key = os.environ.get("FOLLOWTHEMONEY_API_KEY")
    if not api_key:
        return _missing_key_error("FOLLOWTHEMONEY_API_KEY", "search_ca_state_finance")

    if not year:
        year = datetime.now().year

    base_url = "https://api.followthemoney.org/"
    params = {
        "APIKey": api_key,
        "dt": 1,
        "f-fc": "1,2,3",
        "mode": "json",
        "s": "CA",
        "y": year,
    }

    if mode == "by_filer":
        if not filer_name:
            return "ERROR: mode='by_filer' requires filer_name."
        params["f-filer"] = filer_name
    elif mode == "by_donor":
        if not donor_name:
            return "ERROR: mode='by_donor' requires donor_name."
        params["f-donor"] = donor_name
    else:
        return "ERROR: mode must be 'by_filer' or 'by_donor'."

    result = _safe_get(base_url, params=params)

    if not result["ok"]:
        return f"FollowTheMoney lookup failed: {result['error']}"

    return json.dumps(result["data"], indent=2)


# ---------------------------------------------------------------------------
# TOOL 6 — Local County/City Campaign Finance (Exa search over public portals)
# ---------------------------------------------------------------------------

@mcp.tool()
def search_local_county_finance(candidate_or_committee_name: str,
                                 jurisdiction: str = "sacramento") -> str:
    """
    Search for local city/county campaign finance disclosures (Form 460s and
    equivalent) via targeted web search over known public portals.

    IMPORTANT KNOWN LIMITATION: NetFile's API does not return usable JSON —
    it redirects to its front-end portal. This tool does NOT call a NetFile
    API. Instead it runs a scoped web search (via Exa) restricted to known
    public disclosure portal domains and returns links/snippets for a human
    or downstream Claude call to review. Treat results as leads to verify
    manually on the portal, not as structured, machine-parsed filing data.

    Args:
        candidate_or_committee_name: The candidate, committee, or measure name.
        jurisdiction: One of "sacramento", "orange_county", "irvine", "other".
                      Determines which portal domains are prioritized.

    Returns a JSON string of search results (title, url, snippet) plus the
    manual-lookup portal URLs for the chosen jurisdiction.
    """
    api_key = os.environ.get("EXA_API_KEY")

    portal_map = {
        "sacramento": [
            "site:pubdocs.saccounty.gov",
            "site:elections.saccounty.gov",
            "site:netfile.com/public/Sacramento",
        ],
        "orange_county": [
            "site:ocvote.gov",
            "site:netfile.com/public/OrangeCounty",
        ],
        "irvine": [
            "site:cityofirvine.org",
            "site:netfile.com/public/Irvine",
        ],
        "other": [
            "site:netfile.com/public",
        ],
    }

    manual_portals = {
        "sacramento": "https://www.saccounty.gov/elections/Pages/Campaign-Disclosure.aspx",
        "orange_county": "https://ocvote.gov/campaign/",
        "irvine": "https://cityofirvine.org/city-clerk/campaign-finance-disclosure",
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
        "note": ("These are search leads, not parsed filing data. NetFile's own "
                 "API is non-functional (confirmed: redirects to portal, no JSON). "
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

    Args:
        series_id: BLS series ID, e.g. "LAUCN060670000000003" for Sacramento
                   County unemployment rate, or "LASST060000000000003" for
                   statewide California, or "LNS14000000" for national.
        start_year: First year of data to pull. Defaults to two years ago.
        end_year: Last year of data to pull. Defaults to the current year.

    Returns a JSON string with the time series data. BLS LAUS county data
    typically runs 3-5 weeks behind — always check the 'latest' flag and
    footnote codes for preliminary ("P") data.
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

    Args:
        query: The topic, measure, or keyword to search for. Be specific —
               e.g. "Sacramento rent control ballot measure 2026" rather
               than just "rent control".
        days_back: How many days back to search. Defaults to 30.
        num_results: How many results to return. Defaults to 10, max 25.

    Returns a JSON string of results with title, url, published date, and
    a short excerpt for each. Always attribute claims to the specific
    outlet and date — do not present search excerpts as verified fact
    without noting the source.
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
    Return California FPPC compliance guidance for cross-referencing during
    a chat. This is a local, static reference — it does not call an
    external API and does not reflect same-day changes to FPPC regulations.

    Args:
        topic: Optional filter — one of "contribution_limits",
               "ballot_measures", "disclosure", "independent_expenditures",
               "ab2355", "expenditure_ceilings". If omitted, returns the
               full reference document.

    Always tell the user this is a preliminary check and that current
    figures must be verified at fppc.ca.gov before use in any filed or
    client-facing document.
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
        return (f"ERROR: topic must be one of {list(topic_map.keys())} or omitted "
                f"for the full reference.")

    header = topic_map[topic]
    sections = FPPC_GUIDELINES_MD.split("## ")
    for section in sections:
        if section.startswith(header.replace("## ", "")):
            return "## " + section

    return FPPC_GUIDELINES_MD  # fallback, should not normally hit


# ---------------------------------------------------------------------------
# Entrypoint — SSE transport over HTTP for remote/cloud hosting
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    mcp.run(transport="sse", host="0.0.0.0", port=port)
