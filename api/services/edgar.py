"""SEC EDGAR Form 4 filing fetcher and parser.

Uses the EDGAR submissions API (data.sec.gov) to discover Form 4 filings
and parses the ownership XML to extract insider transaction details.

Design philosophy: EDGAR API calls are expensive (rate-limited, unreliable).
The initial ingestion for a company fetches ALL historical Form 4 data and
stores it in Snowflake. Subsequent runs only fetch new filings (incremental).
All analysis operates on the database, never on live API calls.
"""

import hashlib
import logging
import time
from datetime import date, datetime
from typing import Optional
from xml.etree import ElementTree as ET

import requests

from api.config import settings

logger = logging.getLogger(__name__)

_last_request_time: float = 0.0

# Cache the ticker->CIK mapping to avoid repeated downloads
_ticker_cik_cache: dict[str, dict] = {}


def _rate_limited_get(url: str, max_retries: int = 3) -> requests.Response:
    """Make a GET request respecting SEC EDGAR rate limits with retry logic.

    EDGAR enforces 10 requests/second per User-Agent. We use 0.15s spacing
    (conservative) and retry on 403/429/5xx with exponential backoff.
    """
    global _last_request_time

    headers = {
        "User-Agent": settings.SEC_EDGAR_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }

    for attempt in range(max_retries):
        # Enforce rate limit
        elapsed = time.time() - _last_request_time
        wait = settings.SEC_EDGAR_RATE_LIMIT - elapsed
        if wait > 0:
            time.sleep(wait)

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            _last_request_time = time.time()

            if resp.status_code == 200:
                return resp

            if resp.status_code in (403, 429, 500, 502, 503, 504):
                backoff = (2 ** attempt) * 1.0  # 1s, 2s, 4s
                logger.warning(
                    f"EDGAR returned {resp.status_code} for {url}, "
                    f"retrying in {backoff}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(backoff)
                continue

            resp.raise_for_status()

        except requests.exceptions.Timeout:
            backoff = (2 ** attempt) * 1.0
            logger.warning(
                f"Timeout fetching {url}, retrying in {backoff}s "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(backoff)
            continue
        except requests.exceptions.ConnectionError:
            backoff = (2 ** attempt) * 2.0
            logger.warning(
                f"Connection error for {url}, retrying in {backoff}s "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(backoff)
            continue

    # Final attempt -- raise on failure
    resp = requests.get(url, headers=headers, timeout=30)
    _last_request_time = time.time()
    resp.raise_for_status()
    return resp


def resolve_ticker_to_cik(ticker: str) -> Optional[dict]:
    """Resolve a ticker symbol to CIK and company info via EDGAR.

    Results are cached in-memory to avoid redundant API calls.
    Returns dict with keys: cik, name, ticker, exchange, sic
    or None if not found.
    """
    ticker_upper = ticker.upper()
    if ticker_upper in _ticker_cik_cache:
        return _ticker_cik_cache[ticker_upper]

    cik_str = _search_cik_for_ticker(ticker_upper)
    if not cik_str:
        return None

    try:
        url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/CIK{cik_str}.json"
        resp = _rate_limited_get(url)
        data = resp.json()
        result = {
            "cik": data["cik"],
            "name": data.get("name", ""),
            "ticker": ticker_upper,
            "exchange": (data.get("exchanges") or [""])[0],
            "sic": data.get("sic", ""),
        }
        _ticker_cik_cache[ticker_upper] = result
        return result
    except Exception as e:
        logger.error(f"Failed to resolve CIK for {ticker_upper}: {e}")
        return None


def _search_cik_for_ticker(ticker: str) -> Optional[str]:
    """Look up CIK for a ticker using EDGAR company tickers JSON.

    This file is ~2MB and contains all public company tickers.
    We cache it for the process lifetime.
    """
    if not hasattr(_search_cik_for_ticker, "_data"):
        url = "https://www.sec.gov/files/company_tickers.json"
        resp = _rate_limited_get(url)
        _search_cik_for_ticker._data = resp.json()

    for entry in _search_cik_for_ticker._data.values():
        if entry.get("ticker", "").upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10)
    return None


def fetch_form4_filings(
    cik: str, after_date: Optional[date] = None
) -> list[dict]:
    """Fetch Form 4 filing metadata for a company CIK.

    On initial ingestion (after_date=None), fetches ALL historical filings
    including paginated results. On incremental runs, only returns filings
    newer than after_date.

    Returns list of dicts with keys: accession_number, filing_date, primary_doc
    """
    cik_padded = cik.zfill(10)
    url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/CIK{cik_padded}.json"
    resp = _rate_limited_get(url)
    data = resp.json()

    filings = []

    # Process the "recent" filings (most recent ~1000)
    recent = data.get("filings", {}).get("recent", {})
    filings.extend(_extract_form4_from_filing_set(recent, after_date))

    # Process paginated older filings if doing a full historical load
    # The submissions API stores older filings in separate JSON files
    older_files = data.get("filings", {}).get("files", [])
    if older_files and after_date is None:
        logger.info(
            f"Full historical load: {len(older_files)} pagination files to process"
        )
        for file_info in older_files:
            filename = file_info.get("name", "")
            if not filename:
                continue
            page_url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/{filename}"
            try:
                page_resp = _rate_limited_get(page_url)
                page_data = page_resp.json()
                filings.extend(
                    _extract_form4_from_filing_set(page_data, after_date)
                )
            except Exception as e:
                logger.warning(f"Failed to fetch pagination file {filename}: {e}")
                continue
    elif older_files and after_date is not None:
        # Incremental mode: check paginated files only if we haven't found
        # any filings in "recent" newer than after_date. This handles the
        # rare case where a company has so many filings that the cutoff date
        # falls outside the recent window.
        if not filings:
            for file_info in older_files:
                filename = file_info.get("name", "")
                if not filename:
                    continue
                page_url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/{filename}"
                try:
                    page_resp = _rate_limited_get(page_url)
                    page_data = page_resp.json()
                    page_filings = _extract_form4_from_filing_set(
                        page_data, after_date
                    )
                    filings.extend(page_filings)
                    # If this page had no results, older pages won't either
                    if not page_filings:
                        break
                except Exception as e:
                    logger.warning(f"Failed to fetch pagination file {filename}: {e}")
                    continue

    logger.info(
        f"Found {len(filings)} Form 4 filings for CIK {cik} "
        f"(after {after_date or 'all time'})"
    )
    return filings


def _extract_form4_from_filing_set(
    filing_set: dict, after_date: Optional[date]
) -> list[dict]:
    """Extract Form 4 entries from a submissions API filing set."""
    accessions = filing_set.get("accessionNumber", [])
    forms = filing_set.get("form", [])
    dates = filing_set.get("filingDate", [])
    primary_docs = filing_set.get("primaryDocument", [])

    results = []
    for i, form in enumerate(forms):
        if form not in ("4", "4/A"):
            continue

        filing_date = datetime.strptime(dates[i], "%Y-%m-%d").date()
        if after_date and filing_date <= after_date:
            continue

        results.append(
            {
                "accession_number": accessions[i],
                "filing_date": filing_date,
                "primary_doc": primary_docs[i] if i < len(primary_docs) else None,
            }
        )

    return results


def parse_form4_xml(
    cik: str, accession_number: str, filing_date: date, ticker: str,
    primary_doc: Optional[str] = None,
) -> list[dict]:
    """Fetch and parse a Form 4 XML filing into transaction records.

    Uses www.sec.gov for archive access (data.sec.gov doesn't serve all archives).
    The filer CIK is extracted from the accession number prefix.
    If primary_doc is provided, fetches the XML directly (1 API call).
    Otherwise falls back to fetching the index page first (2 API calls).
    """
    accession_clean = accession_number.replace("-", "")
    # The accession number prefix is the filer CIK (reporting owner, not the issuer)
    filer_cik = str(int(accession_number.split("-")[0]))
    archive_base = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{filer_cik}/{accession_clean}"
    )

    xml_text = None

    # Strategy 1: Use primary_doc if available (saves an API call)
    if primary_doc:
        # Strip XSL prefix like "xslF345X05/" -- the raw XML is the base filename
        xml_filename = primary_doc.split("/")[-1] if "/" in primary_doc else primary_doc
        xml_url = f"{archive_base}/{xml_filename}"
        try:
            resp = _rate_limited_get(xml_url)
            xml_text = resp.text
        except Exception as e:
            logger.warning(
                f"Failed to fetch primary doc for {accession_number}: {e}"
            )

    # Strategy 2: Fall back to index page to discover the XML filename
    if xml_text is None:
        index_url = f"{archive_base}/index.json"
        try:
            resp = _rate_limited_get(index_url)
            index_data = resp.json()
            xml_filename = _find_ownership_xml(index_data)
            if not xml_filename:
                logger.warning(
                    f"No ownership XML found in filing {accession_number}"
                )
                return []
            xml_url = f"{archive_base}/{xml_filename}"
            resp = _rate_limited_get(xml_url)
            xml_text = resp.text
        except Exception as e:
            logger.warning(
                f"Failed to fetch XML for {accession_number}: {e}"
            )
            return []

    return _parse_ownership_xml(xml_text, accession_number, filing_date, ticker)


def _find_ownership_xml(index_data: dict) -> Optional[str]:
    """Find the ownership XML filename from a filing index.

    Prioritizes files matching common Form 4 naming patterns.
    """
    items = index_data.get("directory", {}).get("item", [])
    candidates = []
    for item in items:
        name = item.get("name", "")
        if not name.endswith(".xml"):
            continue
        # Skip R-files (rendering metadata) and FilingSummary
        if name.startswith("R") or name == "FilingSummary.xml":
            continue
        # Prefer files with ownership-related names
        lower = name.lower()
        if any(kw in lower for kw in ("ownership", "form4", "doc4", "primary")):
            return name
        candidates.append(name)

    return candidates[0] if candidates else None


def _parse_ownership_xml(
    xml_text: str, accession_number: str, filing_date: date, ticker: str
) -> list[dict]:
    """Parse SEC ownership XML (Form 4) into transaction records."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        logger.warning(f"Failed to parse XML for {accession_number}")
        return []

    # Extract issuer info
    issuer = root.find(".//issuer")
    company_cik = _text(issuer, "issuerCik", "")

    # Handle multiple reporting owners (some filings have >1)
    transactions = []
    for owner_el in root.findall(".//reportingOwner"):
        owner_id = owner_el.find(".//reportingOwnerId")
        insider_cik = _text(owner_id, "rptOwnerCik", "")
        insider_name = _text(owner_id, "rptOwnerName", "Unknown")

        relationship = owner_el.find(".//reportingOwnerRelationship")
        insider_title = _extract_title(relationship)

        # Non-derivative transactions
        for txn in root.findall(".//nonDerivativeTransaction"):
            record = _parse_transaction_element(
                txn, accession_number, filing_date, company_cik, ticker,
                insider_cik, insider_name, insider_title, source="ND",
            )
            if record:
                transactions.append(record)

        # Derivative transactions
        for txn in root.findall(".//derivativeTransaction"):
            record = _parse_transaction_element(
                txn, accession_number, filing_date, company_cik, ticker,
                insider_cik, insider_name, insider_title, source="DV",
            )
            if record:
                transactions.append(record)

    return transactions


def _parse_transaction_element(
    txn, accession_number: str, filing_date: date,
    company_cik: str, ticker: str, insider_cik: str,
    insider_name: str, insider_title: str, source: str = "ND",
) -> Optional[dict]:
    """Parse a single transaction element from Form 4 XML."""
    coding = txn.find(".//transactionCoding")
    txn_code = _text(coding, "transactionCode", "")
    if not txn_code:
        return None

    amounts = txn.find(".//transactionAmounts")
    shares_str = _text(amounts, ".//transactionShares/value", "0")
    price_str = _text(amounts, ".//transactionPricePerShare/value", "")

    try:
        shares = float(shares_str)
    except (ValueError, TypeError):
        shares = 0.0

    try:
        price = float(price_str) if price_str else None
    except (ValueError, TypeError):
        price = None

    total_value = (shares * price) if price else None

    # Transaction date
    txn_date_str = _text(txn, ".//transactionDate/value", "")
    try:
        txn_date = datetime.strptime(txn_date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        txn_date = filing_date

    # Post-transaction holdings
    post_el = txn.find(".//postTransactionAmounts")
    owned_after_str = _text(
        post_el, ".//sharesOwnedFollowingTransaction/value", ""
    )
    try:
        owned_after = float(owned_after_str) if owned_after_str else None
    except (ValueError, TypeError):
        owned_after = None

    # Direct or indirect ownership
    ownership_el = txn.find(".//ownershipNature")
    direct_indirect = _text(ownership_el, "directOrIndirectOwnership/value", "D")

    # Generate deterministic transaction ID (includes insider_cik and source
    # to distinguish non-derivative vs derivative with identical fields)
    txn_id = _make_transaction_id(
        accession_number, insider_cik, txn_code, txn_date_str, shares_str, source
    )

    return {
        "transaction_id": txn_id,
        "accession_number": accession_number,
        "filing_date": filing_date,
        "company_cik": company_cik,
        "ticker": ticker,
        "insider_cik": insider_cik,
        "insider_name": insider_name,
        "insider_title": insider_title,
        "transaction_date": txn_date,
        "transaction_code": txn_code,
        "shares": shares,
        "price_per_share": price,
        "total_value": total_value,
        "shares_owned_after": owned_after,
        "direct_or_indirect": direct_indirect,
    }


def _extract_title(relationship) -> str:
    """Extract the insider's title/role from the relationship element."""
    if relationship is None:
        return "Unknown"
    titles = []
    if _text(relationship, "isDirector", "") == "1":
        titles.append("Director")
    if _text(relationship, "isOfficer", "") == "1":
        officer_title = _text(relationship, "officerTitle", "Officer")
        titles.append(officer_title)
    if _text(relationship, "isTenPercentOwner", "") == "1":
        titles.append("10% Owner")
    if _text(relationship, "isOther", "") == "1":
        other = _text(relationship, "otherText", "Other")
        titles.append(other)
    return ", ".join(titles) if titles else "Unknown"


def _text(parent, path: str, default: str = "") -> str:
    """Safely extract text from an XML element."""
    if parent is None:
        return default
    el = parent.find(path)
    if el is None or el.text is None:
        return default
    return el.text.strip()


def _make_transaction_id(
    accession: str, insider_cik: str, code: str, date_str: str,
    shares_str: str, source: str = "ND",
) -> str:
    """Create a deterministic hash ID for deduplication.

    Includes insider_cik for multi-owner filings and source (ND/DV)
    to distinguish non-derivative vs derivative transactions.
    """
    raw = f"{accession}|{insider_cik}|{source}|{code}|{date_str}|{shares_str}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
