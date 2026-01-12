#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Update/merge conference deadlines into a single JSON file consumed by the frontend.

Sources:
  1) CSA Lab conf-track: https://csalab.site/conf-track/
  2) EasyChair CFP list: https://easychair.org/cfp/
  3) EDAS (optional watchlist): config/edas_watchlist.yaml
  4) ccfddl/ccf-deadlines (optional): GitHub YAMLs

Key requirements implemented:
  (1) For categories like "CCF AI (Artificial Intelligence)", keep only "Artificial Intelligence"
  (2) Rename "Wireless & Communication" to "Wireless/Communication"
  (3) Merge "Networking & Systems" and "Network System" into a single "Network System"
  (4) Only add, do not delete. If already exists in JSON, merge/update fields.

Output schema (compatible with your frontend):
[
  {
    "name": "...",
    "sub": ["..."],                  # list of subjects
    "Location": "...",
    "Start Date": "Jan 12 2026",
    "End Date": "Jan 15 2026",
    "Abstract Deadline": "...",
    "Submission Deadline": "...",
    "Notification": "...",
    "link": "https://..."
  }
]

Notes:
- EDAS does not provide a stable public "all CFPs" endpoint. This script supports a watchlist file.
- ccf-deadlines fetching uses GitHub API. Set GITHUB_TOKEN env var if you hit rate limits.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    import yaml  # PyYAML
except Exception as e:
    raise RuntimeError("Missing dependency: pyyaml. Install with: pip install pyyaml") from e


# -----------------------------
# Global constants
# -----------------------------
CSA_CONFTRACK_URL = "https://csalab.site/conf-track/"
EASYCHAIR_CFP_URL = "https://easychair.org/cfp/"

# GitHub API for ccfddl/ccf-deadlines
CCFDDL_OWNER_REPO = "ccfddl/ccf-deadlines"
GITHUB_API_BASE = "https://api.github.com"
CCFDDL_CONTENTS = f"{GITHUB_API_BASE}/repos/{CCFDDL_OWNER_REPO}/contents"

# If you don't want ccf-deadlines, you can set --disable-ccfddl
DEFAULT_CCF_CATEGORIES = ["DS", "NW", "SC", "SE", "DB", "CT", "CG", "AI", "HI", "MX"]

# ccf-deadlines sub category labels (used AFTER stripping "CCF .. (...)" formatting)
CCF_SUB_LABEL = {
    "DS": "Computer Architecture/Parallel Programming/Storage Technology",
    "NW": "Network System",
    "SC": "Network and System Security",
    "SE": "Software Engineering/Operating System/Programming Language Design",
    "DB": "Database/Data Mining/Information Retrieval",
    "CT": "Computing Theory",
    "CG": "Graphics",
    "AI": "Artificial Intelligence",
    "HI": "Computer–Human Interaction",
    "MX": "Interdiscipline/Mixture/Emerging",
}


# -----------------------------
# Utilities
# -----------------------------
def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", _safe_str(s)).strip()


def _norm_key_exact(name: str) -> str:
    """
    A fairly strict normalized key (keeps years/cycles) for matching.
    """
    s = _safe_str(name).lower()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", s)  # dashes
    s = re.sub(r"[^a-z0-9\- ]+", "", s)
    return s.strip()


_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_CYCLE_RE = re.compile(r"(?i)(?:[-–—]?\s*)?cycle\s*(?:\d+|spring|fall)\b")


def _norm_key_series(name: str) -> str:
    """
    A looser normalized key (removes year and cycle) for fallback matching.
    """
    s = _safe_str(name)
    s = _YEAR_RE.sub("", s)
    s = _CYCLE_RE.sub("", s)
    s = _safe_str(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", s)
    s = re.sub(r"[^a-z0-9\- ]+", "", s)
    return s.strip()


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _dedupe_list(xs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in xs:
        x = _safe_str(x)
        if not x:
            continue
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _clean_deadline_human(val: str) -> str:
    """
    Try to convert common date strings into "Mon DD YYYY".
    If cannot parse, return cleaned raw.
    """
    if not isinstance(val, str) or not val.strip():
        return ""
    s = val

    # remove HTML-like strike markers, parentheses annotations, and extra phrases
    s = re.sub(r"</?strike>", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(.*?\)", "", s)  # (AOE), (UTC), (EDT), etc.
    s = re.sub(r"in\s+\d+\s+days", "", s, flags=re.IGNORECASE)
    s = re.sub(r";?\s*\d{1,2}:\d{2}\s*(AM|PM)", "", s, flags=re.IGNORECASE)
    s = re.sub(r";", "", s)
    s = _norm_space(s)

    # try parse
    for fmt in ("%b %d %Y", "%B %d %Y", "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%b %d %Y")
        except ValueError:
            continue
    return s


def _tz_to_offset(tz_str: str) -> timezone:
    """
    ccf-deadlines timezone supports:
      - AoE -> UTC-12
      - UTC, UTC+/-X
    """
    s = _safe_str(tz_str)
    if not s:
        return timezone.utc
    if s.lower() == "aoe":
        return timezone(timedelta(hours=-12))
    if s.upper() == "UTC":
        return timezone.utc
    m = re.match(r"^UTC([+-])(\d{1,2})$", s.upper())
    if m:
        sign = 1 if m.group(1) == "+" else -1
        hours = int(m.group(2))
        return timezone(timedelta(hours=sign * hours))
    return timezone.utc


def _deadline_to_iso(deadline: str, tz_str: str) -> str:
    """
    Convert ccf-deadlines 'YYYY-MM-DD HH:MM:SS' into ISO 8601 with offset.
    Return "" if empty/TBD.
    """
    ds = _safe_str(deadline)
    if not ds or ds.upper() == "TBD":
        return ""
    tz = _tz_to_offset(tz_str)
    try:
        dt = datetime.strptime(ds, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
        return dt.isoformat()
    except Exception:
        # fallback: keep raw
        return ds


def canonicalize_subject(label: str) -> str:
    """
    Apply your subject rules:
      1) "CCF XX (YYY)" -> "YYY"
      2) "Wireless & Communication" -> "Wireless/Communication"
      3) Merge "Networking & Systems" and "Network System" -> "Network System"
    """
    s = _norm_space(label)

    # (1) keep only parentheses content if matches "CCF ... (...)"
    m = re.match(r"^CCF\s+\w+\s*\((.+)\)$", s, flags=re.IGNORECASE)
    if m:
        s = _norm_space(m.group(1))

    # also handle cases like "CCF NW (Network System)" even if spacing differs
    m2 = re.search(r"\(([^()]+)\)\s*$", s)
    if re.match(r"^CCF\s+\w+", s, flags=re.IGNORECASE) and m2:
        s = _norm_space(m2.group(1))

    # (2) rename wireless label
    if s.lower() == "wireless & communication" or s.lower() == "wireless and communication":
        s = "Wireless/Communication"

    # (3) merge networking/system label
    if s.lower() in {"networking & systems", "networking and systems", "networks and systems"}:
        s = "Network System"
    if s.lower() == "network system":
        s = "Network System"

    return s


def normalize_entry_subjects(entry: Dict[str, Any]) -> None:
    """
    Normalize entry['sub'] to List[str] with canonical subject names.
    """
    raw = entry.get("sub", "")
    subs = []
    if isinstance(raw, list):
        subs = [canonicalize_subject(x) for x in raw]
    elif isinstance(raw, str):
        if raw.strip():
            subs = [canonicalize_subject(raw)]
        else:
            subs = []
    else:
        subs = []

    subs = _dedupe_list([s for s in subs if s])
    if not subs:
        subs = ["Uncategorized"]

    entry["sub"] = subs


def normalize_entry_fields(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure all required keys exist and values are strings/lists as expected.
    """
    out = {
        "name": _norm_space(entry.get("name", "")),
        "sub": entry.get("sub", ""),
        "Location": _norm_space(entry.get("Location", "")),
        "Start Date": _norm_space(entry.get("Start Date", "")),
        "End Date": _norm_space(entry.get("End Date", "")),
        "Abstract Deadline": _norm_space(entry.get("Abstract Deadline", "")),
        "Submission Deadline": _norm_space(entry.get("Submission Deadline", "")),
        "Notification": _norm_space(entry.get("Notification", "")),
        "link": _norm_space(entry.get("link", "")),
    }

    # standardize some common date formats from non-ccf sources into "Mon DD YYYY"
    out["Start Date"] = _clean_deadline_human(out["Start Date"])
    out["End Date"] = _clean_deadline_human(out["End Date"])
    out["Abstract Deadline"] = _clean_deadline_human(out["Abstract Deadline"])
    out["Submission Deadline"] = _clean_deadline_human(out["Submission Deadline"])
    out["Notification"] = _clean_deadline_human(out["Notification"])

    normalize_entry_subjects(out)
    return out


def merge_entries(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge/update logic:
      - Keep existing entry, only update fields with non-empty new values
      - For deadlines, if new is non-empty and differs, update (deadlines can change)
      - For name: prefer longer non-empty
      - For subjects: union + canonicalize + dedupe
      - Do not delete anything
    """
    old = normalize_entry_fields(old)
    new = normalize_entry_fields(new)

    # name: prefer longer
    if new["name"] and (len(new["name"]) > len(old["name"])):
        old["name"] = new["name"]

    # link: update if old empty
    if not old["link"] and new["link"]:
        old["link"] = new["link"]

    # fields that should be refreshed if new provides data
    updatable = ["Location", "Start Date", "End Date", "Abstract Deadline", "Submission Deadline", "Notification"]
    for k in updatable:
        nv = new.get(k, "")
        if nv:
            if (not old.get(k, "")) or (old.get(k, "") != nv):
                old[k] = nv

    # subjects: union
    subs = _dedupe_list([canonicalize_subject(x) for x in (_ensure_list(old.get("sub")) + _ensure_list(new.get("sub")))])
    if not subs:
        subs = ["Uncategorized"]
    old["sub"] = subs

    return old


def build_match_keys(entry: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Return (link_key, exact_name_key, series_key)
    """
    link = _safe_str(entry.get("link", ""))
    link_key = link.lower().strip() if link else ""
    exact = _norm_key_exact(entry.get("name", ""))
    series = _norm_key_series(entry.get("name", ""))
    return link_key, exact, series


# -----------------------------
# Source 1: CSA lab conf-track
# -----------------------------
def fetch_csalab_conftrack(url: str = CSA_CONFTRACK_URL, timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Scrape the table from CSA Lab conf-track page.
    Output entries with minimal fields; subject may be missing from this source.
    """
    print(f"[CSALab] Fetch: {url}")
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find("table")
    if table is None:
        print("[CSALab] Warning: no <table> found.")
        return []

    # header names
    header_cells = table.find_all("tr")[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True) for c in header_cells]
    headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]

    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        row: Dict[str, str] = {}
        for i, col in enumerate(headers):
            if i >= len(tds):
                row[col] = ""
                continue
            cell = tds[i]
            if col.strip().lower().startswith("website"):
                a = cell.find("a", href=True)
                row[col] = _safe_str(a["href"]) if a else cell.get_text(" ", strip=True)
            else:
                row[col] = cell.get_text(" ", strip=True)

        # map to unified schema (best-effort; different versions may have different header names)
        name = row.get("Conf. Name") or row.get("Conference") or row.get("Conf") or row.get("Name") or ""
        loc = row.get("Location") or ""
        start = row.get("Start Date") or ""
        absd = row.get("Abstract Deadline") or ""
        subd = row.get("Submission Deadline") or row.get("Deadline") or ""
        link = row.get("Website") or row.get("Website Link") or row.get("Website URL") or ""

        if not _safe_str(name):
            continue

        rows.append({
            "name": _norm_space(name),
            "sub": "",  # CSA table typically doesn't carry final subject; we'll infer later if possible
            "Location": _norm_space(loc),
            "Start Date": _clean_deadline_human(_safe_str(start)),
            "End Date": "",
            "Abstract Deadline": _clean_deadline_human(_safe_str(absd)),
            "Submission Deadline": _clean_deadline_human(_safe_str(subd)),
            "Notification": "",
            "link": _norm_space(link),
        })

    print(f"[CSALab] Rows: {len(rows)}")
    return rows


# -----------------------------
# Source 2: EasyChair CFP
# -----------------------------
def fetch_easychair_table(url: str = EASYCHAIR_CFP_URL, timeout: int = 25) -> List[Dict[str, Any]]:
    """
    Scrape EasyChair CFP list table.

    We map topics/keywords into ONE of your subjects:
      - Wireless/Communication
      - Network System
      - Security & Privacy (optional)
    """
    print(f"[EasyChair] Fetch: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (+academic use)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find("table")
    if table is None:
        print("[EasyChair] Warning: no <table> found.")
        return []

    out: List[Dict[str, Any]] = []

    # keywords you care about (tune as needed)
    interest_terms = {"5g", "6g", "communication", "wireless", "signal", "network", "security", "privacy"}

    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        # 0: Acronym + link to details (relative)
        a0 = tds[0].find("a", href=True)
        acronym = tds[0].get_text(strip=True) or ""
        detail_url = ""
        if a0 and a0.get("href"):
            detail_url = a0["href"].strip()
            if detail_url.startswith("cfp/"):
                detail_url = detail_url  # relative ok; urljoin later
        # 1: Name
        name = tds[1].get_text(strip=True) or ""
        # 2: Location
        location = tds[2].get_text(strip=True) or ""
        # 3: Submission deadline (data-key ISO like YYYY-MM-DD)
        sub_deadline_iso = tds[3].get("data-key") or ""
        sub_deadline_text = tds[3].get_text(strip=True) or ""
        # 4: Start date
        start_date_iso = tds[4].get("data-key") or ""
        start_date_text = tds[4].get_text(strip=True) or ""
        # 5: Topics tags
        topics = [s.get_text(strip=True) for s in tds[5].select("span.tag")]
        topics_joined = "; ".join([t for t in topics if t]) if topics else ""

        # choose a matched term
        matched = ""
        hay = (topics_joined + " " + name).lower()
        for term in interest_terms:
            if term in hay:
                matched = term
                break

        if not matched:
            continue

        # map matched term -> subject
        if matched in {"security", "privacy"}:
            subject = "Security & Privacy"
        elif matched in {"network"}:
            subject = "Network System"
        else:
            subject = "Wireless/Communication"

        # format dates
        start_fmt = _clean_deadline_human(start_date_text) if start_date_text else ""
        sub_fmt = _clean_deadline_human(sub_deadline_text) if sub_deadline_text else ""

        # best-effort: try ISO keys if text missing
        if not start_fmt and start_date_iso:
            start_fmt = _clean_deadline_human(start_date_iso)
        if not sub_fmt and sub_deadline_iso:
            sub_fmt = _clean_deadline_human(sub_deadline_iso)

        # detail page may contain official website and abstract deadline
        official_link = ""
        abstract_deadline = ""
        if detail_url:
            try:
                official_link, abstract_deadline = fetch_easychair_detail(urljoin(url, detail_url), timeout=timeout)
            except Exception:
                official_link, abstract_deadline = "", ""

        # prefer acronym if name empty
        title = acronym if acronym else name
        if name:
            # include acronym + year if present in name
            title = acronym if acronym else name

        out.append({
            "name": _norm_space(title),
            "sub": [subject],
            "Location": _norm_space(location),
            "Start Date": start_fmt,
            "End Date": "",
            "Abstract Deadline": _clean_deadline_human(abstract_deadline),
            "Submission Deadline": sub_fmt,
            "Notification": "",
            "link": _norm_space(official_link),
        })

    print(f"[EasyChair] Rows: {len(out)}")
    return out


_EASYCHAIR_DETAIL_CACHE: Dict[str, Tuple[str, str]] = {}


def fetch_easychair_detail(detail_url: str, timeout: int = 25) -> Tuple[str, str]:
    """
    Fetch EasyChair CFP detail page and extract:
      - official website (best-effort)
      - abstract deadline (best-effort)
    """
    if detail_url in _EASYCHAIR_DETAIL_CACHE:
        return _EASYCHAIR_DETAIL_CACHE[detail_url]

    headers = {"User-Agent": "Mozilla/5.0 (+academic use)"}
    r = requests.get(detail_url, headers=headers, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    # official website: look for something like "Website:" + URL or any <a> that looks like external
    website = ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http") and "easychair.org" not in href:
            website = href
            break

    # abstract deadline: try regex around "Abstract" and "deadline"
    abs_deadline = ""
    m = re.search(r"Abstract\s+deadline\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})",
                  text, flags=re.IGNORECASE)
    if m:
        abs_deadline = m.group(1)

    _EASYCHAIR_DETAIL_CACHE[detail_url] = (website, abs_deadline)
    return website, abs_deadline


# -----------------------------
# Source 3: EDAS watchlist (optional)
# -----------------------------
def load_edas_watchlist(path: str) -> List[str]:
    """
    config/edas_watchlist.yaml example:
      urls:
        - https://edas.info/web/xxxxxx/
        - https://edas.info/N26675
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    urls = _ensure_list(cfg.get("urls"))
    return [_safe_str(u) for u in urls if _safe_str(u)]


def fetch_edas_watchlist(edas_urls: List[str], timeout: int = 25) -> List[Dict[str, Any]]:
    """
    EDAS does not offer a stable public all-conference CFP list.
    This function scrapes a user-provided list of EDAS pages (watchlist).
    """
    if not edas_urls:
        return []

    out: List[Dict[str, Any]] = []
    headers = {"User-Agent": "Mozilla/5.0"}

    for u in edas_urls:
        try:
            print(f"[EDAS] Fetch: {u}")
            r = requests.get(u, headers=headers, timeout=timeout)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text("\n", strip=True)

            # title
            title = ""
            h1 = soup.find(["h1", "h2"])
            if h1:
                title = h1.get_text(" ", strip=True)
            if not title:
                title = soup.title.get_text(" ", strip=True) if soup.title else ""
            title = _norm_space(title)

            # try to find location/date lines heuristically
            # Many EDAS web pages show something like "June 2-5, 2026 | Duisburg, Germany"
            loc = ""
            start = ""
            end = ""
            # Find the first line containing a month name and a year
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            for ln in lines[:60]:
                if re.search(r"\b(19|20)\d{2}\b", ln) and re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", ln, flags=re.IGNORECASE):
                    # split by "|" or "•"
                    parts = re.split(r"\s*\|\s*|\s*•\s*", ln)
                    if parts:
                        # first part likely date, later part likely location
                        start_end = parts[0].strip()
                        if len(parts) > 1:
                            loc = parts[1].strip()
                        # keep raw date; we won't over-parse ranges too aggressively here
                        start = _clean_deadline_human(start_end)
                    break

            # deadlines: look for "Paper Submission Deadline" / "Submission Deadline"
            sub_deadline = ""
            m = re.search(r"(Paper\s+Submission\s+Deadline|Submission\s+Deadline)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})",
                          text, flags=re.IGNORECASE)
            if m:
                sub_deadline = m.group(2)

            out.append({
                "name": title or _norm_space(u),
                "sub": ["Wireless/Communication"],  # default guess; you can adjust later in YAML/curation
                "Location": _norm_space(loc),
                "Start Date": _clean_deadline_human(start),
                "End Date": _clean_deadline_human(end),
                "Abstract Deadline": "",
                "Submission Deadline": _clean_deadline_human(sub_deadline),
                "Notification": "",
                "link": u,
            })
        except Exception as e:
            print(f"[EDAS] Warning: failed to fetch/parse {u}: {e}")

    print(f"[EDAS] Rows: {len(out)}")
    return out


# -----------------------------
# Source 4: ccf-deadlines (optional)
# -----------------------------
class GitHubClient:
    def __init__(self, token: Optional[str] = None, timeout: int = 30) -> None:
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({
            "User-Agent": "academic-conference-tracker/1.0",
            "Accept": "application/vnd.github+json",
        })
        if token:
            self.sess.headers.update({"Authorization": f"Bearer {token}"})

    def get_json(self, url: str) -> Any:
        r = self.sess.get(url, timeout=self.timeout)
        if r.status_code == 403 and "rate limit" in r.text.lower():
            raise RuntimeError("GitHub API rate limit hit. Set GITHUB_TOKEN env var.")
        r.raise_for_status()
        return r.json()

    def get_text(self, url: str) -> str:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.text


def fetch_ccfddl_yamls(gh: GitHubClient, categories: List[str]) -> List[Dict[str, Any]]:
    """
    Download all YAML under ccf-deadlines/conference/<CAT>/*.yml
    """
    out: List[Dict[str, Any]] = []
    for cat in categories:
        api_url = f"{CCFDDL_CONTENTS}/conference/{cat}"
        items = gh.get_json(api_url)
        if not isinstance(items, list):
            continue
        for it in items:
            name = it.get("name", "")
            if not str(name).endswith(".yml"):
                continue
            dl = it.get("download_url")
            if not dl:
                continue
            yml_text = gh.get_text(dl)
            loaded = yaml.safe_load(yml_text)
            if isinstance(loaded, list) and loaded and isinstance(loaded[0], dict):
                out.append(loaded[0])
            elif isinstance(loaded, dict):
                out.append(loaded)
    return out


_MONTH_MAP = {
    "Jan": "January", "Feb": "February", "Mar": "March", "Apr": "April",
    "May": "May", "Jun": "June", "Jul": "July", "Aug": "August",
    "Sep": "September", "Sept": "September", "Oct": "October",
    "Nov": "November", "Dec": "December"
}


def _parse_ccf_date_range(date_str: str, year: int) -> Tuple[str, str]:
    """
    Parse ccf-deadlines inst['date'] into ("Mon DD YYYY", "Mon DD YYYY").
    Handles common patterns:
      - "June 12-17, 2026"
      - "April 29-May 4, 2026"
      - "May 19, 2026"
    """
    s = _safe_str(date_str)
    if not s:
        return "", ""

    # remove trailing year (we'll use 'year')
    s = re.sub(rf",\s*{year}\b", "", s)
    s = re.sub(rf"\b{year}\b", "", s).strip()

    # normalize month abbreviations
    for abbr, full in _MONTH_MAP.items():
        s = re.sub(rf"\b{re.escape(abbr)}\b", full, s)

    if "-" not in s:
        try:
            dt = datetime.strptime(f"{s}, {year}", "%B %d, %Y")
            d = dt.strftime("%b %d %Y")
            return d, d
        except Exception:
            return "", ""

    left, right = [p.strip() for p in s.split("-", 1)]
    has_month_in_right = bool(re.search(r"\b[A-Za-z]+\b", right))
    if not has_month_in_right:
        m = re.match(r"^([A-Za-z]+)\s+\d{1,2}$", left)
        if m:
            right = f"{m.group(1)} {right}"

    try:
        dt_start = datetime.strptime(f"{left}, {year}", "%B %d, %Y")
        dt_end = datetime.strptime(f"{right}, {year}", "%B %d, %Y")
        return dt_start.strftime("%b %d %Y"), dt_end.strftime("%b %d %Y")
    except Exception:
        return "", ""


def ccfddl_to_entries(
    ccf_confs: List[Dict[str, Any]],
    year_from: int,
    year_to: int,
) -> List[Dict[str, Any]]:
    """
    Transform ccf-deadlines YAML objects into frontend entries.
    Subject is the *label only* (no 'CCF XX (...)' prefix), then canonicalized.
    Timeline is expanded (each round becomes a separate entry).
    """
    out: List[Dict[str, Any]] = []
    for conf in ccf_confs:
        title = _safe_str(conf.get("title"))
        sub_code = _safe_str(conf.get("sub"))
        label = CCF_SUB_LABEL.get(sub_code, sub_code) if sub_code else "Uncategorized"
        label = canonicalize_subject(label)  # also merges network system naming

        confs_list = _ensure_list(conf.get("confs"))
        for inst in confs_list:
            if not isinstance(inst, dict):
                continue
            year = int(inst.get("year", 0) or 0)
            if year < year_from or year > year_to:
                continue

            place = _safe_str(inst.get("place"))
            date_str = _safe_str(inst.get("date"))
            tz_str = _safe_str(inst.get("timezone"))
            link = _safe_str(inst.get("link"))

            start_d, end_d = _parse_ccf_date_range(date_str, year)

            timeline = _ensure_list(inst.get("timeline"))
            if not timeline:
                out.append({
                    "name": f"{title} {year}".strip(),
                    "sub": [label],
                    "Location": place,
                    "Start Date": start_d,
                    "End Date": end_d,
                    "Abstract Deadline": "",
                    "Submission Deadline": "",
                    "Notification": "",
                    "link": link,
                })
                continue

            for t in timeline:
                if not isinstance(t, dict):
                    continue
                ddl = _deadline_to_iso(_safe_str(t.get("deadline")), tz_str)
                abs_ddl = _deadline_to_iso(_safe_str(t.get("abstract_deadline")), tz_str)
                comment = _safe_str(t.get("comment"))

                suffix = f" - {comment}" if comment else ""
                out.append({
                    "name": f"{title} {year}{suffix}".strip(),
                    "sub": [label],
                    "Location": place,
                    "Start Date": start_d,
                    "End Date": end_d,
                    "Abstract Deadline": abs_ddl,
                    "Submission Deadline": ddl,
                    "Notification": "",
                    "link": link,
                })

    print(f"[CCFDDL] Rows: {len(out)}")
    return out


# -----------------------------
# Merge/update pipeline
# -----------------------------
def load_existing_json(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    # canonicalize subjects in existing too (requirement 1-3)
    out = [normalize_entry_fields(x) for x in data if isinstance(x, dict)]
    return out


def build_index(existing: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Build indices for fast matching.
    We store multiple keys -> index. If collisions occur, keep first.
    """
    idx: Dict[str, int] = {}
    for i, e in enumerate(existing):
        link_key, exact, series = build_match_keys(e)
        if link_key:
            idx.setdefault("L:" + link_key, i)
        if exact:
            idx.setdefault("N:" + exact, i)
        if series:
            # fallback key
            idx.setdefault("S:" + series, i)
    return idx


def merge_update(
    existing: List[Dict[str, Any]],
    new_entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Only add; do not delete.
    If match found -> merge/update. Else append.
    """
    existing = [normalize_entry_fields(e) for e in existing]
    idx = build_index(existing)

    added = 0
    updated = 0

    for ne in new_entries:
        ne2 = normalize_entry_fields(ne)
        link_key, exact, series = build_match_keys(ne2)

        hit = None
        if link_key and ("L:" + link_key) in idx:
            hit = idx["L:" + link_key]
        elif exact and ("N:" + exact) in idx:
            hit = idx["N:" + exact]
        elif series and ("S:" + series) in idx:
            hit = idx["S:" + series]

        if hit is None:
            existing.append(ne2)
            new_i = len(existing) - 1
            # update index with new entry keys
            lk, ex, se = build_match_keys(ne2)
            if lk:
                idx.setdefault("L:" + lk, new_i)
            if ex:
                idx.setdefault("N:" + ex, new_i)
            if se:
                idx.setdefault("S:" + se, new_i)
            added += 1
        else:
            existing[hit] = merge_entries(existing[hit], ne2)
            updated += 1

    print(f"[Merge] Updated: {updated}, Added: {added}, Kept total: {len(existing)}")
    return existing


def sort_output(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort by Submission Deadline (parseable first), then name.
    Empty deadlines go last.
    """
    def _ts(s: str) -> Optional[float]:
        s = _safe_str(s)
        if not s:
            return None
        # ISO first
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
        # human "Mon DD YYYY"
        try:
            dt = datetime.strptime(s, "%b %d %Y")
            return dt.timestamp()
        except Exception:
            return None

    def _key(e: Dict[str, Any]):
        ts = _ts(_safe_str(e.get("Submission Deadline")))
        if ts is None:
            return (1, 9e18, _safe_str(e.get("name")).lower())
        return (0, ts, _safe_str(e.get("name")).lower())

    return sorted(entries, key=_key)


def write_json(path: str, entries: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
        f.write("\n")


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="data/conferences.json", help="Existing JSON path (baseline)")
    ap.add_argument("--out", dest="out_path", default="data/conferences.json", help="Output JSON path")
    ap.add_argument("--disable-ccfddl", action="store_true", help="Disable ccf-deadlines source")
    ap.add_argument("--ccf-categories", default=",".join(DEFAULT_CCF_CATEGORIES),
                    help="Comma-separated CCF categories (e.g., DS,NW,SC,SE,DB,CT,CG,AI,HI,MX)")
    ap.add_argument("--year-from", type=int, default=2026, help="ccf-deadlines include instances from this year")
    ap.add_argument("--year-to", type=int, default=2028, help="ccf-deadlines include instances up to this year")
    ap.add_argument("--edas-watchlist", default="config/edas_watchlist.yaml", help="EDAS watchlist yaml path")
    ap.add_argument("--github-token", default=os.getenv("GITHUB_TOKEN", ""), help="GitHub token (env GITHUB_TOKEN)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    # 0) Load existing baseline (only add, do not delete)
    existing = load_existing_json(args.in_path)
    print(f"[Load] Existing entries: {len(existing)}")

    new_entries: List[Dict[str, Any]] = []

    # 1) CSA lab
    try:
        new_entries.extend(fetch_csalab_conftrack())
    except Exception as e:
        print(f"[CSALab] Error: {e}")

    # 2) EasyChair
    try:
        new_entries.extend(fetch_easychair_table())
    except Exception as e:
        print(f"[EasyChair] Error: {e}")

    # 3) EDAS watchlist
    try:
        edas_urls = load_edas_watchlist(args.edas_watchlist)
        if edas_urls:
            new_entries.extend(fetch_edas_watchlist(edas_urls))
        else:
            print("[EDAS] Watchlist empty or missing; skipped.")
    except Exception as e:
        print(f"[EDAS] Error: {e}")

    # 4) ccf-deadlines
    if not args.disable_ccfddl:
        try:
            cats = [c.strip() for c in args.ccf_categories.split(",") if c.strip()]
            gh = GitHubClient(token=(args.github_token or None))
            ccf_yamls = fetch_ccfddl_yamls(gh, cats)
            new_entries.extend(ccfddl_to_entries(ccf_yamls, year_from=args.year_from, year_to=args.year_to))
        except Exception as e:
            print(f"[CCFDDL] Error: {e}")
    else:
        print("[CCFDDL] Disabled by flag.")

    # 5) Apply subject canonicalization to new entries (and also infer CSA subjects best-effort)
    # CSA source often lacks 'sub'; we default its empty sub to "Uncategorized" already.
    # If you want CSA entries to map into Wireless/Communication/Network System automatically,
    # you can add a keyword-based inference here. We keep it conservative.
    normalized_new = [normalize_entry_fields(e) for e in new_entries]

    # 6) Merge/update into existing (only add, not delete)
    merged = merge_update(existing, normalized_new)

    # 7) Canonicalize all subjects again (requirement 1-3) + final sort
    merged = [normalize_entry_fields(e) for e in merged]
    merged = sort_output(merged)

    # 8) Write output
    write_json(args.out_path, merged)
    print(f"[Write] Output -> {args.out_path} (entries={len(merged)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())