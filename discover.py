#!/usr/bin/env python3
"""
discover.py — find long-tail and trending questions for a seed topic.

Free sources (no API keys required):
  - Google Autocomplete (alphabet soup + question-prefix expansion)
  - Reddit JSON endpoints (search + rising in target subreddits)
  - Google Trends via pytrends (related top + rising queries)

Usage:
  python discover.py --seed "remesas venezuela" --subreddits venezuela,vzla
  python discover.py --seed "arepas" --hl es --gl ve --geo VE
  python discover.py --seed "best running shoes" --hl en --gl us --geo US \\
                     --subreddits running,RunningShoeGeeks
"""

import argparse
import csv
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests


def _load_dotenv(path=None):
    """Tiny .env loader — no external dep. Keys already in os.environ win."""
    if path is None:
        path = Path(__file__).resolve().parent / ".env"
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

QUESTION_STARTERS = {
    "es": [
        "qué", "que", "cuál", "cual", "cuáles", "cuales",
        "cuándo", "cuando", "cómo", "como", "dónde", "donde",
        "adónde", "adonde", "por qué", "por que", "para qué", "para que",
        "quién", "quien", "quiénes", "quienes",
        "cuánto", "cuanto", "cuánta", "cuanta",
        "cuántos", "cuantos", "cuántas", "cuantas",
    ],
    "en": [
        "what", "how", "why", "which", "when", "where", "who",
        "can", "do", "does", "is", "are", "should", "will",
    ],
}

ALPHABET = list("abcdefghijklmnopqrstuvwxyz")
# Realistic UA — DDG rate-limits identifying bot UAs aggressively (returns 403).
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

SERP_FORUM_DOMAINS = (
    "reddit.com", "quora.com", "stackexchange.com", "stackoverflow.com",
    "youtube.com", "facebook.com", "tiktok.com", "forocoches.com",
    "taringa.net", "medium.com", "linkedin.com",
)

# DDG's "kl" region parameter — short locale code -> DDG region code.
# Covers Spanish-speaking Venezuelan diaspora markets + common English fallbacks.
LOCALE_TO_KL = {
    "ve": "ve-es", "co": "co-es", "us": "us-es", "es": "es-es",
    "cl": "cl-es", "ar": "ar-es", "pa": "pa-es", "mx": "mx-es",
    "pe": "pe-es", "ec": "ec-es", "do": "do-es", "bo": "bo-es",
    "us-en": "us-en", "uk": "uk-en", "ca": "ca-en", "au": "au-en",
}

# Curated domain-authority scores (0-10) for common high-authority sites.
# Fills the gap that pure title-match heuristics miss: a SERP full of weak
# titles but owned by giants (banesco.com, BBC, etc.) is still unwinnable.
KNOWN_AUTHORITY = {
    # global giants
    "wikipedia.org": 10, "youtube.com": 10, "google.com": 10, "amazon.com": 10,
    "facebook.com": 10, "instagram.com": 9, "tiktok.com": 9,
    "twitter.com": 9, "x.com": 9, "reddit.com": 9, "linkedin.com": 9,
    "github.com": 9, "microsoft.com": 10, "apple.com": 10,
    "medium.com": 8, "quora.com": 8, "pinterest.com": 8,
    # news
    "bbc.com": 10, "cnn.com": 9, "nytimes.com": 10, "reuters.com": 10,
    "bloomberg.com": 9, "forbes.com": 9, "wsj.com": 10, "theguardian.com": 10,
    # money / remittance
    "westernunion.com": 9, "moneygram.com": 8, "wise.com": 9,
    "remitly.com": 8, "paypal.com": 9, "zelle.com": 8,
    # venezuelan
    "banesco.com": 9, "banescointernacional.com": 8, "mercantilbanco.com": 8,
    "provincial.com": 8, "bancodevenezuela.com": 8,
    "bcv.org.ve": 9, "saime.gob.ve": 9, "patria.org.ve": 9,
    "cne.gob.ve": 9, "seniat.gob.ve": 9, "gob.ve": 9,
    "eluniversal.com": 8, "elnacional.com": 8, "elestimulo.com": 7,
    "efectococuyo.com": 7, "2001online.com": 7, "eldiario.com": 7,
    "noticierodigital.com": 6,
}

_authority_cache = {}


def domain_authority(domain):
    """Return 0-10 authority score for a domain.

    Chain: cache -> curated list -> TLD pattern -> Open PageRank API
    (if OPR_API_KEY env set) -> default 5.
    """
    if not domain:
        return 5
    d = domain.lower().lstrip("www.")
    if d in _authority_cache:
        return _authority_cache[d]

    score = None
    if d in KNOWN_AUTHORITY:
        score = KNOWN_AUTHORITY[d]
    else:
        # match suffixes (e.g. any .gov.ve subdomain)
        for k, v in KNOWN_AUTHORITY.items():
            if d.endswith("." + k):
                score = v
                break
        # govt / edu TLD patterns
        if score is None:
            if re.search(r"\.(gov|edu)(\.|$)", d) or d.endswith(".mil"):
                score = 9
            elif re.search(r"\.gob\.[a-z]{2}$", d):
                score = 9

    if score is None:
        score = _opr_lookup(d)

    if score is None:
        score = 5  # unknown -> assume medium

    _authority_cache[d] = score
    return score


def _opr_lookup(domain):
    """Optional Open PageRank lookup; returns None if not configured or errors."""
    key = os.environ.get("OPR_API_KEY")
    if not key:
        return None
    try:
        r = requests.get(
            "https://openpagerank.com/api/v1.0/getPageRank",
            params={"domains[]": domain},
            headers={"API-OPR": key},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        data = r.json().get("response", [])
        if data and data[0].get("status_code") == 200:
            pr = float(data[0].get("page_rank_decimal", 0))
            return round(pr)
    except Exception as e:
        print(f"  opr lookup error: {e}", file=sys.stderr)
    return None


# Module-level autocomplete cache. Populated/persisted by load_autocomplete_cache /
# flush_autocomplete_cache; checked transparently in autocomplete().
_AUTOCOMPLETE_CACHE = {}
_AUTOCOMPLETE_CACHE_DIRTY = False


def _ac_key(query, hl, gl):
    return f"{hl}|{gl}|{query}"


def load_autocomplete_cache(path, ttl_days=7):
    """Read cache from disk. Drops entries older than ttl_days."""
    global _AUTOCOMPLETE_CACHE
    import json
    from datetime import timedelta
    p = Path(path)
    if not p.exists():
        _AUTOCOMPLETE_CACHE = {}
        return
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        cutoff = datetime.now() - timedelta(days=ttl_days)
        kept = {}
        for k, v in raw.items():
            try:
                ts = datetime.fromisoformat(v["ts"])
                if ts >= cutoff:
                    kept[k] = v
            except Exception:
                pass
        _AUTOCOMPLETE_CACHE = kept
    except Exception:
        _AUTOCOMPLETE_CACHE = {}


def flush_autocomplete_cache(path):
    if not _AUTOCOMPLETE_CACHE_DIRTY:
        return
    import json
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_AUTOCOMPLETE_CACHE, ensure_ascii=False),
                 encoding="utf-8")


def autocomplete(query, hl="es", gl="ve", timeout=10):
    global _AUTOCOMPLETE_CACHE_DIRTY
    key = _ac_key(query, hl, gl)
    cached = _AUTOCOMPLETE_CACHE.get(key)
    if cached is not None:
        return cached["results"]
    url = "https://suggestqueries.google.com/complete/search"
    params = {"client": "firefox", "q": query, "hl": hl, "gl": gl}
    try:
        r = requests.get(url, params=params, timeout=timeout,
                         headers={"User-Agent": UA})
        if r.status_code == 200:
            data = r.json()
            if len(data) >= 2 and isinstance(data[1], list):
                SERP_STATS["autocomplete_ok"] += 1
                _AUTOCOMPLETE_CACHE[key] = {
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "results": data[1],
                }
                _AUTOCOMPLETE_CACHE_DIRTY = True
                return data[1]
        SERP_STATS["autocomplete_fail"] += 1
    except Exception as e:
        print(f"  autocomplete error for '{query}': {e}", file=sys.stderr)
        SERP_STATS["autocomplete_fail"] += 1
    return []


def expand_seed(seed, hl="es", gl="ve", workers=8):
    """Fan out alphabet-soup + question-prefix expansion concurrently.

    Google's suggestqueries endpoint tolerates parallel requests well
    (no observed blocking up to ~10 concurrent). Cuts wall time from
    ~15s/seed (sequential, 0.3s sleeps) to ~1-2s/seed.
    """
    from concurrent.futures import ThreadPoolExecutor

    starters = QUESTION_STARTERS.get(hl[:2], QUESTION_STARTERS["en"])
    queries = [seed]
    queries.extend(f"{seed} {l}" for l in ALPHABET)
    queries.extend(f"{p} {seed}" for p in starters)

    suggestions = set()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for results in ex.map(lambda q: autocomplete(q, hl, gl), queries):
            for s in results:
                s = s.lower().strip()
                if s:
                    suggestions.add(s)
    return suggestions


def is_question(phrase, hl="es"):
    p = phrase.lower().strip()
    if p.endswith("?"):
        return True
    starters = QUESTION_STARTERS.get(hl[:2], QUESTION_STARTERS["en"])
    return any(p == s or p.startswith(s + " ") for s in starters)


# Intent classification. Rule-based, cheap (no HTTP), runs before SERP checks.
# Dynamic intents (price-today, breaking news) have SERPs refreshed multiple
# times per day by dedicated trackers — unrankable for evergreen content.
INTENT_PATTERNS = [
    # (intent_name, is_dynamic, [regex patterns])
    ("price-today", True, [
        r"\bhoy\b", r"\b(precio|valor|cotizacion|cotización)\b.*\bhoy\b",
        r"\b(today|tonight|now)\b", r"\bprice\s+today\b",
        r"\b\d{1,2}\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)",
        r"\bcuanto\s+(cuesta|vale|esta|cobra)\b.*\bhoy\b",
    ]),
    ("tutorial", False, [
        r"\btutorial\b", r"\bpaso\s+a\s+paso\b", r"\bstep[-\s]by[-\s]step\b",
        r"\bgu[ií]a\s+(de|para|completa)\b", r"\bguide\b",
    ]),
    ("how-to", False, [
        r"^c[oó]mo\b", r"^how\s+to\b", r"^como\s+(hacer|enviar|usar|activar|configurar|instalar|obtener|sacar)\b",
    ]),
    ("what-is", False, [
        r"^qu[eé]\s+(es|son|significa)\b", r"^what\s+(is|are|does)\b",
        r"^para\s+qu[eé]\s+sirve\b",
    ]),
    ("why", False, [
        r"^por\s+qu[eé]\b", r"^why\b",
    ]),
    ("compare", False, [
        r"\bvs\.?\b", r"\bversus\b", r"\bdiferencia\s+entre\b",
        r"\bdifference\s+between\b", r"\bmejor\s+\w+\s+o\s+\w+\b",
    ]),
    ("list", False, [
        r"^mejores?\s+\d*\s*\w+", r"^top\s+\d+", r"^best\s+\w+",
        r"^\d+\s+(mejores|best|top)\b",
    ]),
    ("review", False, [
        r"\breview\b", r"\brese[ñn]a\b", r"\bopiniones?\b",
        r"\bes\s+confiable\b", r"\bes\s+(bueno|buena|seguro|segura)\b",
    ]),
    ("location", True, [
        r"\bcerca\s+de\s+mi\b", r"\bnear\s+me\b",
        r"\ben\s+(caracas|maracaibo|valencia|barquisimeto|maracay|ciudad\s+guayana)\b",
    ]),
    ("when", False, [
        r"^cu[aá]ndo\b", r"^when\b",
    ]),
    ("where", False, [
        r"^d[oó]nde\b", r"^where\b",
    ]),
]


def classify_intent(phrase):
    """Return (intent_name, is_dynamic). 'general' if no pattern matches."""
    p = phrase.lower().strip()
    for name, dynamic, patterns in INTENT_PATTERNS:
        for pat in patterns:
            if re.search(pat, p):
                return (name, dynamic)
    return ("general", False)


_REDDIT_CACHE = {}
_REDDIT_CACHE_DIRTY = False


def load_reddit_cache(path, ttl_days=2):
    global _REDDIT_CACHE
    import json
    from datetime import timedelta
    p = Path(path)
    if not p.exists():
        _REDDIT_CACHE = {}
        return
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        cutoff = datetime.now() - timedelta(days=ttl_days)
        kept = {}
        for k, v in raw.items():
            try:
                ts = datetime.fromisoformat(v["ts"])
                if ts >= cutoff:
                    kept[k] = v
            except Exception:
                pass
        _REDDIT_CACHE = kept
    except Exception:
        _REDDIT_CACHE = {}


def flush_reddit_cache(path):
    if not _REDDIT_CACHE_DIRTY:
        return
    import json
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_REDDIT_CACHE, ensure_ascii=False),
                 encoding="utf-8")


def reddit_fetch(url, params=None, timeout=15):
    global _REDDIT_CACHE_DIRTY
    # Cache by URL + params
    cache_key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
    cached = _REDDIT_CACHE.get(cache_key)
    if cached is not None:
        return cached["children"]
    try:
        r = requests.get(url, params=params or {},
                         headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code == 200:
            SERP_STATS["reddit_ok"] += 1
            children = r.json().get("data", {}).get("children", [])
            _REDDIT_CACHE[cache_key] = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "children": children,
            }
            _REDDIT_CACHE_DIRTY = True
            return children
        print(f"  reddit {url} -> {r.status_code}", file=sys.stderr)
        SERP_STATS["reddit_fail"] += 1
    except Exception as e:
        print(f"  reddit error {url}: {e}", file=sys.stderr)
        SERP_STATS["reddit_fail"] += 1
    return []


def reddit_search(seed, subreddits, limit=50):
    out = []
    for sub in subreddits:
        children = reddit_fetch(
            f"https://www.reddit.com/r/{sub}/search.json",
            {"q": seed, "restrict_sr": "true", "sort": "relevance",
             "limit": limit, "t": "year"},
        )
        for post in children:
            d = post.get("data", {})
            out.append({
                "title": d.get("title", ""),
                "subreddit": sub,
                "score": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "permalink": f"https://reddit.com{d.get('permalink', '')}",
                "source": "reddit_search",
            })
        time.sleep(1)
    return out


def reddit_rising(subreddits, limit=25):
    out = []
    for sub in subreddits:
        children = reddit_fetch(
            f"https://www.reddit.com/r/{sub}/rising.json",
            {"limit": limit},
        )
        for post in children:
            d = post.get("data", {})
            out.append({
                "title": d.get("title", ""),
                "subreddit": sub,
                "score": d.get("score", 0),
                "num_comments": d.get("num_comments", 0),
                "permalink": f"https://reddit.com{d.get('permalink', '')}",
                "source": "reddit_rising",
            })
        time.sleep(1)
    return out


_TRENDS_CACHE = {}
_TRENDS_CACHE_DIRTY = False


def load_trends_cache(path, ttl_days=7):
    global _TRENDS_CACHE
    import json
    from datetime import timedelta
    p = Path(path)
    if not p.exists():
        _TRENDS_CACHE = {}
        return
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        cutoff = datetime.now() - timedelta(days=ttl_days)
        kept = {}
        for k, v in raw.items():
            try:
                ts = datetime.fromisoformat(v["ts"])
                if ts >= cutoff:
                    kept[k] = v
            except Exception:
                pass
        _TRENDS_CACHE = kept
    except Exception:
        _TRENDS_CACHE = {}


def flush_trends_cache(path):
    if not _TRENDS_CACHE_DIRTY:
        return
    import json
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_TRENDS_CACHE, ensure_ascii=False),
                 encoding="utf-8")


# Once trends fails this many times in one process, stop trying entirely.
# Pytrends silently retries internally and adds 5-30s of latency per failure.
_TRENDS_FAIL_THRESHOLD = 2


def _do_trends_fetch(seed, geo, hl):
    """Inner pytrends call — runs in a worker thread with a hard timeout."""
    from pytrends.request import TrendReq
    pt = TrendReq(hl=hl, tz=240, timeout=(5, 8), retries=0)
    pt.build_payload([seed], geo=geo, timeframe="today 12-m")
    rq = pt.related_queries()
    top = rq.get(seed, {}).get("top")
    rising = rq.get(seed, {}).get("rising")
    return {
        "top": top.to_dict("records") if top is not None else [],
        "rising": rising.to_dict("records") if rising is not None else [],
    }


def trends_related(seed, geo="VE", hl="es-419"):
    """Cache-first, fail-fast Trends fetch.

    - Disk cache hit → return immediately
    - Once trends_fail >= _TRENDS_FAIL_THRESHOLD this run, return empty
      without attempting (avoids cascading 30s backoffs)
    - Otherwise: run pytrends in a thread with a 10s hard timeout
    """
    global _TRENDS_CACHE_DIRTY
    key = f"{geo}|{hl}|{seed}"
    cached = _TRENDS_CACHE.get(key)
    if cached is not None:
        return cached["payload"]

    if SERP_STATS["trends_fail"] >= _TRENDS_FAIL_THRESHOLD:
        empty = {"top": [], "rising": []}
        _TRENDS_CACHE[key] = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "payload": empty,
        }
        _TRENDS_CACHE_DIRTY = True
        return empty

    try:
        import importlib
        importlib.import_module("pytrends.request")
    except ImportError:
        print("  pytrends not installed; skipping Trends layer", file=sys.stderr)
        SERP_STATS["trends_fail"] += 1
        return {"top": [], "rising": []}

    from concurrent.futures import ThreadPoolExecutor, TimeoutError as TFE
    payload = None
    err = None
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_do_trends_fetch, seed, geo, hl)
        try:
            payload = fut.result(timeout=10)
        except TFE:
            err = "timeout (>10s)"
        except Exception as e:
            err = str(e)[:80]

    if payload is not None:
        SERP_STATS["trends_ok"] += 1
        _TRENDS_CACHE[key] = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "payload": payload,
        }
        _TRENDS_CACHE_DIRTY = True
        return payload

    print(f"  trends error: {err}", file=sys.stderr)
    SERP_STATS["trends_fail"] += 1
    empty = {"top": [], "rising": []}
    _TRENDS_CACHE[key] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "payload": empty,
    }
    _TRENDS_CACHE_DIRTY = True
    return empty


def score_phrase(phrase, is_q, word_count):
    """Demand-only score. Tiered to give variance even when SERP data is absent.

    Range: 0 (junk) to 7 (long question with multi-word distinctive content).
    Calibrated so the median demand sits at ~3 and clear long-tails at ~5-7,
    keeping score discrimination useful even when SERP scoring fails entirely.
    """
    s = 0
    if is_q:
        s += 2
    # Length tiers: each step adds another point
    if word_count >= 3:
        s += 1
    if word_count >= 5:
        s += 1
    if word_count >= 7:
        s += 1  # Long-tail bonus — these are typically high-intent
    if word_count >= 9:
        s += 1
    return s


LOCALE_TO_GL_HL = {
    "ve": ("ve", "es"), "co": ("co", "es"), "us": ("us", "es"),
    "es": ("es", "es"), "cl": ("cl", "es"), "ar": ("ar", "es"),
    "pa": ("pa", "es"), "mx": ("mx", "es"), "pe": ("pe", "es"),
    "bo": ("bo", "es"), "ec": ("ec", "es"), "do": ("do", "es"),
    "us-en": ("us", "en"), "uk": ("uk", "en"),
    "ca": ("ca", "en"), "au": ("au", "en"),
}


def _build_serp_metrics(domains, urls, titles, query, locale):
    """Common metric computation shared by all SERP backends."""
    if not domains:
        return None
    forum_count = sum(
        1 for d in domains if any(f in d for f in SERP_FORUM_DOMAINS)
    )
    counts = Counter(domains)
    max_repeat = max(counts.values())

    q_words = [w for w in query.lower().split() if len(w) >= 3]
    threshold = max(2, int(len(q_words) * 0.6)) if q_words else 1
    title_matches = 0
    for t in titles[:10]:
        tl = t.lower()
        hits = sum(1 for w in q_words if w in tl)
        if hits >= threshold:
            title_matches += 1

    top5 = domains[:5]
    authorities = [domain_authority(d) for d in top5]
    avg_authority = sum(authorities) / len(authorities) if authorities else 5

    return {
        "locale": locale or "",
        "forum_count": forum_count,
        "title_match_count": title_matches,
        "max_domain_repeat": max_repeat,
        "top_domain": domains[0],
        "top_domains": ";".join(domains[:5]),
        "top_urls": urls[:5],
        "avg_authority": round(avg_authority, 1),
        "max_authority": max(authorities) if authorities else 5,
    }


def _check_serp_ddg(query, timeout=15, locale=None):
    """DDG HTML scrape backend (free)."""
    data = {"q": query}
    if locale:
        kl = LOCALE_TO_KL.get(locale, locale)
        data["kl"] = kl
    try:
        r = requests.post(
            "https://html.duckduckgo.com/html/",
            data=data,
            headers={"User-Agent": UA},
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        urls_raw = re.findall(r'class="result__a"\s+href="([^"]+)"', r.text)
        titles = re.findall(r'class="result__a"[^>]*>([^<]+)</a>', r.text)
    except Exception as e:
        print(f"  serp[ddg] error for '{query}': {e}", file=sys.stderr)
        return None

    domains, clean_urls = [], []
    for u in urls_raw:
        try:
            host = urlparse(u).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            # DDG injects internal nav / sponsored / instant-answer links —
            # filter so they don't pollute the organic top-10 metrics.
            if not host or host.endswith("duckduckgo.com"):
                continue
            domains.append(host)
            clean_urls.append(u)
            if len(domains) >= 10:
                break
        except Exception:
            pass

    serp = _build_serp_metrics(domains, clean_urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "ddg"
    return serp


def _check_serp_serpapi(query, timeout=20, locale=None):
    """Real Google SERP via SerpApi. Includes PAA, featured snippets, etc.
    Requires SERPAPI_KEY env var. Costs ~$0.005 per request.
    Returns None when no API key or on error.
    """
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        return None
    gl, hl = LOCALE_TO_GL_HL.get(locale, ("us", "en"))
    params = {
        "engine": "google", "q": query, "gl": gl, "hl": hl,
        "num": 10, "api_key": api_key,
    }
    try:
        r = requests.get("https://serpapi.com/search", params=params,
                         timeout=timeout)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f"  serp[serpapi] error for '{query}': {e}", file=sys.stderr)
        return None

    organic = data.get("organic_results", [])[:10]
    domains, urls, titles, dates = [], [], [], []
    for o in organic:
        url = o.get("link", "")
        title = o.get("title", "") or ""
        try:
            host = urlparse(url).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
                urls.append(url)
                titles.append(title)
                if o.get("date"):
                    dates.append(o["date"])
        except Exception:
            pass

    serp = _build_serp_metrics(domains, urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "serpapi"

    # SERP feature signals — only available via Google directly
    answer_box = data.get("answer_box")
    paa = data.get("related_questions") or data.get("people_also_ask") or []
    inline_videos = data.get("inline_videos") or []
    serp["has_featured_snippet"] = bool(answer_box)
    serp["has_paa"] = len(paa) > 0
    serp["paa_questions"] = [
        q.get("question", "").strip()
        for q in paa[:6] if q.get("question")
    ]
    serp["has_video"] = len(inline_videos) > 0
    return serp


def _check_serp_serper(query, timeout=15, locale=None):
    """Serper.dev — real Google SERPs via API. ~$0.001/req. Best free-tier backend
    when key is set (fast, reliable, real Google ranking).
    """
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        return None
    gl, hl = LOCALE_TO_GL_HL.get(locale, ("us", "en")) if locale else ("us", "en")
    payload = {"q": query, "gl": gl, "hl": hl, "num": 10}
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            json=payload,
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f"  serp[serper] error for '{query}': {e}", file=sys.stderr)
        return None

    items = data.get("organic", [])[:10]
    domains, urls, titles = [], [], []
    for o in items:
        url = o.get("link", "")
        title = o.get("title", "") or ""
        try:
            host = urlparse(url).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
                urls.append(url)
                titles.append(title)
        except Exception:
            pass

    serp = _build_serp_metrics(domains, urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "serper"
    # Serper returns PAA + answer-box too — pull them in
    paa = data.get("peopleAlsoAsk", [])
    if paa:
        serp["has_paa"] = True
        serp["paa_questions"] = [q.get("question", "") for q in paa[:6]
                                 if q.get("question")]
    if data.get("answerBox"):
        serp["has_featured_snippet"] = True
    return serp


# Brave Search supports a fixed set of country codes; map our locales to the
# nearest match (or ALL if no good match) to avoid 422s.
BRAVE_COUNTRY_MAP = {
    "ve": "ALL", "co": "ALL", "pe": "ALL", "pa": "ALL", "ec": "ALL",
    "do": "ALL", "bo": "ALL", "uy": "ALL", "py": "ALL",
    "ar": "AR", "cl": "CL", "es": "ES", "mx": "MX",
    "us": "US", "uk": "GB", "ca": "CA", "au": "AU",
}


def _check_serp_brave(query, timeout=15, locale=None):
    """Brave Search API — independent index. Free 2000/month."""
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        return None
    gl, hl = LOCALE_TO_GL_HL.get(locale, ("us", "en")) if locale else ("us", "en")
    brave_country = BRAVE_COUNTRY_MAP.get(gl, "ALL")
    params = {"q": query, "country": brave_country,
              "search_lang": hl, "count": 10}
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params=params,
            headers={"X-Subscription-Token": api_key,
                     "Accept": "application/json"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f"  serp[brave] error for '{query}': {e}", file=sys.stderr)
        return None

    items = (data.get("web") or {}).get("results", [])[:10]
    domains, urls, titles = [], [], []
    for o in items:
        url = o.get("url", "")
        title = o.get("title", "") or ""
        try:
            host = urlparse(url).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
                urls.append(url)
                titles.append(title)
        except Exception:
            pass

    serp = _build_serp_metrics(domains, urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "brave"
    return serp


def _check_serp_google_cse(query, timeout=15, locale=None):
    """Google Custom Search JSON API — real Google organic results.

    Requires GOOGLE_CSE_API_KEY + GOOGLE_CSE_ID env vars.
    Free tier: 100 queries/day per key. Returns None when no creds or on error.
    """
    api_key = os.environ.get("GOOGLE_CSE_API_KEY")
    cse_id = os.environ.get("GOOGLE_CSE_ID")
    if not (api_key and cse_id):
        return None
    params = {"key": api_key, "cx": cse_id, "q": query, "num": 10}
    if locale:
        gl, hl = LOCALE_TO_GL_HL.get(locale, ("us", "en"))
        params["gl"] = gl
        params["hl"] = hl
    try:
        r = requests.get("https://www.googleapis.com/customsearch/v1",
                         params=params, timeout=timeout)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        print(f"  serp[google_cse] error for '{query}': {e}", file=sys.stderr)
        return None

    items = data.get("items", [])[:10]
    domains, urls, titles = [], [], []
    for o in items:
        url = o.get("link", "")
        title = o.get("title", "") or ""
        try:
            host = urlparse(url).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
                urls.append(url)
                titles.append(title)
        except Exception:
            pass

    serp = _build_serp_metrics(domains, urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "google_cse"
    return serp


def _check_serp_mojeek(query, timeout=15, locale=None):
    """Mojeek HTML scrape — independent search index. Use as a 3rd-tier
    fallback when both DDG and Startpage are throttling."""
    headers = {
        "User-Agent": UA,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }
    params = {"q": query}
    if locale:
        gl, _hl = LOCALE_TO_GL_HL.get(locale, ("us", "en"))
        params["arc"] = gl
    try:
        r = requests.get("https://www.mojeek.com/search", params=params,
                         headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        # Mojeek title-anchor structure:
        # <h2><a class="title" title="..." href="URL">visible title</a></h2>
        result_links = re.findall(
            r'<h2>\s*<a[^>]*class="title"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)</a>',
            r.text,
        )
        urls, titles = [], []
        for url, title_html in result_links:
            urls.append(url)
            t = re.sub(r"<[^>]+>", "", title_html)
            titles.append(t.strip())
    except Exception as e:
        print(f"  serp[mojeek] error for '{query}': {e}", file=sys.stderr)
        return None

    domains, clean_urls = [], []
    for u in urls:
        try:
            host = urlparse(u).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            if not host or host.endswith("mojeek.com"):
                continue
            domains.append(host)
            clean_urls.append(u)
            if len(domains) >= 10:
                break
        except Exception:
            pass

    serp = _build_serp_metrics(domains, clean_urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "mojeek"
    return serp


def _check_serp_startpage(query, timeout=15, locale=None):
    """Startpage HTML scrape — proxies Google, friendlier to scrapers than DDG/Bing.

    Returns same shape as other backends. None on failure.
    """
    headers = {
        "User-Agent": UA,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml",
    }
    params = {"query": query}
    if locale:
        gl, _hl = LOCALE_TO_GL_HL.get(locale, ("us", "en"))
        params["cat"] = "web"
        # Startpage's region param is "lui" or "language"; both are spotty
        # so we mostly rely on hl in the query language. Locale is best-effort.
    try:
        r = requests.get("https://www.startpage.com/do/search",
                         params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        # Startpage organic results: <a class="result-title result-link ..." href="...">title</a>
        result_links = re.findall(
            r'<a[^>]*class="[^"]*result-(?:title|link)[^"]*"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)</a>',
            r.text,
        )
        urls, titles = [], []
        for url, title_html in result_links:
            urls.append(url)
            t = re.sub(r"<[^>]+>", "", title_html)
            titles.append(t.strip())
    except Exception as e:
        print(f"  serp[startpage] error for '{query}': {e}", file=sys.stderr)
        return None

    domains, clean_urls = [], []
    for u in urls:
        try:
            host = urlparse(u).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            # Filter Startpage's own internal redirects
            if not host or host.endswith("startpage.com"):
                continue
            domains.append(host)
            clean_urls.append(u)
            if len(domains) >= 10:
                break
        except Exception:
            pass

    serp = _build_serp_metrics(domains, clean_urls, titles, query, locale)
    if serp is None:
        return None
    serp["source"] = "startpage"
    return serp


# Module-level run stats — pipelined into the TL;DR status line.
SERP_STATS = {"serpapi_ok": 0,
              "serper_ok": 0, "serper_fail": 0, "serper_skipped": 0,
              "brave_ok": 0, "brave_fail": 0, "brave_skipped": 0,
              "google_cse_ok": 0, "google_cse_fail": 0, "google_cse_skipped": 0,
              "ddg_ok": 0, "ddg_fail": 0, "ddg_skipped": 0,
              "startpage_ok": 0, "startpage_fail": 0, "startpage_skipped": 0,
              "mojeek_ok": 0, "fail": 0,
              "freshness_ok": 0, "freshness_fail": 0,
              "trends_ok": 0, "trends_fail": 0,
              "reddit_ok": 0, "reddit_fail": 0,
              "autocomplete_ok": 0, "autocomplete_fail": 0}

# After this many DDG failures in one run, skip DDG and go straight to Startpage.
# Avoids wasting ~2s per phrase on a known-blocked endpoint.
_DDG_GIVE_UP_AFTER = 5


def reset_serp_stats():
    for k in SERP_STATS:
        SERP_STATS[k] = 0


def check_serp(query, timeout=15, locale=None):
    """Dispatch chain for SERP backends.

    Order (best → fallback):
      0. SerpApi          — paid, premium, only when SERPAPI_KEY set
      1. Serper.dev       — real Google, ~$0.001/req, fast & reliable
      2. Brave            — independent index, free 2k/mo
      3. Google CSE       — real Google, free 100/day
      4. DDG              — free scrape (often rate-limited)
      5. Startpage        — free scrape (Google proxy)
      6. Mojeek           — free scrape (independent index)

    Each backend short-circuits after `_DDG_GIVE_UP_AFTER` failures in this
    run (saves ~2s per query when a backend is blocking).
    """
    if os.environ.get("SERPAPI_KEY"):
        s = _check_serp_serpapi(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["serpapi_ok"] += 1
            return s

    # Tier 1: Serper
    if (os.environ.get("SERPER_API_KEY")
            and SERP_STATS["serper_fail"] < _DDG_GIVE_UP_AFTER):
        s = _check_serp_serper(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["serper_ok"] += 1
            return s
        SERP_STATS["serper_fail"] += 1
    elif os.environ.get("SERPER_API_KEY"):
        SERP_STATS["serper_skipped"] += 1

    # Tier 2: Brave
    if (os.environ.get("BRAVE_API_KEY")
            and SERP_STATS["brave_fail"] < _DDG_GIVE_UP_AFTER):
        s = _check_serp_brave(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["brave_ok"] += 1
            return s
        SERP_STATS["brave_fail"] += 1
    elif os.environ.get("BRAVE_API_KEY"):
        SERP_STATS["brave_skipped"] += 1

    # Tier 3: Google CSE
    if (os.environ.get("GOOGLE_CSE_API_KEY")
            and SERP_STATS["google_cse_fail"] < _DDG_GIVE_UP_AFTER):
        s = _check_serp_google_cse(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["google_cse_ok"] += 1
            return s
        SERP_STATS["google_cse_fail"] += 1
    elif os.environ.get("GOOGLE_CSE_API_KEY"):
        SERP_STATS["google_cse_skipped"] += 1

    # Tier 4: DDG
    if SERP_STATS["ddg_fail"] < _DDG_GIVE_UP_AFTER:
        s = _check_serp_ddg(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["ddg_ok"] += 1
            return s
        SERP_STATS["ddg_fail"] += 1
    else:
        SERP_STATS["ddg_skipped"] += 1

    # Tier 5: Startpage
    if SERP_STATS["startpage_fail"] < _DDG_GIVE_UP_AFTER:
        s = _check_serp_startpage(query, timeout=timeout, locale=locale)
        if s:
            SERP_STATS["startpage_ok"] += 1
            return s
        SERP_STATS["startpage_fail"] += 1
    else:
        SERP_STATS["startpage_skipped"] += 1

    # Tier 6: Mojeek
    s = _check_serp_mojeek(query, timeout=timeout, locale=locale)
    if s:
        SERP_STATS["mojeek_ok"] += 1
        return s

    SERP_STATS["fail"] += 1
    return None


SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
ENGLISH_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def extract_page_date(html):
    """Best-effort published/updated date extraction. Returns datetime or None."""
    from datetime import datetime as dt
    if not html:
        return None
    sample = html[:200000]

    # 1. og / article meta tags
    patterns = [
        r'(?:property|name)=["\'](?:article:published_time|article:modified_time|og:updated_time|og:article:published_time|datePublished|dateModified)["\'][^>]*content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'](?:article:published_time|article:modified_time|og:updated_time|datePublished|dateModified)["\']',
    ]
    for pat in patterns:
        m = re.search(pat, sample, re.IGNORECASE)
        if m:
            try:
                iso = m.group(1).replace("Z", "+00:00")
                return dt.fromisoformat(iso[:19])
            except Exception:
                pass

    # 2. JSON-LD
    for m in re.finditer(r'"date(?:Published|Modified)"\s*:\s*"([^"]+)"', sample):
        try:
            iso = m.group(1).replace("Z", "+00:00")
            return dt.fromisoformat(iso[:19])
        except Exception:
            pass

    # 3. <time datetime="...">
    m = re.search(r'<time[^>]*datetime=["\']([^"\']+)["\']', sample, re.IGNORECASE)
    if m:
        try:
            iso = m.group(1).replace("Z", "+00:00")
            return dt.fromisoformat(iso[:19])
        except Exception:
            pass

    # 4. Spanish visible date: "15 de enero de 2024"
    m = re.search(
        r'(\d{1,2})\s+de\s+(' + "|".join(SPANISH_MONTHS) + r')\s+de\s+(\d{4})',
        sample, re.IGNORECASE,
    )
    if m:
        try:
            return dt(int(m.group(3)), SPANISH_MONTHS[m.group(2).lower()], int(m.group(1)))
        except Exception:
            pass

    # 5. English visible date: "January 15, 2024" or "Jan 15 2024"
    m = re.search(
        r'\b(' + "|".join(ENGLISH_MONTHS) + r')\s+(\d{1,2}),?\s+(\d{4})\b',
        sample, re.IGNORECASE,
    )
    if m:
        try:
            return dt(int(m.group(3)), ENGLISH_MONTHS[m.group(1).lower()], int(m.group(2)))
        except Exception:
            pass

    # 6. ISO date anywhere plausible
    m = re.search(r'\b(20\d{2})-(\d{2})-(\d{2})\b', sample)
    if m:
        try:
            return dt(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass

    return None


def _extract_visible_text(html):
    """Strip tags + boilerplate, return normalized visible text."""
    if not html:
        return ""
    cleaned = re.sub(
        r"<(script|style|noscript|nav|footer|header|aside|form)[^>]*>.*?</\1>",
        " ", html, flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(r"<[^>]+>", " ", cleaned)
    text = re.sub(r"&[a-z#0-9]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _estimate_word_count(html):
    return len(_extract_visible_text(html).split())


def fetch_page_stats(url, timeout=6):
    """Fetch URL once, return {'age_days', 'word_count', 'body_tokens'}.

    body_tokens is a set of lowercased word tokens from visible text, used by
    callers for query-body overlap analysis without reshipping the raw text.
    """
    try:
        r = requests.get(
            url, timeout=timeout,
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml",
            },
            allow_redirects=True,
        )
        if r.status_code != 200:
            SERP_STATS["freshness_fail"] += 1
            return None
        html = r.text
        d = extract_page_date(html)
        text = _extract_visible_text(html)
        words = text.split()
        tokens = set(re.findall(r"\w+", text.lower()))
        SERP_STATS["freshness_ok"] += 1
        return {
            "age_days": (datetime.now() - d).days if d else None,
            "word_count": len(words),
            "body_tokens": tokens,
        }
    except Exception:
        SERP_STATS["freshness_fail"] += 1
        return None


def check_freshness(serp, query="", top_n=3, workers=3):
    """Fetch top_n result URLs in parallel. Populate age, word count, and
    query/body match ratio. One HTTP request per URL covers all three signals.
    """
    if not serp or not serp.get("top_urls"):
        return serp
    from concurrent.futures import ThreadPoolExecutor
    urls = serp["top_urls"][:top_n]
    q_tokens = set(
        w for w in re.findall(r"\w+", query.lower()) if len(w) >= 3
    ) if query else set()

    ages, words, match_ratios = [], [], []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for stats in ex.map(fetch_page_stats, urls):
            if not stats:
                continue
            if stats["age_days"] is not None:
                ages.append(stats["age_days"])
            if stats["word_count"] > 0:
                words.append(stats["word_count"])
            if q_tokens and stats.get("body_tokens"):
                hits = len(q_tokens & stats["body_tokens"])
                match_ratios.append(hits / len(q_tokens))

    if ages:
        serp["avg_age_days"] = int(sum(ages) / len(ages))
        serp["min_age_days"] = min(ages)
        serp["fresh_count"] = sum(1 for a in ages if a <= 90)
    else:
        serp["avg_age_days"] = ""
        serp["min_age_days"] = ""
        serp["fresh_count"] = ""
    if words:
        serp["avg_words"] = int(sum(words) / len(words))
        serp["min_words"] = min(words)
    else:
        serp["avg_words"] = ""
        serp["min_words"] = ""
    if match_ratios:
        serp["avg_body_match"] = round(sum(match_ratios) / len(match_ratios), 2)
        serp["min_body_match"] = round(min(match_ratios), 2)
    else:
        serp["avg_body_match"] = ""
        serp["min_body_match"] = ""
    return serp


def serp_weakness(serp, is_dynamic=False):
    """0-10+ score, higher = weaker SERP = easier to rank.

    Combines signals:
      + forum presence
      + title-match rate
      + domain repetition
      - authority wall
      + staleness (when freshness data available)
      - dynamic intent penalty (price-today etc. — SERP refreshed daily)
    """
    if not serp:
        return 0
    s = 0
    if serp["forum_count"] >= 3:
        s += 3
    elif serp["forum_count"] >= 1:
        s += 1
    if serp["title_match_count"] <= 2:
        s += 3
    elif serp["title_match_count"] <= 4:
        s += 1
    if serp["max_domain_repeat"] >= 4:
        s += 3
    elif serp["max_domain_repeat"] >= 3:
        s += 2

    avg_auth = serp.get("avg_authority", 5)
    if avg_auth >= 8:
        s -= 4
    elif avg_auth >= 6.5:
        s -= 2
    elif avg_auth >= 5.5:
        s -= 1

    avg_age = serp.get("avg_age_days")
    if isinstance(avg_age, int):
        if avg_age >= 730:
            s += 2
        elif avg_age >= 365:
            s += 1
        elif avg_age <= 60:
            s -= 1

    avg_w = serp.get("avg_words")
    if isinstance(avg_w, int) and avg_w > 0:
        if avg_w < 500:
            s += 2
        elif avg_w < 1000:
            s += 1
        elif avg_w >= 3500:
            s -= 2
        elif avg_w >= 2500:
            s -= 1

    avg_bm = serp.get("avg_body_match")
    if isinstance(avg_bm, (int, float)) and avg_bm > 0:
        # Top pages mention only a fraction of query words -> they're not
        # answering the query directly, a dedicated post can own it.
        if avg_bm < 0.4:
            s += 2
        elif avg_bm < 0.6:
            s += 1
        elif avg_bm >= 0.95:
            s -= 1

    # SERP feature signals (only populated when SerpApi backend is in use)
    if serp.get("has_featured_snippet"):
        # Stealable with structured content + schema markup
        s += 1
    if serp.get("has_video"):
        # Video carousel lowers organic real estate
        s -= 1

    if is_dynamic:
        s -= 3

    return max(0, s)


def main():
    """Single-seed entry point. Thin wrapper around batch.py — translates
    --seed (singular) into batch.py's --seeds and delegates so single-seed
    users get clustering, shortlist, history, outlines, etc. for free.

    Unknown args (--workers, --shortlist, --recurse, --locales...) pass
    through unchanged.
    """
    ap = argparse.ArgumentParser(
        description="Single-seed long-tail discovery (delegates to batch.py).")
    ap.add_argument("--seed", required=True, help="Seed topic / keyword.")
    args, extra = ap.parse_known_args()

    sys.argv = [sys.argv[0], "--seeds", args.seed] + extra
    from batch import main as batch_main
    batch_main()


if __name__ == "__main__":
    main()
