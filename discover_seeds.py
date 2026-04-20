#!/usr/bin/env python3
"""discover_seeds.py — auto-suggest seed topics from your site + competitors + niche.

Sources:
  - Your domain's sitemap     (proven topics for your site, +2 weight)
  - Competitor sitemaps       (industry-validated topics, +1 per competitor)
  - Niche-template combinator (verbs × nouns × locations, +1 baseline)

Returns ranked candidates with sources visible. Output is a JSON list of dicts
(phrase, score, sources). Designed to feed batch.py --seeds.

Usage:
  python discover_seeds.py --domain habitaone.com \\
      --niche "venezuela real estate" \\
      --competitors remax.com.ve,rentahouse.com.ve

  # Or via Python:
  from discover_seeds import discover_seeds
  candidates = discover_seeds(
      domain="habitaone.com",
      niche="venezuela real estate",
      competitors=["remax.com.ve"],
  )
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def _fetch(url, timeout=10):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _parse_sitemap_urls(xml_text):
    """Extract <loc> URLs from sitemap XML. Tolerates namespace + malformed."""
    urls = set()
    if not xml_text:
        return urls
    try:
        root = ET.fromstring(xml_text)
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag == "loc" and elem.text:
                urls.add(elem.text.strip())
    except ET.ParseError:
        # Regex fallback for malformed XML
        urls.update(re.findall(r"<loc>([^<]+)</loc>", xml_text))
    return urls


def fetch_sitemap_urls(domain, max_urls=300, max_subsitemaps=8):
    """Pull URLs from a domain's sitemap. Handles index files + sub-sitemaps."""
    base = domain.strip()
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    base = base.rstrip("/")

    candidates = [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_index.xml",
        f"{base}/sitemap-index.xml",
        f"{base}/sitemaps/sitemap.xml",
    ]
    found_urls = set()
    sub_sitemaps = []

    for url in candidates:
        text = _fetch(url)
        if not text:
            continue
        for u in _parse_sitemap_urls(text):
            if u.endswith(".xml"):
                sub_sitemaps.append(u)
            else:
                found_urls.add(u)
        if found_urls or sub_sitemaps:
            break  # found a working sitemap

    # Fetch sub-sitemaps in parallel
    if sub_sitemaps:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for text in ex.map(_fetch, sub_sitemaps[:max_subsitemaps]):
                if text:
                    for u in _parse_sitemap_urls(text):
                        if not u.endswith(".xml"):
                            found_urls.add(u)
                        if len(found_urls) >= max_urls:
                            break

    return list(found_urls)[:max_urls]


_LANG_PREFIXES = {"en", "es", "fr", "de", "pt", "it", "ja", "zh",
                  "es-ve", "es-co", "es-mx", "es-es", "en-us", "en-gb"}


def slug_to_phrase(url, drop_numeric=True):
    """Convert a URL path to a search-shape phrase."""
    try:
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        # Drop file extensions
        parts = [re.sub(r"\.[a-z]+$", "", p) for p in parts]
        # Strip leading language code (en/, es/, en-us/, etc.)
        if parts and parts[0].lower() in _LANG_PREFIXES:
            parts = parts[1:]
        # Replace -, _ with spaces
        phrase = " ".join(parts).replace("-", " ").replace("_", " ")
        if drop_numeric:
            phrase = re.sub(r"\b\d+\b", "", phrase)
        phrase = re.sub(r"[^\w\s]", " ", phrase, flags=re.UNICODE)
        phrase = re.sub(r"\s+", " ", phrase).strip().lower()
        return phrase
    except Exception:
        return ""


_JUNK_SUBSTRINGS = (
    "page", "category", "tag", "author", "feed", "comment",
    "wp content", "wp admin", "login", "logout",
    "photo", "image", "thumb", "icon", "scraped", "true o",
    "1024x768", "x768", "x1024",
)


def _looks_like_id_token(tok):
    """Tokens like 'gav2662471768953239' or '5c9d98373e380697' — IDs not words."""
    if len(tok) < 5:
        return False
    has_digit = any(c.isdigit() for c in tok)
    has_alpha = any(c.isalpha() for c in tok)
    if not (has_digit and has_alpha):
        return False
    # Long mixed alphanum without typical word patterns
    digit_ratio = sum(1 for c in tok if c.isdigit()) / len(tok)
    return digit_ratio > 0.2 or len(tok) > 12


def _good_phrase(phrase):
    """True if phrase looks like a search query, not a slug ID/garbage."""
    if not phrase:
        return False
    wc = len(phrase.split())
    if not 2 <= wc <= 6:
        return False
    if any(s in phrase for s in _JUNK_SUBSTRINGS):
        return False
    tokens = phrase.split()
    # Reject phrases with ANY ID-looking token
    if any(_looks_like_id_token(t) for t in tokens):
        return False
    # Reject phrases where most tokens are 1-2 chars (URL noise)
    short_count = sum(1 for t in tokens if len(t) <= 2)
    if short_count > len(tokens) // 2:
        return False
    return True


def candidates_from_sitemap(domain, label, max_urls=300):
    """Extract phrase candidates (2–6 words) from a domain's sitemap."""
    urls = fetch_sitemap_urls(domain, max_urls=max_urls)
    counts = defaultdict(int)
    for url in urls:
        phrase = slug_to_phrase(url)
        if not _good_phrase(phrase):
            continue
        counts[phrase] += 1
    return [(p, label) for p in counts]


# Common Spanish verb/noun/location templates for real-estate niches.
# Extensible: pass a different template_pack to discover_seeds().
DEFAULT_TEMPLATES = {
    "real estate es": {
        "verbs": ["comprar", "vender", "alquilar", "rentar"],
        "nouns": ["casa", "apartamento", "terreno", "inmueble", "departamento"],
        "locations": ["venezuela", "caracas", "maracaibo", "valencia",
                      "barquisimeto", "maracay", "ciudad guayana"],
        "modifiers": ["barata", "lujo", "obra nueva", "amueblado"],
    },
    "real estate en": {
        "verbs": ["buy", "sell", "rent", "find"],
        "nouns": ["house", "apartment", "land", "property", "condo"],
        "locations": ["caracas", "maracaibo", "valencia"],
        "modifiers": ["cheap", "luxury", "new"],
    },
}


def template_candidates(template_key):
    """Generate niche-template seed candidates from cartesian product."""
    tmpl = DEFAULT_TEMPLATES.get(template_key)
    if not tmpl:
        return []
    out = []
    for v in tmpl["verbs"]:
        for n in tmpl["nouns"]:
            out.append((f"{v} {n}", "template"))
            for loc in tmpl["locations"]:
                out.append((f"{v} {n} {loc}", "template"))
    return out


def detect_template(niche):
    """Map a freeform niche string to a template key."""
    if not niche:
        return None
    nl = niche.lower()
    if any(k in nl for k in ["real estate", "inmobiliaria", "bienes raices",
                              "bienes raíces", "casa", "apartamento", "vivienda"]):
        return "real estate es" if any(c in nl for c in [
            "ñ", "á", "é", "í", "ó", "ú", "venezuela", "españa", "mexico"
        ]) or not nl.isascii() else "real estate en"
    return None


def discover_seeds(domain=None, niche=None, competitors=None, limit=20,
                   max_urls_per_site=300):
    """Main entry. Returns ranked list of {phrase, score, sources}."""
    candidates = defaultdict(lambda: {"sources": set(), "score": 0})

    sources_to_fetch = []
    if domain:
        sources_to_fetch.append((domain, "yours", 2))
    if competitors:
        for comp in competitors[:15]:  # cap to keep latency reasonable
            short = comp.split("/")[-1] if "/" in comp else comp
            sources_to_fetch.append((comp, f"comp:{short}", 1))

    # Parallel sitemap fetches across all sites (4 workers)
    def _process(args):
        domain_, label_, weight_ = args
        return label_, weight_, candidates_from_sitemap(
            domain_, label_, max_urls=max_urls_per_site)

    if sources_to_fetch:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for label, weight, found in ex.map(_process, sources_to_fetch):
                for phrase, _src in found:
                    candidates[phrase]["sources"].add(label)
                    candidates[phrase]["score"] += weight

    # Niche templates
    template_key = detect_template(niche)
    if template_key:
        for phrase, _src in template_candidates(template_key):
            candidates[phrase]["sources"].add("template")
            candidates[phrase]["score"] += 1

    # Rank by score desc, then by phrase length asc (shorter seeds preferred)
    ranked = sorted(
        candidates.items(),
        key=lambda kv: (-kv[1]["score"], len(kv[0])),
    )

    return [
        {"phrase": p, "score": c["score"], "sources": sorted(c["sources"])}
        for p, c in ranked[:limit]
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", help="Your site's domain (sitemap mining)")
    ap.add_argument("--niche", help="Niche keywords (template combinator)")
    ap.add_argument("--competitors", default="",
                    help="Comma-separated competitor domains")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--out", help="Write seeds.txt (one phrase per line)")
    args = ap.parse_args()

    competitors = [c.strip() for c in args.competitors.split(",") if c.strip()]
    candidates = discover_seeds(
        domain=args.domain, niche=args.niche,
        competitors=competitors, limit=args.limit,
    )

    if args.out:
        Path(args.out).write_text(
            "\n".join(c["phrase"] for c in candidates), encoding="utf-8")
        print(f"Wrote {len(candidates)} seeds to {args.out}", file=sys.stderr)

    print(json.dumps(candidates, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
