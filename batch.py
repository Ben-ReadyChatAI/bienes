#!/usr/bin/env python3
"""
batch.py — run discovery across multiple seeds and merge into ranked CSVs.

Phrases that appear under multiple seeds get a cross-seed boost (stronger
signal that the phrase spans your niche, not just one angle of it).

Usage:
  python batch.py --seeds "remesas venezuela,dolar paralelo,arepas recetas" \\
                  --subreddits venezuela,vzla

  python batch.py --seeds-file seeds.txt --hl es --gl ve --geo VE \\
                  --subreddits venezuela,vzla

seeds.txt format — one seed per line, blank lines and # comments ignored:
  remesas venezuela
  dolar paralelo
  # diaspora topics
  homologar titulo venezolano espana
"""

import argparse
import csv
import json
import re
import shutil
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from discover import (
    expand_seed,
    reddit_search,
    reddit_rising,
    trends_related,
    is_question,
    score_phrase,
    check_serp,
    check_freshness,
    serp_weakness,
    classify_intent,
    SERP_STATS,
    reset_serp_stats,
    load_autocomplete_cache,
    flush_autocomplete_cache,
    load_trends_cache,
    flush_trends_cache,
    load_reddit_cache,
    flush_reddit_cache,
)


def build_status_summary(stats, n_serp_checks_attempted):
    """Render a single status string for the TL;DR header.

    Returns ('ok'|'degraded', summary_string).
    """
    serp_ok = (stats["serpapi_ok"] + stats["serper_ok"] + stats["brave_ok"]
               + stats["google_cse_ok"] + stats["ddg_ok"]
               + stats["startpage_ok"] + stats["mojeek_ok"])
    serp_total = serp_ok + stats["fail"]
    parts = []
    degraded = False

    if serp_total > 0:
        # Which backend did most of the work
        backends = []
        if stats["serpapi_ok"]:
            backends.append(f"serpapi={stats['serpapi_ok']}")
        if stats["serper_ok"]:
            backends.append(f"serper={stats['serper_ok']}")
        if stats["brave_ok"]:
            backends.append(f"brave={stats['brave_ok']}")
        if stats["google_cse_ok"]:
            backends.append(f"google_cse={stats['google_cse_ok']}")
        if stats["ddg_ok"]:
            backends.append(f"ddg={stats['ddg_ok']}")
        if stats["startpage_ok"]:
            backends.append(f"startpage={stats['startpage_ok']}")
        if stats["mojeek_ok"]:
            backends.append(f"mojeek={stats['mojeek_ok']}")
        backend_str = ", ".join(backends) if backends else "none"
        if serp_ok < serp_total * 0.7:
            degraded = True
            parts.append(
                f"⚠ SERP DEGRADED ({serp_ok}/{serp_total} succeeded; {backend_str})"
            )
        else:
            parts.append(f"SERP OK ({serp_ok}/{serp_total}; {backend_str})")
    elif n_serp_checks_attempted > 0:
        degraded = True
        parts.append("⚠ SERP UNAVAILABLE (all backends failed)")

    fr_total = stats["freshness_ok"] + stats["freshness_fail"]
    if fr_total > 0:
        if stats["freshness_ok"] < fr_total * 0.5:
            degraded = True
            parts.append(
                f"⚠ FRESHNESS DEGRADED ({stats['freshness_ok']}/{fr_total})"
            )

    if stats["trends_fail"] and not stats["trends_ok"]:
        degraded = True
        parts.append("⚠ Trends unavailable (rate-limited)")

    if stats["reddit_fail"] and not stats["reddit_ok"]:
        degraded = True
        parts.append("⚠ Reddit unavailable")

    ac_total = stats["autocomplete_ok"] + stats["autocomplete_fail"]
    if ac_total and stats["autocomplete_ok"] < ac_total * 0.5:
        degraded = True
        parts.append(
            f"⚠ AUTOCOMPLETE DEGRADED ({stats['autocomplete_ok']}/{ac_total})"
        )

    if not parts:
        return ("ok", "✅ all signals OK")
    if not degraded:
        return ("ok", "✅ " + " | ".join(parts))
    return ("degraded", " | ".join(parts))


class RateLimiter:
    """Global token-bucket across threads. Caps network SERP requests per second.

    A single lock + last-timestamp is enough at small concurrency (<10 workers)
    and avoids a background refill thread.
    """

    def __init__(self, per_second=3.0):
        self.interval = 1.0 / max(per_second, 0.1)
        self.lock = threading.Lock()
        self.last_ts = 0.0

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.last_ts + self.interval - now
            if wait > 0:
                time.sleep(wait)
            self.last_ts = time.monotonic()


def fetch_phrase_serp(row, locales, cache, check_freshness_flag, rate_limiter=None):
    """Run all SERP + optional freshness work for a single row. Mutates in place.
    Returns True if any network call was made.
    """
    query = row["phrase"]
    any_network = False

    def _net_check(locale=None):
        nonlocal any_network
        any_network = True
        if rate_limiter is not None:
            rate_limiter.acquire()
        return check_serp(query, locale=locale)

    if locales:
        locale_serps = {}
        for loc in locales:
            if cache is not None:
                cached = cache.get(query, loc)
                if cached is not None:
                    locale_serps[loc] = cached
                    continue
                # Skip recent failures — they cost ~3s per attempt and likely
                # still fail. The 30-min TTL handles eventual recovery.
                if cache.is_known_failure(query, loc):
                    locale_serps[loc] = None
                    continue
            try:
                s = _net_check(locale=loc)
            except Exception:
                s = None
            locale_serps[loc] = s
            if cache is not None:
                if s:
                    cache.put(query, loc, s)
                else:
                    cache.put_failure(query, loc)

        best_serp, best_weakness, best_locale = None, -99, None
        per_locale_weakness = {}
        for loc, serp in locale_serps.items():
            if not serp:
                continue
            w = serp_weakness(serp, is_dynamic=row["is_dynamic"])
            per_locale_weakness[loc] = w
            if w > best_weakness:
                best_weakness, best_serp, best_locale = w, serp, loc
        if best_serp:
            row["serp"] = best_serp
            row["best_locale"] = best_locale
            row["per_locale_weakness"] = per_locale_weakness
    else:
        if cache is not None:
            cached = cache.get(query, None)
            if cached is not None:
                row["serp"] = cached
            elif cache.is_known_failure(query, None):
                row["serp"] = None
            else:
                s = _net_check()
                row["serp"] = s
                if s:
                    cache.put(query, None, s)
                else:
                    cache.put_failure(query, None)
        else:
            row["serp"] = _net_check()

    if row["serp"] and check_freshness_flag:
        if any_network or "avg_age_days" not in row["serp"]:
            check_freshness(row["serp"], query=query)
    return any_network


class SerpCache:
    """JSON-backed cache of SERP results keyed by (phrase, locale).

    Successful entries live `ttl_days`; failure entries live `failure_ttl_minutes`
    (much shorter — external services recover quickly so we want to retry soon,
    but skip retries within a single test cycle to keep warm runs fast).
    """

    FAILURE_MARKER = {"_failed": True}

    def __init__(self, path, ttl_days=7, failure_ttl_minutes=30):
        self.path = Path(path)
        self.ttl = timedelta(days=ttl_days)
        self.failure_ttl = timedelta(minutes=failure_ttl_minutes)
        self.data = {}
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                now = datetime.now()
                for k, v in raw.items():
                    try:
                        ts = datetime.fromisoformat(v["ts"])
                        is_fail = v.get("fail", False)
                        cutoff = ts + (self.failure_ttl if is_fail else self.ttl)
                        if cutoff >= now:
                            self.data[k] = v
                    except Exception:
                        pass
            except Exception:
                self.data = {}
        self.hits = 0
        self.misses = 0
        self.writes = 0
        self.skipped_failures = 0

    @staticmethod
    def key(phrase, locale):
        return f"{phrase}|{locale or '_'}"

    def get(self, phrase, locale):
        """Return cached serp dict on hit, None on miss/known-failure.
        Caller can distinguish via is_known_failure()."""
        v = self.data.get(self.key(phrase, locale))
        if v is None:
            self.misses += 1
            return None
        if v.get("fail"):
            self.skipped_failures += 1
            return None
        self.hits += 1
        return v["serp"]

    def is_known_failure(self, phrase, locale):
        """True if this (phrase, locale) recently failed across all backends."""
        v = self.data.get(self.key(phrase, locale))
        return bool(v and v.get("fail"))

    def put(self, phrase, locale, serp):
        if not serp:
            return
        self.data[self.key(phrase, locale)] = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "serp": serp,
        }
        self.writes += 1

    def put_failure(self, phrase, locale):
        """Record an all-backends-failed result so warm runs skip the retry."""
        self.data[self.key(phrase, locale)] = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "serp": None,
            "fail": True,
        }
        self.writes += 1

    def flush(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self.data, ensure_ascii=False, default=list),
            encoding="utf-8",
        )


def load_last_autocomplete(history_dir):
    """Return {phrase: weakness_or_demand} from the most recent history CSV."""
    history_dir = Path(history_dir)
    if not history_dir.exists():
        return {}, None
    csvs = sorted(history_dir.glob("batch_autocomplete_*.csv"))
    if not csvs:
        return {}, None
    last = csvs[-1]
    prev = {}
    with open(last, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                weakness = row.get("serp_weakness", "")
                w = int(weakness) if weakness not in ("", None) else None
            except (ValueError, TypeError):
                w = None
            try:
                combined = int(row["combined"]) if row.get("combined") else None
            except (ValueError, TypeError):
                combined = None
            prev[row["phrase"]] = {"weakness": w, "combined": combined}
    return prev, last.name


def load_seeds(args):
    if args.seeds_file:
        path = Path(args.seeds_file)
        lines = path.read_text(encoding="utf-8").splitlines()
        return [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
    if args.seeds:
        return [s.strip() for s in args.seeds.split(",") if s.strip()]
    raise SystemExit("Provide --seeds or --seeds-file")


def process_seed(seed, phrase_rows, reddit_rows, trends_rows, subs, args):
    """Run autocomplete + reddit + trends for one seed, merging into accumulators.

    Always seeds phrase_rows with the user's seed text itself — guarantees the
    seed survives even if autocomplete returns 0 results that minute.
    """
    print("  autocomplete...")
    suggestions = expand_seed(seed, hl=args.hl, gl=args.gl)
    if not suggestions:
        # One retry — autocomplete is occasionally flaky and 1 retry recovers
        # most failures. Costs nothing on the happy path.
        time.sleep(0.5)
        suggestions = expand_seed(seed, hl=args.hl, gl=args.gl)
    # Always inject the seed itself so it can't be dropped from phrase_rows.
    suggestions = set(suggestions) | {seed.lower().strip()}
    print(f"    -> {len(suggestions)} suggestions")
    for s in suggestions:
        entry = phrase_rows[s]
        entry["seeds"].add(seed)
        entry["is_q"] = entry["is_q"] or is_question(s, hl=args.hl)
        entry["wc"] = max(entry["wc"], len(s.split()))

    if subs:
        print("  reddit (search + rising)...")
        posts = reddit_search(seed, subs) + reddit_rising(subs)
        print(f"    -> {len(posts)} posts")
        for p in posts:
            pk = p["permalink"]
            if pk not in reddit_rows:
                reddit_rows[pk] = {**p, "seeds": set()}
            reddit_rows[pk]["seeds"].add(seed)

    trends_hl = f"{args.hl}-419" if args.hl == "es" else args.hl
    print("  trends...")
    tr = trends_related(seed, geo=args.geo, hl=trends_hl)
    print(f"    -> top={len(tr['top'])}, rising={len(tr['rising'])}")
    for bucket in ("top", "rising"):
        for row in tr[bucket]:
            q = row.get("query", "")
            if not q:
                continue
            entry = trends_rows[(bucket, q)]
            entry["seeds"].add(seed)
            entry["value"] = row.get("value", "")


INTENT_ACTION = {
    "how-to": "Write a comprehensive how-to.",
    "what-is": "Write a thorough explainer.",
    "why": "Write a first-principles explainer.",
    "compare": "Write a head-to-head comparison.",
    "list": "Write a ranked list post.",
    "tutorial": "Write a step-by-step tutorial.",
    "review": "Write a hands-on review.",
    "when": "Write a timeline / history piece.",
    "where": "Write a location / directory guide.",
    "location": "Write a local guide — validate local SERP manually first.",
    "price-today": "Skip — SERP refreshed daily by dedicated trackers.",
    "general": "Write a definitive long-form article.",
}


def explain_row(row):
    """Return a list of plain-English reasons this phrase scored well.

    Includes baseline SERP facts (always shown when SERP data exists) plus
    any extreme-condition signals — so the reader sees something concrete
    even on a "neutral" SERP.
    """
    reasons = []
    serp = row["serp"] or {}
    age = serp.get("avg_age_days")
    words = serp.get("avg_words")
    body = serp.get("avg_body_match")
    tm = serp.get("title_match_count")
    auth = serp.get("avg_authority")
    forum = serp.get("forum_count")
    repeat = serp.get("max_domain_repeat")

    # Baseline facts (shown when no extreme signal fires for the same metric)
    baseline = []
    if isinstance(tm, int) and 3 <= tm <= 7:
        baseline.append(f"SERP saturation: {tm}/10 titles match query")
    if isinstance(age, int) and 60 < age < 365:
        baseline.append(f"top pages average {age} days old")
    if isinstance(words, int) and 1000 <= words < 3500:
        baseline.append(f"top pages around {words} avg words (typical depth)")
    if isinstance(auth, (int, float)) and 5.2 < auth < 8:
        baseline.append(f"mixed authority on the SERP (avg DR {auth})")
    if isinstance(body, (int, float)) and 0.6 <= body < 0.95:
        baseline.append(
            f"top pages cover {int(body*100)}% of query terms"
        )

    # Source attribution (helpful for trust)
    src = serp.get("source")
    if src:
        baseline.append(f"SERP source: {src}")

    if isinstance(age, int) and age >= 730:
        reasons.append(f"top pages are {age // 365}+ years stale")
    elif isinstance(age, int) and age >= 365:
        reasons.append(f"top pages are {age} days stale")
    if isinstance(words, int) and words > 0:
        if words < 500:
            reasons.append(f"top pages are very thin ({words} avg words)")
        elif words < 1000:
            reasons.append(f"top pages are thin ({words} avg words)")
        elif words >= 3500:
            reasons.append(f"top pages are comprehensive ({words} avg words — hard)")
    if isinstance(tm, int):
        if tm <= 2:
            reasons.append(f"Google is reaching — only {tm}/10 titles match query")
        elif tm >= 8:
            reasons.append(f"SERP already saturated ({tm}/10 on-target titles)")
    if isinstance(body, (int, float)):
        if body < 0.6:
            reasons.append(
                f"top pages only partially cover query ({int(body*100)}% body match)"
            )
    if isinstance(forum, int) and forum >= 3:
        reasons.append(f"SERP dominated by forums/Q&A ({forum}/10)")
    if isinstance(auth, (int, float)):
        if auth >= 8:
            reasons.append(f"high-authority brands own the SERP (avg DR {auth})")
        elif auth <= 5.2:
            reasons.append(f"no major brand wall (avg DR {auth})")
    if isinstance(repeat, int) and repeat >= 4:
        reasons.append(f"thin SERP — one site appears {repeat} times in top 10")
    if row.get("seed_count", 0) >= 2:
        reasons.append(f"appears under {row['seed_count']} seeds (cross-niche demand)")

    # Append baseline if extreme reasons are sparse
    if len(reasons) < 2:
        reasons.extend(baseline[: max(0, 3 - len(reasons))])

    # Last resort: if there's still no signal (SERP fetch failed entirely),
    # surface the demand-only context so the entry isn't a blank.
    if len(reasons) < 2:
        reasons.append(
            f"demand score {row['demand']} (intent: {row.get('intent', 'general')}, "
            f"{row.get('seed_count', 1)} seed match, {row['wc']} words)"
        )
        reasons.append(
            "no SERP data — DDG/Startpage rate-limited or no organic results"
        )
    if serp.get("has_featured_snippet"):
        reasons.append("featured snippet present — stealable with structured content + schema")
    if serp.get("has_video"):
        reasons.append("video carousel reduces organic real estate")
    if serp.get("has_paa"):
        reasons.append(f"PAA box ({len(serp.get('paa_questions') or [])} adjacent questions) — feed back as seeds")
    plw = row.get("per_locale_weakness")
    if plw and len(plw) > 1:
        best = max(plw.items(), key=lambda kv: kv[1])
        worst = min(plw.items(), key=lambda kv: kv[1])
        if best[1] != worst[1]:
            reasons.append(
                f"locale gap: easiest in es-{best[0].upper()} (weakness {best[1]}) "
                f"vs es-{worst[0].upper()} (weakness {worst[1]})"
            )
    return reasons


def write_seeds_report_markdown(ranked, user_seeds, outpath):
    """Per-seed productivity report — tells the user which seeds earn their keep."""
    def combined(r):
        return r["demand"] + (
            serp_weakness(r["serp"], is_dynamic=r["is_dynamic"]) if r["serp"] else 0
        )

    keys = [s.strip().lower() for s in user_seeds]
    per = {k: {"total": 0, "good": 0, "strong": 0, "scores": [], "top": []}
           for k in keys}

    for r in ranked:
        if r["is_dynamic"]:
            continue
        c = combined(r)
        seed_list = [s.strip() for s in (r.get("seeds") or "").split(";") if s.strip()]
        for s in seed_list:
            if s not in per:
                continue
            per[s]["total"] += 1
            per[s]["scores"].append(c)
            if c >= 6:
                per[s]["good"] += 1
            if c >= 8:
                per[s]["strong"] += 1
            per[s]["top"].append((c, r["phrase"]))

    def verdict_for(stats):
        if stats["total"] == 0:
            return "produced nothing — drop it"
        if stats["good"] == 0:
            return "low-value — consider dropping"
        if stats["strong"] >= 3 or stats["good"] >= 5:
            return "high-value — keep expanding"
        if stats["good"] >= 2:
            return "moderate — useful"
        return "marginal — one opportunity only"

    lines = []
    lines.append(f"# Seed Productivity Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")
    lines.append(
        "Per-seed productivity. A seed's value = how many ≥6-scored phrases it yielded. "
        "Use this to prune seeds for the next run."
    )
    lines.append("")
    lines.append("---")
    lines.append("")

    ranked_seeds = sorted(
        per.items(),
        key=lambda kv: (-kv[1]["good"], -kv[1]["strong"], -kv[1]["total"]),
    )
    for seed, stats in ranked_seeds:
        v = verdict_for(stats)
        avg = (sum(stats["scores"]) / len(stats["scores"])) if stats["scores"] else 0
        lines.append(f"## `{seed}` — {v}")
        lines.append("")
        lines.append(
            f"- **{stats['total']}** total phrases "
            f"| **{stats['good']}** scored ≥6 "
            f"| **{stats['strong']}** scored ≥8 "
            f"| avg score **{avg:.1f}**"
        )
        if stats["top"]:
            top3 = sorted(stats["top"], reverse=True)[:3]
            lines.append("")
            lines.append("**Top phrases from this seed:**")
            for c, p in top3:
                lines.append(f"- ({c}) {p}")
        lines.append("")

    drops = [s for s, st in per.items() if st["good"] == 0]
    keepers = [s for s, st in per.items() if st["good"] >= 2]
    marginals = [s for s, st in per.items() if st["good"] == 1]
    lines.append("---")
    lines.append("")
    lines.append("## Recommendation for next run")
    lines.append("")
    if keepers:
        lines.append("**Keep:**")
        for s in keepers:
            lines.append(f"- `{s}`")
        lines.append("")
    if marginals:
        lines.append("**Keep but watch** (only one hit this run):")
        for s in marginals:
            lines.append(f"- `{s}`")
        lines.append("")
    if drops:
        lines.append("**Consider dropping:**")
        for s in drops:
            lines.append(f"- `{s}`")
        lines.append("")
    if not (keepers or marginals or drops):
        lines.append("(no seeds evaluated yet — run with --check-serp for meaningful scores)")
        lines.append("")

    outpath.write_text("\n".join(lines), encoding="utf-8")


def compute_token_idf(phrase_rows):
    """IDF for every 4+ char token across the phrase corpus.

    Rare tokens get high weight, common tokens (`como`, `venezuela`, `remesas`)
    get low weight. Lets outline ranking pivot on distinctive tokens.
    """
    from math import log
    n_docs = len(phrase_rows)
    if n_docs == 0:
        return {}
    df = defaultdict(int)
    for p in phrase_rows:
        tokens = set(w for w in re.findall(r"\w+", p.lower()) if len(w) >= 4)
        for t in tokens:
            df[t] += 1
    return {t: log((n_docs + 1) / (c + 1)) + 1.0 for t, c in df.items()}


def _weighted_jaccard(a_tokens, b_tokens, idf):
    shared = a_tokens & b_tokens
    union = a_tokens | b_tokens
    if not union:
        return 0.0
    sw = sum(idf.get(t, 1.0) for t in shared)
    uw = sum(idf.get(t, 1.0) for t in union)
    return sw / uw if uw > 0 else 0.0


def generate_outline(best_phrase, cluster, phrase_rows, reddit_rows,
                     hl="es", max_sections=8, token_idf=None,
                     paa_questions=None):
    """Synthesize an H2 outline for a target phrase from in-memory signals:
      - Cluster siblings (same SERP = same article scope)
      - Autocomplete siblings (topical long-tails, ranked by IDF-weighted similarity)
      - Reddit post titles (real reader wording, same weighting)

    Passing `token_idf` (from compute_token_idf) makes sibling ranking prefer
    phrases that share *distinctive* tokens (chile, zoom, banesco) over
    generic connectors (como, enviar, remesas). No new HTTP calls.
    """
    idf = token_idf or {}
    sections = []
    seen = {best_phrase.lower()}
    phrase_tokens = set(w for w in re.findall(r"\w+", best_phrase.lower())
                        if len(w) >= 4)
    # Track which distinctive tokens made primary unique vs. the corpus
    distinctive_primary = sorted(
        phrase_tokens,
        key=lambda t: -idf.get(t, 1.0),
    )[:3]
    distinctive_set = set(distinctive_primary)

    # 0. PAA questions (highest-quality — Google itself says these are adjacent intents)
    for q in (paa_questions or [])[:4]:
        ql = q.lower().strip()
        if ql in seen or not ql:
            continue
        seen.add(ql)
        sections.append(q)

    # 1. Cluster siblings — already proven SERP-equivalent
    if cluster:
        for r in cluster:
            p = r.get("phrase", "")
            pl = p.lower()
            if pl in seen or not p:
                continue
            seen.add(pl)
            sections.append(p)

    # 2. Autocomplete siblings — IDF-weighted similarity, bonus if they share
    #    one of primary's most-distinctive tokens (the "tail" that makes it unique)
    adjacent = []
    for p, entry in phrase_rows.items():
        pl = p.lower()
        if pl in seen:
            continue
        p_tokens = set(w for w in re.findall(r"\w+", pl) if len(w) >= 4)
        if not p_tokens:
            continue
        # Skip pure supersets/substrings of connectors only (no distinctive shared token)
        shared = phrase_tokens & p_tokens
        if len(shared) < 2:
            continue
        if not (distinctive_set & shared):
            continue
        if not is_question(pl, hl):
            continue
        sim = _weighted_jaccard(phrase_tokens, p_tokens, idf)
        adjacent.append((p, sim, entry.get("wc", 0)))
    adjacent.sort(key=lambda x: (-x[1], -x[2]))
    for p, _, _ in adjacent[:5]:
        pl = p.lower()
        if pl in seen or len(sections) >= max_sections:
            continue
        seen.add(pl)
        sections.append(p)

    # 3. Reddit titles — same IDF-weighted ranking, prefer distinctive-token overlap
    reddit_c = []
    for row in reddit_rows.values():
        title = (row.get("title") or "").strip()
        if not title or len(title) > 120:
            continue
        tl = title.lower()
        if tl in seen:
            continue
        t_tokens = set(w for w in re.findall(r"\w+", tl) if len(w) >= 4)
        shared = phrase_tokens & t_tokens
        if len(shared) < 1:
            continue
        if not (is_question(tl, hl) or "?" in title or "como " in tl
                or "cómo " in tl or "por que" in tl or "por qué" in tl):
            continue
        sim = _weighted_jaccard(phrase_tokens, t_tokens, idf)
        # Reddit bonus: mention of a distinctive-primary token gets boosted
        if distinctive_set & shared:
            sim *= 1.5
        reddit_c.append((title, sim, row.get("score", 0)))
    reddit_c.sort(key=lambda x: (-x[1], -x[2]))
    for title, _, _ in reddit_c[:3]:
        tl = title.lower()
        if tl in seen or len(sections) >= max_sections:
            continue
        seen.add(tl)
        sections.append(title)

    return sections[:max_sections]


def cluster_by_serp_overlap(rows, jaccard_threshold=0.4, min_shared=2,
                             token_idf=None, phrase_jaccard_threshold=0.7,
                             max_cluster_size=8):
    """Group phrases whose top-5 SERPs overlap, with phrase-similarity fallback.

    Primary: union-find on DOMAIN-set Jaccard (not URL — same authoritative
    source surfaces across different backends).
    Fallback (when SERP-based clustering produces no merges, common when
    backends throttle and engines disagree): IDF-weighted phrase-token
    Jaccard. Catches near-duplicate paraphrases like "como vender mi casa
    rapido" / "como vender mi casa lo más rápido posible".
    """
    # Always operate on the full input. SERP-overlap clustering naturally
    # skips rows without SERP data; phrase-token clustering then catches
    # paraphrases regardless of SERP availability.
    scored = list(rows)

    n = len(scored)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Stage 1: SERP-based clustering (when domains are present)
    domain_sets = [
        set(r["serp"]["top_domains"].split(";")[:5]) if r.get("serp") else set()
        for r in scored
    ]
    for i in range(n):
        if not domain_sets[i]:
            continue
        for j in range(i + 1, n):
            if not domain_sets[j]:
                continue
            inter = len(domain_sets[i] & domain_sets[j])
            if inter < min_shared:
                continue
            union_size = len(domain_sets[i] | domain_sets[j])
            if inter / union_size >= jaccard_threshold:
                union(i, j)

    # Stage 2: unweighted phrase-token Jaccard fallback. IDF weighting hurts
    # here because destination-paraphrases ("desde usa" vs "desde chile") differ
    # only by a distinctive token that dominates the union under IDF.
    phrase_tokens = [
        set(w for w in re.findall(r"\w+", r["phrase"].lower())
            if len(w) >= 4)
        for r in scored
    ]

    # Track current cluster size to prevent runaway transitive merges
    sizes = [1] * n
    for i in range(n):
        for j in range(i + 1, n):
            ri, rj = find(i), find(j)
            if ri == rj:
                continue  # already in same cluster
            if sizes[ri] + sizes[rj] > max_cluster_size:
                continue  # would exceed cap
            a, b = phrase_tokens[i], phrase_tokens[j]
            if not (a and b):
                continue
            jac = len(a & b) / len(a | b)
            if jac >= phrase_jaccard_threshold:
                # Update size BEFORE union for the next iteration
                new_size = sizes[ri] + sizes[rj]
                union(i, j)
                # The new root might be ri or rj; update both for safety
                sizes[find(i)] = new_size

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(scored[i])
    # Sort clusters: largest (most phrases captured) first, then by best score
    def cluster_key(cl):
        best = max(
            cl,
            key=lambda r: r["demand"] + (
                serp_weakness(r["serp"], is_dynamic=r["is_dynamic"]) if r["serp"] else 0
            ),
        )
        best_score = best["demand"] + (
            serp_weakness(best["serp"], is_dynamic=best["is_dynamic"]) if best["serp"] else 0
        )
        return (-len(cl), -best_score)
    return sorted(groups.values(), key=cluster_key)


def write_shortlist_markdown(ranked, seeds, outpath, top_n=20, stats=None):
    """Render a top-N markdown shortlist with plain-English reasoning per row."""
    def combined_score(r):
        return r["demand"] + (
            serp_weakness(r["serp"], is_dynamic=r["is_dynamic"]) if r["serp"] else 0
        )

    def cluster_best(cluster):
        """Pick cluster representative — prefer entries WITH SERP data."""
        scored_in = [r for r in cluster if r["serp"]]
        if scored_in:
            return max(scored_in, key=combined_score)
        return max(cluster, key=combined_score)

    prev_map = (stats or {}).get("prev_map") or {}

    def diff_tag(row):
        """Return a short tag like 'NEW', 'WEAKENED +3', 'STRENGTHENED -2' or ''."""
        if not prev_map:
            return ""
        p = prev_map.get(row["phrase"])
        if p is None:
            return "NEW"
        if row["serp"] is None or p["weakness"] is None:
            return ""
        delta = serp_weakness(row["serp"], is_dynamic=row["is_dynamic"]) - p["weakness"]
        if delta >= 2:
            return f"WEAKENED +{delta}"
        if delta <= -2:
            return f"STRENGTHENED {delta}"
        return ""

    candidates = [r for r in ranked if not r["is_dynamic"]]
    scored = [r for r in candidates if r["serp"]]
    unscored = [r for r in candidates if not r["serp"]]

    # Cluster from all candidates so paraphrases with no SERP data still merge.
    cluster_input = candidates  # not just scored — phrase fallback runs over all
    clusters = cluster_by_serp_overlap(
        cluster_input, token_idf=(stats or {}).get("token_idf") or {})

    # Keep best N articles (clusters count as 1 article each)
    picked_clusters = clusters[:top_n]
    phrases_in_clusters = {id(r) for cl in picked_clusters for r in cl}
    # Fill remaining slots with unclustered / unscored rows
    remaining_slots = max(0, top_n - len(picked_clusters))
    orphans = [r for r in scored if id(r) not in phrases_in_clusters][:remaining_slots]
    if len(picked_clusters) + len(orphans) < top_n:
        slots = top_n - len(picked_clusters) - len(orphans)
        unscored.sort(key=lambda r: (-r["demand"], -r["wc"], r["phrase"]))
        orphans.extend(unscored[:slots])

    # Audience tagging — used both for diversity preservation AND post-pick
    # injection so the top-N consistently spans buyer/seller/diaspora/financing.
    AUDIENCE_PATTERNS = {
        "diaspora": re.compile(
            r"\b(extranjero|exterior|desde\s+\w+|emigra|diaspora)\b"),
        "financing": re.compile(r"\b(credito|hipotec|banco|prestamo|financ)\b"),
        "sellers": re.compile(r"\b(vender|venta\b)\b"),
        "buyers": re.compile(r"\b(comprar|compra\b)\b"),
    }

    def audiences_of(row):
        p = row["phrase"].lower()
        return {a for a, pat in AUDIENCE_PATTERNS.items() if pat.search(p)}

    def picks_intents():
        s = {cluster_best(c)["intent"] for c in picked_clusters}
        s.update(o["intent"] for o in orphans)
        return s

    def picks_audiences():
        """Audiences covered by the picked set — counts ALL phrases inside
        each cluster (not just the cluster's display title), since clusters
        capture multiple phrases under one article and the audience signal
        survives even if the title differs."""
        s = set()
        for c in picked_clusters:
            for r in c:
                s.update(audiences_of(r))
        for o in orphans:
            s.update(audiences_of(o))
        return s

    def removable_orphans(needed_audiences):
        """Orphans that aren't the sole carrier of a needed audience."""
        out = []
        for o in orphans:
            o_aud = audiences_of(o)
            # Aud's still covered by others if we drop this one?
            others_aud = set()
            for c in picked_clusters:
                others_aud.update(audiences_of(cluster_best(c)))
            for x in orphans:
                if x is o:
                    continue
                others_aud.update(audiences_of(x))
            if (o_aud & needed_audiences) and not (o_aud & others_aud):
                continue  # would drop a unique audience — protect it
            out.append(o)
        return out

    def inject_for_diversity(picks_fn, missing_set, candidate_filter):
        """Add or swap to ensure diversity. Falls back to append-overflow when
        no removable slot exists — lets shortlist exceed top_n by a few entries
        rather than missing a critical diversity dimension."""
        for needed in sorted(missing_set):
            cand = next(
                (r for r in candidates
                 if candidate_filter(r, needed)
                 and r not in orphans
                 and id(r) not in phrases_in_clusters),
                None,
            )
            if cand is None:
                continue
            removable = removable_orphans(target_audiences)
            if removable:
                weakest = min(removable, key=combined_score)
                orphans.remove(weakest)
                orphans.append(cand)
            elif picked_clusters and len(picked_clusters) > 1:
                weakest_cluster_idx = min(
                    range(len(picked_clusters)),
                    key=lambda i: combined_score(cluster_best(picked_clusters[i])),
                )
                picked_clusters.pop(weakest_cluster_idx)
                orphans.append(cand)
            else:
                # Last resort: just append (lets top exceed top_n slightly)
                orphans.append(cand)

    # Step 1: intent diversity injection
    all_intents = {r["intent"] for r in candidates if r.get("intent")}
    target_audiences = {a for r in candidates for a in audiences_of(r)}
    if len(picks_intents()) < 3 and len(all_intents) >= 3:
        missing_intents = all_intents - picks_intents()
        inject_for_diversity(
            picks_intents, missing_intents,
            lambda r, intent: r["intent"] == intent,
        )

    # Step 2: audience injection
    if target_audiences:
        missing_aud = target_audiences - picks_audiences()
        if missing_aud:
            inject_for_diversity(
                picks_audiences, missing_aud,
                lambda r, aud: aud in audiences_of(r),
            )

    lines = []
    lines.append(f"# Blog Topic Shortlist — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    # TL;DR — headline answer in 5-7 lines, before the detail
    new_count = sum(
        1 for c in picked_clusters
        for r in c if r["phrase"] not in prev_map
    ) + sum(1 for r in orphans if r["phrase"] not in prev_map) if prev_map else 0

    weakened_count = 0
    if prev_map:
        for c in picked_clusters:
            for r in c:
                if r["phrase"] not in prev_map or not r["serp"]:
                    continue
                p = prev_map[r["phrase"]]
                if p["weakness"] is None:
                    continue
                delta = serp_weakness(r["serp"], is_dynamic=r["is_dynamic"]) - p["weakness"]
                if delta >= 2:
                    weakened_count += 1

    lines.append("## TL;DR")
    lines.append("")
    status_msg = (stats or {}).get("status_msg")
    if status_msg:
        lines.append(f"- **Pipeline status**: {status_msg}")
    if picked_clusters:
        top_cluster = picked_clusters[0]
        top_phrase = max(top_cluster, key=combined_score)
        lines.append(
            f"- **Write this first**: `{top_phrase['phrase']}` "
            f"(score **{combined_score(top_phrase)}**, "
            f"intent: {top_phrase['intent']})"
        )
    elif orphans:
        top_phrase = orphans[0]
        lines.append(
            f"- **Write this first**: `{top_phrase['phrase']}` "
            f"(score **{combined_score(top_phrase)}**, "
            f"intent: {top_phrase['intent']})"
        )
    total_articles = len(picked_clusters) + len(orphans)
    captured_phrases = sum(len(c) for c in picked_clusters) + len(orphans)
    lines.append(
        f"- **Articles to write**: {total_articles} (capturing {captured_phrases} phrases)"
    )
    if prev_map:
        if new_count:
            lines.append(f"- **NEW since last run**: {new_count} phrase(s)")
        if weakened_count:
            lines.append(
                f"- **WEAKENED since last run**: {weakened_count} phrase(s) "
                "got easier to rank"
            )
        if not new_count and not weakened_count:
            lines.append("- **Diff**: no new openings since last run")
    lines.append("")

    if stats:
        lines.append(
            f"From **{stats['n_seeds']}** user seed(s), "
            f"**{stats.get('rounds', 1)}** round(s), "
            f"**{stats['n_phrases']}** unique phrases analyzed."
        )
    lines.append(
        f"Showing **{total_articles}** articles "
        f"capturing **{captured_phrases}** phrases "
        f"(SERP-overlap clustering groups phrases targetable by one article)."
    )
    lines.append("")
    lines.append(f"Source seeds: {', '.join(seeds)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    phrase_rows_ref = (stats or {}).get("phrase_rows") or {}
    reddit_rows_ref = (stats or {}).get("reddit_rows") or {}
    token_idf = (stats or {}).get("token_idf") or {}
    hl = (stats or {}).get("hl", "es")

    def render_row(best, cluster=None):
        serp = best["serp"] or {}
        combined = combined_score(best)
        reasons = explain_row(best)
        action = INTENT_ACTION.get(best["intent"], INTENT_ACTION["general"])
        tag = diff_tag(best)
        out = []
        title_prefix = f"`{tag}` " if tag else ""
        out.append(f"### {title_prefix}{best['phrase']}")
        out.append("")
        bits = [
            f"**Score: {combined}**",
            f"intent: `{best['intent']}`",
        ]
        if cluster and len(cluster) > 1:
            bits.append(f"captures **{len(cluster)}** phrases")
        out.append(" | ".join(bits))
        out.append("")
        if cluster and len(cluster) > 1:
            others = [r for r in cluster if r["phrase"] != best["phrase"]]
            others.sort(key=lambda r: -combined_score(r))
            out.append("**Also captures** (same SERP cluster):")
            for r in others:
                out.append(f"- {r['phrase']}  (score {combined_score(r)})")
            out.append("")
        if reasons:
            out.append("**SERP signals:**")
            for r in reasons:
                out.append(f"- {r}")
            out.append("")
        if serp.get("top_domains"):
            out.append(f"**Top-5 competitors**: {serp['top_domains']}")
            out.append("")
        out.append(f"**Action**: {action}")
        out.append("")
        outline = generate_outline(
            best["phrase"], cluster or [],
            phrase_rows_ref, reddit_rows_ref, hl=hl,
            token_idf=token_idf,
            paa_questions=(serp.get("paa_questions") if serp else None),
        )
        if outline:
            out.append("<details><summary><b>Suggested outline</b> "
                       f"({len(outline)} sections)</summary>")
            out.append("")
            for s in outline:
                out.append(f"- {s}")
            out.append("")
            out.append("</details>")
            out.append("")
        return out

    for i, cluster in enumerate(picked_clusters, 1):
        best = cluster_best(cluster)
        lines.append(f"## {i}.")
        lines.extend(render_row(best, cluster=cluster))

    for j, row in enumerate(orphans, len(picked_clusters) + 1):
        lines.append(f"## {j}.")
        lines.extend(render_row(row))

    # Grouped-by-intent index at the end — helps content calendar planning
    by_intent = defaultdict(list)
    picked_bests = [max(c, key=combined_score) for c in picked_clusters] + orphans
    for row in picked_bests:
        by_intent[row["intent"]].append(row["phrase"])
    if len(by_intent) > 1:
        lines.append("---")
        lines.append("")
        lines.append("## By intent (content calendar view)")
        lines.append("")
        for intent in sorted(by_intent, key=lambda k: -len(by_intent[k])):
            lines.append(f"### {intent}  ({len(by_intent[intent])})")
            for p in by_intent[intent]:
                lines.append(f"- {p}")
            lines.append("")

    outpath.write_text("\n".join(lines), encoding="utf-8")


# Tokens we ignore when measuring "topical relevance" for harvest. They're
# too common in Spanish/English queries to discriminate meaningfully.
_HARVEST_STOPTOKENS = {
    "venezuela", "como", "para", "esta", "estas", "este", "estos",
    "the", "and", "what", "with", "from", "about", "into",
    "casa", "país", "venezolano", "venezolanos",
}


def harvest_new_seeds(trends_rows, reddit_rows, original_seeds, used_seeds,
                      limit=8, hl="es"):
    """Pick promising unused seeds from Trends rising + Reddit titles.

    Trends rising is preferred (high demand signal, search-shaped queries).
    Reddit titles are a fallback ONLY when Trends provides too few — and even
    then are filtered hard:
      - Must share >=2 distinctive (non-stopword) tokens with original seeds
      - Must look like a search query, not a rant/joke
      - Dynamic intents excluded
    """
    distinctive_originals = set()
    for s in original_seeds:
        for w in re.findall(r"\w+", s.lower()):
            if len(w) >= 4 and w not in _HARVEST_STOPTOKENS:
                distinctive_originals.add(w)

    trends_picks = []
    reddit_picks = []

    # 1. Trends rising queries — high quality, search-native phrasing
    for (bucket, q), entry in trends_rows.items():
        if bucket != "rising":
            continue
        ql = q.lower().strip()
        if ql in used_seeds:
            continue
        if len(ql.split()) < 3 or len(ql.split()) > 10:
            continue
        _, is_dyn = classify_intent(ql)
        if is_dyn:
            continue
        value = entry.get("value", 0)
        try:
            v = int(value) if value else 0
        except (TypeError, ValueError):
            v = 0
        trends_picks.append((ql, v, "trends_rising"))

    trends_picks.sort(key=lambda c: -c[1])

    # 2. Reddit titles — STRICT filter (only fallback)
    for row in reddit_rows.values():
        title = row.get("title", "").strip()
        if not title:
            continue
        tl = title.lower()
        if tl in used_seeds:
            continue
        n_words = len(tl.split())
        if n_words < 3 or n_words > 10:
            continue
        title_tokens = set(w for w in re.findall(r"\w+", tl)
                           if len(w) >= 4 and w not in _HARVEST_STOPTOKENS)
        # Require >=2 DISTINCTIVE shared tokens (not just "venezuela")
        distinctive_overlap = len(title_tokens & distinctive_originals)
        if distinctive_overlap < 2:
            continue
        if not (is_question(tl, hl=hl) or "?" in tl):
            continue
        _, is_dyn = classify_intent(tl)
        if is_dyn:
            continue
        score = row.get("score", 0) + distinctive_overlap * 5
        reddit_picks.append((tl, score, "reddit_title"))

    reddit_picks.sort(key=lambda c: -c[1])

    # Combine: Trends first (always preferred), Reddit only fills remaining slots.
    picked, seen = [], set()
    for source_list in (trends_picks, reddit_picks):
        for text, _score, source in source_list:
            if text in seen:
                continue
            seen.add(text)
            picked.append((text, source))
            if len(picked) >= limit:
                return picked
    return picked


def main():
    ap = argparse.ArgumentParser(
        description="Batch discovery across multiple seeds.")
    ap.add_argument("--seeds", help="Comma-separated list of seed topics.")
    ap.add_argument("--seeds-file", help="Text file, one seed per line.")
    ap.add_argument("--subreddits", default="", help="Subreddits mined for every seed.")
    ap.add_argument("--hl", default="es")
    ap.add_argument("--gl", default="ve")
    ap.add_argument("--geo", default="VE")
    ap.add_argument("--output", default="output")
    ap.add_argument("--check-serp", type=int, default=0,
                    help="After merging, fetch DDG SERP for top N phrases.")
    ap.add_argument("--check-freshness", action="store_true",
                    help="Additionally fetch top-3 result URLs to extract page ages.")
    ap.add_argument("--recurse", type=int, default=0,
                    help="After round 1, harvest Trends-rising + Reddit titles "
                         "as new seeds and run N additional rounds.")
    ap.add_argument("--recurse-limit", type=int, default=8,
                    help="Max new seeds harvested per recursion round.")
    ap.add_argument("--shortlist", type=int, default=20,
                    help="Write a ranked markdown shortlist of top N non-dynamic "
                         "candidates with plain-English reasoning. 0 to disable.")
    ap.add_argument("--locales", default="",
                    help="Comma-separated locales to check each SERP across "
                         "(e.g. 've,co,us,es'). Pick the weakest (easiest-to-rank) "
                         "for each phrase. Multiplies SERP requests by N locales.")
    ap.add_argument("--cache-ttl-days", type=int, default=7,
                    help="SERP cache TTL in days (0 disables cache).")
    ap.add_argument("--force-refresh", action="store_true",
                    help="Ignore SERP cache and refetch all.")
    ap.add_argument("--diff-last", action="store_true",
                    help="Compare shortlist against the previous run's "
                         "autocomplete CSV and flag NEW / CHANGED entries.")
    ap.add_argument("--workers", type=int, default=1,
                    help="Parallel phrase workers during SERP phase. Combined "
                         "with --rate-limit for safety. Default 1 (serial).")
    ap.add_argument("--rate-limit", type=float, default=3.0,
                    help="Max SERP requests per second across all workers "
                         "(default 3.0).")
    args = ap.parse_args()

    seeds = load_seeds(args)
    subs = [s.strip() for s in args.subreddits.split(",") if s.strip()]
    outdir = Path(args.output)
    outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    reset_serp_stats()

    # Load disk caches (no-op if --force-refresh or files missing)
    if args.cache_ttl_days > 0 and not args.force_refresh:
        load_autocomplete_cache(outdir / "autocomplete_cache.json",
                                ttl_days=args.cache_ttl_days)
        load_trends_cache(outdir / "trends_cache.json",
                          ttl_days=args.cache_ttl_days)
        load_reddit_cache(outdir / "reddit_cache.json", ttl_days=2)

    phrase_rows = defaultdict(lambda: {"seeds": set(), "is_q": False, "wc": 0})
    reddit_rows = {}
    trends_rows = defaultdict(lambda: {"seeds": set(), "value": ""})

    used_seeds = set()
    round_seeds = [s.strip().lower() for s in seeds]
    rounds_done = 0
    expand_phase_start = time.time()

    for round_num in range(args.recurse + 1):
        if not round_seeds:
            break
        label = "round 1 (user seeds)" if round_num == 0 else \
                f"round {round_num + 1} (harvested)"
        print(f"\n=== {label}: {len(round_seeds)} seed(s) ===")
        for i, seed in enumerate(round_seeds, 1):
            if seed in used_seeds:
                continue
            used_seeds.add(seed)
            print(f"\n[{label} seed {i}/{len(round_seeds)}] {seed}")
            process_seed(seed, phrase_rows, reddit_rows, trends_rows, subs, args)
            # Removed inter-seed sleep — rate-limiter and per-API caches handle
            # pacing. Was costing ~8s per run with no benefit.
        rounds_done = round_num + 1

        if round_num < args.recurse:
            harvested = harvest_new_seeds(
                trends_rows, reddit_rows,
                original_seeds=seeds,
                used_seeds=used_seeds,
                limit=args.recurse_limit,
                hl=args.hl,
            )
            if not harvested:
                print("\n(no adjacent seeds worth harvesting; stopping recursion)")
                break
            print(f"\nHarvested {len(harvested)} adjacent seed(s) for next round:")
            for t, source in harvested:
                print(f"  + [{source}] {t}")
            round_seeds = [t for t, _ in harvested]
        else:
            round_seeds = []

    expand_phase_start_to_serp = time.time() - expand_phase_start
    serp_phase_elapsed = 0.0
    output_phase_start = time.time()

    # Pre-compute Reddit topic signal: which phrases overlap with high-engagement
    # Reddit titles. Used to nudge demand for phrases the community is
    # actually discussing, so demand-only ranking still discriminates.
    reddit_topic_tokens = defaultdict(int)  # token -> total upvote score
    for r in reddit_rows.values():
        title = (r.get("title") or "").lower()
        score = r.get("score") or 0
        if score < 5 or len(title) > 200:
            continue
        for t in re.findall(r"\w+", title):
            if len(t) >= 5:
                reddit_topic_tokens[t] += score

    def reddit_resonance(phrase):
        """Sum of community upvotes on titles sharing 4+ char tokens."""
        toks = set(w for w in re.findall(r"\w+", phrase.lower()) if len(w) >= 5)
        return sum(reddit_topic_tokens[t] for t in toks
                   if t in reddit_topic_tokens)

    ranked = []
    for phrase, entry in phrase_rows.items():
        seed_count = len(entry["seeds"])
        base = score_phrase(phrase, entry["is_q"], entry["wc"])
        # Cross-seed boost (heavier than before — strong signal)
        cross_seed_bonus = (seed_count - 1) * 2
        # Reddit resonance: tier bucketed so the bonus is bounded
        rr = reddit_resonance(phrase)
        if rr >= 1000:
            reddit_bonus = 3
        elif rr >= 200:
            reddit_bonus = 2
        elif rr >= 30:
            reddit_bonus = 1
        else:
            reddit_bonus = 0
        demand = base + cross_seed_bonus + reddit_bonus
        intent, is_dyn = classify_intent(phrase)
        ranked.append({
            "phrase": phrase, "is_q": entry["is_q"], "wc": entry["wc"],
            "intent": intent, "is_dynamic": is_dyn,
            "seed_count": seed_count,
            "seeds": ";".join(sorted(entry["seeds"])),
            "demand": demand,
            "reddit_resonance": rr,
            "serp": None,
        })
    # Push dynamic-intent phrases down so SERP-check budget hits winnable ones.
    # Final tiebreak on phrase string for run-to-run determinism — without it,
    # tied entries shuffle each run and the top-N drifts.
    ranked.sort(key=lambda r: (r["is_dynamic"], -r["demand"],
                               -r["seed_count"], -r["wc"], r["phrase"]))

    cache = None
    if args.cache_ttl_days > 0 and not args.force_refresh:
        cache = SerpCache(outdir / "serp_cache.json",
                          ttl_days=args.cache_ttl_days)

    if args.check_serp > 0:
        n = min(args.check_serp, len(ranked))
        locales = [l.strip() for l in args.locales.split(",") if l.strip()]
        locale_note = f" across {len(locales)} locales" if locales else ""
        extra = " + freshness" if args.check_freshness else ""
        cache_note = " (cache on)" if cache else " (cache off)"
        parallel_note = (f" [workers={args.workers}, "
                         f"rate<={args.rate_limit:.1f}/s]"
                         if args.workers > 1 else "")
        print(f"\nChecking DuckDuckGo SERP for top {n} merged phrases"
              f"{locale_note}{extra}{cache_note}{parallel_note}...")
        start = time.time()
        rate_limiter = RateLimiter(per_second=args.rate_limit)

        if args.workers > 1:
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futures = {
                    ex.submit(
                        fetch_phrase_serp, row, locales, cache,
                        args.check_freshness, rate_limiter,
                    ): row
                    for row in ranked[:n]
                }
                done = 0
                for fut in as_completed(futures):
                    row = futures[fut]
                    done += 1
                    try:
                        fut.result()
                    except Exception as e:
                        print(f"  [{done}/{n}] ERROR on '{row['phrase']}': {e}")
                        continue
                    print(f"  [{done}/{n}] {row['phrase']}")
        else:
            for i, row in enumerate(ranked[:n], 1):
                print(f"  [{i}/{n}] {row['phrase']}")
                fetch_phrase_serp(
                    row, locales, cache, args.check_freshness, rate_limiter,
                )

        elapsed = time.time() - start
        serp_phase_elapsed = elapsed
        if cache is not None:
            cache_stats = (
                f", cache hits={cache.hits}, misses={cache.misses}, "
                f"skipped_known_failures={cache.skipped_failures}"
            )
        else:
            cache_stats = ""
        print(f"  SERP phase: {elapsed:.1f}s total "
              f"({elapsed/max(1, n):.2f}s avg/phrase{cache_stats})")
        if cache is not None:
            cache.flush()
        output_phase_start = time.time()

    ac_path = outdir / f"batch_autocomplete_{ts}.csv"
    with open(ac_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phrase", "intent", "is_dynamic",
                    "is_question", "word_count", "seed_count",
                    "seeds", "demand",
                    "serp_best_locale", "serp_per_locale_weakness",
                    "serp_forum_count", "serp_title_match", "serp_max_repeat",
                    "serp_top_domain", "serp_top_domains",
                    "serp_avg_authority", "serp_max_authority",
                    "serp_avg_age_days", "serp_fresh_count",
                    "serp_avg_words", "serp_min_words",
                    "serp_avg_body_match", "serp_min_body_match",
                    "serp_weakness", "combined"])
        rows = []
        for r in ranked:
            serp = r["serp"] or {}
            if r["serp"]:
                weakness = serp_weakness(r["serp"], is_dynamic=r["is_dynamic"])
                combined = r["demand"] + weakness
            else:
                weakness = -3 if r["is_dynamic"] else ""
                combined = r["demand"] + (weakness if isinstance(weakness, int) else 0)
            plw = r.get("per_locale_weakness", {})
            plw_str = ";".join(f"{k}:{v}" for k, v in plw.items()) if plw else ""
            rows.append([
                r["phrase"], r["intent"], r["is_dynamic"],
                r["is_q"], r["wc"], r["seed_count"], r["seeds"],
                r["demand"],
                r.get("best_locale", ""), plw_str,
                serp.get("forum_count", ""),
                serp.get("title_match_count", ""),
                serp.get("max_domain_repeat", ""),
                serp.get("top_domain", ""),
                serp.get("top_domains", ""),
                serp.get("avg_authority", ""),
                serp.get("max_authority", ""),
                serp.get("avg_age_days", ""),
                serp.get("fresh_count", ""),
                serp.get("avg_words", ""),
                serp.get("min_words", ""),
                serp.get("avg_body_match", ""),
                serp.get("min_body_match", ""),
                weakness, combined,
            ])
        rows.sort(
            key=lambda r: (-(r[24] if isinstance(r[24], (int, float)) else r[7]),
                           -r[5], -r[4]),
        )
        for r in rows:
            w.writerow(r)

    rd_path = outdir / f"batch_reddit_{ts}.csv"
    with open(rd_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "subreddit", "score", "num_comments", "permalink",
                    "source", "seed_count", "seeds"])
        for p in sorted(reddit_rows.values(), key=lambda p: -p["score"]):
            w.writerow([p["title"], p["subreddit"], p["score"], p["num_comments"],
                        p["permalink"], p["source"], len(p["seeds"]),
                        ";".join(sorted(p["seeds"]))])

    tr_path = outdir / f"batch_trends_{ts}.csv"
    with open(tr_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "query", "value", "seed_count", "seeds"])
        rows = []
        for (bucket, q), entry in trends_rows.items():
            rows.append((bucket, q, entry["value"], len(entry["seeds"]),
                         ";".join(sorted(entry["seeds"]))))
        rows.sort(key=lambda r: (r[0] != "rising", -r[3]))
        for r in rows:
            w.writerow(r)

    prev_map, prev_name = {}, None
    if args.diff_last:
        prev_map, prev_name = load_last_autocomplete(outdir / "history")
        if prev_name:
            print(f"\nComparing against previous run: {prev_name}")

    # Archive this run's autocomplete CSV into history for future diffs.
    history_dir = outdir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ac_path, history_dir / ac_path.name)

    sl_path = None
    if args.shortlist > 0:
        sl_path = outdir / f"batch_shortlist_{ts}.md"
        token_idf = compute_token_idf(phrase_rows.keys())
        status_kind, status_msg = build_status_summary(
            SERP_STATS, n_serp_checks_attempted=args.check_serp
        )
        write_shortlist_markdown(
            ranked, seeds, sl_path, top_n=args.shortlist,
            stats={
                "n_seeds": len(seeds),
                "rounds": rounds_done,
                "n_phrases": len(phrase_rows),
                "prev_map": prev_map,
                "prev_name": prev_name,
                "phrase_rows": phrase_rows,
                "reddit_rows": reddit_rows,
                "token_idf": token_idf,
                "hl": args.hl,
                "status_kind": status_kind,
                "status_msg": status_msg,
            },
        )

    sr_path = outdir / f"batch_seeds_report_{ts}.md"
    write_seeds_report_markdown(ranked, seeds, sr_path)

    print()
    print("=" * 60)
    print(f"BATCH SUMMARY — {len(seeds)} seeds")
    print("=" * 60)
    print(f"Unique autocomplete phrases: {len(phrase_rows)}")
    print(f"Unique reddit posts:         {len(reddit_rows)}")
    print(f"Unique trends queries:       {len(trends_rows)}")

    print()
    print("Cross-seed question phrases (appear under 2+ seeds):")
    cross = sorted(
        ((p, e) for p, e in phrase_rows.items() if len(e["seeds"]) >= 2 and e["is_q"]),
        key=lambda kv: (-len(kv[1]["seeds"]), -kv[1]["wc"]),
    )
    if not cross:
        print("  (none — try related seeds to surface overlap)")
    for phrase, entry in cross[:20]:
        print(f"  [{len(entry['seeds'])}x] ? {phrase}")

    print()
    print("Top 15 question long-tails overall:")
    ranked = sorted(
        ((p, e) for p, e in phrase_rows.items() if e["is_q"]),
        key=lambda kv: (-kv[1]["wc"], -len(kv[1]["seeds"])),
    )
    for phrase, entry in ranked[:15]:
        print(f"  ? {phrase}  (from: {', '.join(sorted(entry['seeds']))})")

    print()
    rising = [(q, e) for (b, q), e in trends_rows.items() if b == "rising"]
    if rising:
        print("Trends RISING across batch:")
        for q, e in sorted(rising, key=lambda x: -len(x[1]["seeds"]))[:10]:
            print(f"  ^ {q}  (+{e['value']})  (seeds: {', '.join(sorted(e['seeds']))})")

    status_kind, status_msg = build_status_summary(
        SERP_STATS, n_serp_checks_attempted=args.check_serp
    )

    # Persist disk caches
    if args.cache_ttl_days > 0:
        flush_autocomplete_cache(outdir / "autocomplete_cache.json")
        flush_trends_cache(outdir / "trends_cache.json")
        flush_reddit_cache(outdir / "reddit_cache.json")

    print()
    print(f"Pipeline status: {status_msg}")
    print(f"Phase timing: expansion={expand_phase_start_to_serp:.1f}s "
          f"serp={serp_phase_elapsed:.1f}s "
          f"output={time.time() - output_phase_start:.1f}s")
    print()
    print(f"Output written to: {outdir}/")
    print(f"  - {ac_path.name}")
    print(f"  - {rd_path.name}")
    print(f"  - {tr_path.name}")
    if sl_path:
        print(f"  - {sl_path.name}  <-- ranked shortlist, read this first")
    print(f"  - {sr_path.name}  <-- seed productivity report")


if __name__ == "__main__":
    main()
