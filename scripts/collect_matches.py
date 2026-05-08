"""
Collects ~200k matches from the deadlock-api.com bulk metadata endpoint.

Sampling strategy: weekly stratification across Aug 2025 – Apr 2026 (the full
Mina era). The full period is divided into ~34 one-week windows and ~6,000
matches are taken from the start of each window. This ensures even patch
coverage and a consistent temporal basis for the 14-day rolling averages used
in the secondary analysis, avoiding the bias of a pure start-of-month sample.

Match ID → date mapping derived from public_player_match_history.parquet.

Output: data/collected_matches.jsonl  (one JSON object per line, one match per line)

Usage:
    python3 collect_matches.py
    python3 collect_matches.py --api-key YOUR_KEY_HERE
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse

API_BASE    = "https://api.deadlock-api.com"
OUTPUT_FILE = "data/collected_matches.jsonl"
USER_AGENT  = "Deadlock Counter Item Study Dataset Requests (@gohomecookrice)"

BATCH_SIZE      = 1_000  # per request (server 500s above ~3k with player items included)
TARGET_PER_WEEK = 6_000  # ~204k total across 34 weeks, ~6 requests per week

# Weekly match ID boundaries derived from public_player_match_history.parquet.
# Each tuple: (label, week_start_id, week_end_id)
# Ranges were computed by interpolating monthly min/max IDs across the Mina era
# (Aug 2025 – Apr 2026). Each window covers ~7 days of match activity.
WEEK_WINDOWS = [
    # August 2025
    ("2025-W32", 38_408_619, 38_956_500),
    ("2025-W33", 38_956_501, 39_504_382),
    ("2025-W34", 39_504_383, 40_052_264),
    ("2025-W35", 40_052_265, 40_611_632),
    # September 2025
    ("2025-W36", 40_611_634, 41_404_909),
    ("2025-W37", 41_404_910, 42_198_185),
    ("2025-W38", 42_198_186, 42_991_461),
    ("2025-W39", 42_991_462, 43_779_832),
    # October 2025
    ("2025-W40", 43_779_833, 44_409_896),
    ("2025-W41", 44_409_897, 45_039_960),
    ("2025-W42", 45_039_961, 45_669_793),
    ("2025-W43", 45_669_794, 46_299_627),
    # November 2025
    ("2025-W44", 46_299_638, 46_724_879),
    ("2025-W45", 46_724_880, 47_150_121),
    ("2025-W46", 47_150_122, 47_675_083),
    ("2025-W47", 47_675_084, 48_200_047),
    # December 2025
    ("2025-W48", 48_200_048, 48_651_271),
    ("2025-W49", 48_651_272, 49_102_495),
    ("2025-W50", 49_102_496, 49_555_710),
    ("2025-W51", 49_555_711, 50_008_927),
    # January 2026
    ("2026-W01", 50_008_931, 51_039_637),
    ("2026-W02", 51_039_638, 52_070_344),
    ("2026-W03", 52_070_345, 53_101_051),
    ("2026-W04", 53_101_052, 54_144_829),
    # February 2026
    ("2026-W05", 54_144_839, 56_445_934),
    ("2026-W06", 56_445_935, 58_746_030),
    ("2026-W07", 58_746_031, 61_047_126),
    ("2026-W08", 61_047_127, 63_188_639),
    # March 2026
    ("2026-W09", 63_188_643, 65_490_120),
    ("2026-W10", 65_490_121, 67_791_598),
    ("2026-W11", 67_791_599, 70_002_386),
    ("2026-W12", 70_002_387, 72_213_175),
    # April 2026 (partial)
    ("2026-W13", 72_213_176, 73_196_674),
    ("2026-W14", 73_196_675, 74_721_873),
]


def fetch_batch(min_match_id, max_match_id, api_key=None):
    params = urllib.parse.urlencode({
        "include_player_info": "true",
        "include_player_items": "true",
        "include_player_kda": "true",
        "game_mode": "normal",
        "min_match_id": min_match_id,
        "max_match_id": max_match_id,
        "order_by": "match_id",
        "order_direction": "asc",
        "limit": BATCH_SIZE,
    }) + "&match_mode=ranked,unranked"
    url = f"{API_BASE}/v1/matches/metadata?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if api_key:
        req.add_header("Authorization", api_key)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def load_seen_weeks():
    """Returns set of week labels already fully collected."""
    seen = set()
    progress_file = OUTPUT_FILE + ".progress"
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            seen = set(line.strip() for line in f if line.strip())
    return seen


def mark_week_done(week_label):
    with open(OUTPUT_FILE + ".progress", "a") as f:
        f.write(week_label + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-key", default=None, help="deadlock-api.com API key")
    args = parser.parse_args()

    # Stay comfortably under the rate limit in both modes.
    # No key: limit is 10 req/min → wait 8s (75% of limit)
    # With key: limit is 10 req/10s → wait 1.5s (75% of limit)
    delay = 1.5 if args.api_key else 8.0

    os.makedirs("data", exist_ok=True)
    done_weeks = load_seen_weeks()
    remaining  = [w for w in WEEK_WINDOWS if w[0] not in done_weeks]

    print(f"Weeks complete: {len(done_weeks)}/{len(WEEK_WINDOWS)}")
    print(f"Weeks remaining: {len(remaining)}")
    print(f"Estimated requests: {len(remaining)}  |  delay: {delay}s  |  ETA: ~{len(remaining) * delay / 60:.1f} min\n")

    with open(OUTPUT_FILE, "a") as out:
        for week_label, id_min, id_max in remaining:
            collected = 0
            cursor = id_min
            print(f"[{week_label}] id range {id_min:,} — {id_max:,}")

            while collected < TARGET_PER_WEEK and cursor <= id_max:
                retries = 0
                batch = []
                while True:
                    try:
                        batch = fetch_batch(cursor, id_max, args.api_key)
                        break
                    except urllib.error.HTTPError as e:
                        if e.code == 429:
                            wait = int(e.headers.get("Retry-After", 30))
                            print(f"  rate limited — waiting {wait}s")
                            time.sleep(wait)
                        else:
                            retries += 1
                            if retries > 3:
                                print(f"  ERROR {e.code} after 3 retries — skipping rest of week")
                                break
                            time.sleep(10 * retries)
                    except Exception as e:
                        retries += 1
                        if retries > 3:
                            print(f"  ERROR after 3 retries ({e}) — skipping rest of week")
                            break
                        time.sleep(10 * retries)

                if not batch:
                    break

                for match in batch:
                    out.write(json.dumps(match) + "\n")
                out.flush()

                collected += len(batch)
                cursor = batch[-1]["match_id"] + 1
                print(f"  {len(batch):,} fetched  week_total={collected:,}  cursor={cursor:,}")
                time.sleep(delay)

            mark_week_done(week_label)
            print(f"  [{week_label}] done — {collected:,} matches\n")

    # Final count
    total = sum(1 for _ in open(OUTPUT_FILE))
    print(f"\nDone. {total:,} matches written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
