"""
Fills in the gap weeks visible in the temporal coverage chart (~W39–W42, late
Sep / early Oct 2025) where data appears sparse or missing.  Output goes to a
separate file so it can be inspected before merging with the main dataset.

Usage:
    python3 collect_matches_gap.py
    python3 collect_matches_gap.py --api-key YOUR_KEY_HERE
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse

API_BASE    = "https://api.deadlock-api.com"
OUTPUT_FILE = "data/collected_matches_gap.jsonl"
USER_AGENT  = "Deadlock Counter Item Study Dataset Requests (@gohomecookrice)"

BATCH_SIZE      = 1_000
TARGET_PER_WEEK = 6_000

# Only the weeks that appear low/missing in the coverage chart.
# Extend this list if the gap turns out to be wider once you inspect the output.
GAP_WINDOWS = [
    ("2025-W46", 47_150_122, 47_675_083),
]


def fetch_batch(min_match_id, max_match_id, api_key=None):
    params = urllib.parse.urlencode({
        "include_player_info":  "true",
        "include_player_items": "true",
        "include_player_kda":   "true",
        "game_mode":            "normal",
        "min_match_id":         min_match_id,
        "max_match_id":         max_match_id,
        "order_by":             "match_id",
        "order_direction":      "asc",
        "limit":                BATCH_SIZE,
    }) + "&match_mode=ranked,unranked"
    url = f"{API_BASE}/v1/matches/metadata?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if api_key:
        req.add_header("Authorization", api_key)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def load_done():
    progress_file = OUTPUT_FILE + ".progress"
    if not os.path.exists(progress_file):
        return set()
    with open(progress_file) as f:
        return set(line.strip() for line in f if line.strip())


def mark_done(week_label):
    with open(OUTPUT_FILE + ".progress", "a") as f:
        f.write(week_label + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args()

    delay = 1.5 if args.api_key else 8.0

    os.makedirs("data", exist_ok=True)
    done     = load_done()
    pending  = [w for w in GAP_WINDOWS if w[0] not in done]

    print(f"Gap weeks to collect : {len(GAP_WINDOWS)}")
    print(f"Already done         : {len(done)}")
    print(f"Remaining            : {len(pending)}  |  delay: {delay}s\n")

    with open(OUTPUT_FILE, "a") as out:
        for week_label, id_min, id_max in pending:
            collected = 0
            cursor    = id_min
            print(f"[{week_label}] id range {id_min:,} — {id_max:,}")

            while collected < TARGET_PER_WEEK and cursor <= id_max:
                retries = 0
                batch   = []
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
                                print(f"  ERROR {e.code} after 3 retries — skipping week")
                                break
                            time.sleep(10 * retries)
                    except Exception as e:
                        retries += 1
                        if retries > 3:
                            print(f"  ERROR after 3 retries ({e}) — skipping week")
                            break
                        time.sleep(10 * retries)

                if not batch:
                    break

                for match in batch:
                    out.write(json.dumps(match) + "\n")
                out.flush()

                collected += len(batch)
                cursor     = batch[-1]["match_id"] + 1
                print(f"  {len(batch):,} fetched  week_total={collected:,}  cursor={cursor:,}")
                time.sleep(delay)

            mark_done(week_label)
            print(f"  [{week_label}] done — {collected:,} matches\n")

    total = sum(1 for _ in open(OUTPUT_FILE))
    print(f"\nDone. {total:,} matches written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
