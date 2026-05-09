"""
Collects the most recent N matches for a given rank band.

Pulls backward from the latest available match, so the output reflects the
current meta rather than a stratified historical sample. Intended for the
all-hero volcano plot analysis.

Pass --rank high to collect only high-rank matches (avg_badge ≥ 100 both
teams). Pass --rank low for low-rank (avg_badge < 100). Default: high.

Output: data/collected_matches_recent_{rank}.jsonl

Usage:
    python3 collect_matches_recent.py --rank high
    python3 collect_matches_recent.py --rank low --api-key YOUR_KEY_HERE
    python3 collect_matches_recent.py --rank high --target 25000
"""

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse

API_BASE        = "https://api.deadlock-api.com"
USER_AGENT      = "Deadlock Counter Item Study Dataset Requests (@gohomecookrice)"
HIGH_RANK_BADGE = 100
BATCH_SIZE      = 500


def fetch_batch(max_match_id, api_key, rank):
    base = {
        "include_player_info":  "true",
        "include_player_items": "true",
        "include_player_kda":   "true",
        "game_mode":            "normal",
        "order_by":             "match_id",
        "order_direction":      "desc",
        "limit":                BATCH_SIZE,
    }
    if max_match_id is not None:
        base["max_match_id"] = max_match_id
    if rank == "high":
        base["min_average_badge"] = HIGH_RANK_BADGE
    elif rank == "low":
        base["max_average_badge"] = HIGH_RANK_BADGE - 1

    params = urllib.parse.urlencode(base) + "&match_mode=ranked,unranked"
    url    = f"{API_BASE}/v1/matches/metadata?{params}"
    req    = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    if api_key:
        req.add_header("Authorization", api_key)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-key", default=None, help="deadlock-api.com API key")
    parser.add_argument("--rank", choices=["high", "low"], default="high",
                        help="high: avg_badge ≥ 100 both teams; low: avg_badge < 100")
    parser.add_argument("--target", type=int, default=25_000,
                        help="Number of matches to collect (default: 25000)")
    args = parser.parse_args()

    output_file   = f"data/collected_matches_recent_{args.rank}.jsonl"
    progress_file = output_file + ".progress"

    # No key: 10 req/min → 8s delay.  With key: 10 req/10s → 1.5s delay.
    delay = 1.5 if args.api_key else 8.0

    os.makedirs("data", exist_ok=True)

    # Resume from cursor saved in progress file
    cursor = None
    collected = 0
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            parts = f.read().strip().split()
        if len(parts) == 2:
            cursor, collected = int(parts[0]), int(parts[1])
            print(f"Resuming from match_id {cursor:,}  (already collected: {collected:,})")

    print(f"Target: {args.target:,} {args.rank}-rank matches")
    print(f"Output: {output_file}")
    print(f"Delay:  {delay}s/request\n")

    with open(output_file, "a") as out:
        while collected < args.target:
            retries = 0
            batch   = []
            while True:
                try:
                    batch = fetch_batch(cursor, args.api_key, args.rank)
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 429:
                        wait = int(e.headers.get("Retry-After", 30))
                        print(f"  rate limited — waiting {wait}s")
                        time.sleep(wait)
                    else:
                        retries += 1
                        if retries > 3:
                            print(f"  ERROR {e.code} after 3 retries — aborting")
                            return
                        time.sleep(10 * retries)
                except Exception as e:
                    retries += 1
                    if retries > 3:
                        print(f"  ERROR after 3 retries ({e}) — aborting")
                        return
                    time.sleep(10 * retries)

            if not batch:
                print("No more matches returned — done.")
                break

            for match in batch:
                out.write(json.dumps(match) + "\n")
            out.flush()

            collected += len(batch)
            # Cursor moves backward: next page starts below the lowest ID in this batch
            cursor = batch[-1]["match_id"] - 1

            # Save progress so the script can be resumed
            with open(progress_file, "w") as pf:
                pf.write(f"{cursor} {collected}\n")

            print(f"  fetched {len(batch):,}  total={collected:,}  next_max_id={cursor:,}")
            time.sleep(delay)

    # Clean up progress file on clean finish
    if collected >= args.target and os.path.exists(progress_file):
        os.remove(progress_file)

    print(f"\nDone. {collected:,} matches written to {output_file}")


if __name__ == "__main__":
    main()
