# Do Counter Items Actually Work? Testing Item Effectiveness in Valve's Deadlock

**ICS 604: Applied Data Science — Final Project**

Deadlock is a 6v6 competitive game by Valve where players purchase items throughout a match. Some items are widely considered "counter items" by the player community — purchases made to neutralize a specific opposing hero. This project applies formal hypothesis testing to determine whether four such counter items provide a real win-rate advantage, or whether the community consensus is driven by correlation with skilled play.

## Research Questions

1. Is each counter item purchased at a significantly higher rate when its target hero is on the enemy team?
2. When a player buys the counter item, is their win rate meaningfully higher after controlling for skill tier?
3. Do results confirm, refute, or complicate community consensus on each counter relationship?
4. Does counter item effectiveness differ across skill tiers?

### Counter Item Pairs Studied

| Item | Target Hero | Community Belief |
|---|---|---|
| Knockdown | Dynamo | Strong consensus counter |
| Disarming Hex | Haze | Widely accepted counter |
| Slowing Hex | Mina | Explicitly cited in guides |
| Dispel Magic | Infernus | Considered an auto-buy |

## Repository Structure

```
.
├── final.ipynb                      # Main analysis notebook (run this)
├── requirements.txt                 # Python dependencies
├── scripts/
│   ├── collect_matches.py           # Pulls match data from deadlock-api.com
│   ├── collect_matches_gap.py       # Fills gaps in collected match data
│   ├── json_to_parquet.py           # Converts collected JSONL → Parquet
│   └── read_parquet.py              # Utility for inspecting parquet files
├── exploratory/                     # Earlier-stage analysis notebooks (not graded)
├── data/                            # Dataset files (see Data section below)
│   ├── public_items.parquet         # Item ID → name/tier/cost lookup (455 rows)
│   └── public_heroes.parquet        # Hero ID → name lookup (38 heroes)
└── deadlock_proposal_submission.pdf # Original project proposal
```

## Data

Match data was collected from the [deadlock-api.com](https://api.deadlock-api.com) public API:

- **Match metadata:** `https://api.deadlock-api.com/v1/matches/metadata?include_player_items=true`
- **Item/hero lookups:** `https://files.deadlock-api.com/Default/buckets/db-snapshot/public/`

The full collected dataset (`data/collected_matches.jsonl`) is ~12 GB and is not included in this repository. To reproduce data collection, run the scripts in order:

```bash
python scripts/collect_matches.py          # collect raw match JSON
python scripts/collect_matches_gap.py      # fill any collection gaps
python scripts/json_to_parquet.py          # convert to parquet for analysis
```

The lookup tables (`data/public_items.parquet`, `data/public_heroes.parquet`) are small and included in the repository.

> The graders should contact the author for a copy of the processed parquet dataset if re-collection is not feasible.

## Environment Setup

Python 3.11+ required.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Running the Analysis

With the environment activated, launch Jupyter and open `final.ipynb`:

```bash
jupyter lab
```

Then run all cells top-to-bottom (**Kernel → Restart Kernel and Run All Cells**). All outputs and figures are pre-executed and visible in the committed notebook.

The notebook covers:
1. Data loading and preprocessing
2. Chi-square test of independence (purchase rate vs. target hero presence)
3. Two-proportion Z-test (win rate with vs. without counter item, per skill tier)
4. Logistic regression controlling for skill tier and hero identity
5. Rolling 14-day correlation analysis
6. Results summary and classification (Confirmed / Overrated / Undiscovered)
