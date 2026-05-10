# Do Counter Items Actually Work? Testing Item Effectiveness in Valve's Deadlock

**ICS 604: Applied Data Science — Final Project**

Deadlock is a 6v6 competitive game by Valve where players purchase items throughout a match. Some items are widely considered "counter items" by the player community — purchases made to neutralize a specific opposing hero. This project applies formal hypothesis testing to determine whether five such counter items provide a real win-rate advantage, or whether the community consensus is driven by correlation with skilled play.

## Research Questions

1. Is each counter item purchased at a significantly higher rate when its target hero is on the enemy team?
2. When a player buys the counter item, is their win rate meaningfully higher after controlling for skill tier?
3. Do results confirm, refute, or complicate community consensus on each counter relationship?
4. Does counter item effectiveness differ across skill tiers?

### Counter Item Pairs Studied

| Item | Target Hero | Archetype |
|---|---|---|
| Slowing Hex | Mina | High mobility |
| Dispel Magic | Infernus | Debuff / Damage over time |
| Knockdown | Dynamo | Channeled ability |
| Metal Skin | Vyper | Weapon (reactive) |
| Disarming Hex | Haze | Weapon (offensive) |

---

## Repository Structure

```
.
├── final_recent_high.ipynb              # Main analysis — high rank (run as-is)
├── final_recent_low.ipynb               # Main analysis — low rank (run as-is)
├── final.ipynb                          # Configurable version (preset selection at top)
├── volcano_plot.ipynb                   # Standalone all-hero volcano plot with tabular output
├── requirements.txt                     # Python dependencies
├── scripts/
│   ├── collect_matches_recent.py        # Collect most-recent N matches by rank band
│   ├── collect_matches.py               # Historical stratified dataset (not used in paper)
│   ├── collect_matches_gap.py           # Gap-fill utility for collect_matches.py
│   ├── json_to_parquet.py               # Converts collected JSONL → Parquet
│   └── read_parquet.py                  # Utility for inspecting parquet files
├── data/                                # Dataset files (gitignored — see Data section)
│   ├── public_items.parquet             # Item ID → name/tier/cost lookup (455 rows)
│   └── public_heroes.parquet            # Hero ID → name lookup (38 heroes)
├── exploratory/                         # Earlier-stage analysis notebooks
└── deadlock_proposal_submission.pdf     # Original project proposal
```

---

## Environment Setup

Python 3.11+ required.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Datasets

The analysis uses two datasets, one per rank band. The raw JSONL files are large and excluded from the repo — collect them before running the notebooks.

| File | Script | Purpose |
|---|---|---|
| `data/collected_matches_recent_high.jsonl` | `collect_matches_recent.py --rank high` | ~25k most recent high-rank matches |
| `data/collected_matches_recent_low.jsonl` | `collect_matches_recent.py --rank low` | ~25k most recent low-rank matches |

### Collecting the data

Run from the repo root with the virtual environment active. An API key is optional but increases the rate limit significantly.

```bash
python scripts/collect_matches_recent.py --rank high [--api-key YOUR_KEY]
python scripts/collect_matches_recent.py --rank low  [--api-key YOUR_KEY]
```

Both commands are resumable — if interrupted, re-run the same command and it will continue from where it left off.

The lookup tables (`data/public_items.parquet`, `data/public_heroes.parquet`) are small and already included in the repository.

---

## Running the Analysis

Launch Jupyter from the repo root:

```bash
jupyter lab
```

### Main analysis — two dedicated notebooks

Run each notebook once with no configuration needed:

- **`final_recent_high.ipynb`** — high rank analysis. Run: **Kernel → Restart Kernel and Run All Cells**
- **`final_recent_low.ipynb`** — low rank analysis. Run: **Kernel → Restart Kernel and Run All Cells**

Run both to generate all figures. The cross-rank comparison (Figure 4 in the paper) renders automatically in each notebook once both `counter_item_summary.csv` files exist.

Each notebook covers: hold rate & win rate, item purchase rates by hero, core builder vs situational buyer analysis, all-hero volcano plot, and cross-rank comparison.

### `volcano_plot.ipynb` — Standalone volcano plot with tabular output

Uses the same recent datasets. Set `RANK_MODE = 'high'` or `'low'` at the top of the notebook. Produces the volcano figure and a sortable table of all significant hero × item pairs.

Run once with `RANK_MODE = 'high'`, then again with `'low'`.

---

## Output Figures

Figures are saved to the `figures/` directory (gitignored) when the notebooks are run:

```
figures/
├── 05_cross_rank_comparison.png
├── recent_high/
│   ├── 01_hold_rate_win_rate.png
│   ├── 02_item_buy_rate_per_hero.png
│   ├── 03_core_vs_situational.png
│   ├── 04_volcano_high_rank.png
│   └── counter_item_summary.csv
└── recent_low/
    ├── 01_hold_rate_win_rate.png
    ├── 02_item_buy_rate_per_hero.png
    ├── 03_core_vs_situational.png
    ├── 04_volcano_low_rank.png
    └── counter_item_summary.csv
```
