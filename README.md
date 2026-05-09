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

---

## Repository Structure

```
.
├── final.ipynb                          # Main analysis notebook
├── volcano_plot.ipynb                   # All-hero item effectiveness volcano plot
├── requirements.txt                     # Python dependencies
├── scripts/
│   ├── collect_matches.py               # Historical stratified dataset (~25k matches)
│   ├── collect_matches_recent.py        # Most-recent N matches by rank band
│   ├── collect_matches_gap.py           # Gap-fill utility for collect_matches.py
│   ├── json_to_parquet.py               # Converts collected JSONL → Parquet
│   └── read_parquet.py                  # Utility for inspecting parquet files
├── data/                                # Dataset files (gitignored — see Data section)
│   ├── public_items.parquet             # Item ID → name/tier/cost lookup (455 rows)
│   └── public_heroes.parquet           # Hero ID → name lookup (38 heroes)
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

There are four datasets used across the two notebooks, each collected by a different script invocation. The raw JSONL files are large and excluded from the repo — collect them before running the notebooks.

| File | Script | Purpose |
|---|---|---|
| `data/collected_matches.jsonl` | `collect_matches.py` | Historical high-rank, stratified across the full Mina era (Aug 2025–Apr 2026). Used by `final.ipynb`. |
| `data/collected_matches_gap.jsonl` | `collect_matches_gap.py` | Fills gaps in the historical dataset. Also used by `final.ipynb`. |
| `data/collected_matches_recent_high.jsonl` | `collect_matches_recent.py --rank high` | Most recent 25k high-rank matches. Used by `volcano_plot.ipynb` with `RANK_MODE = 'high'`. |
| `data/collected_matches_recent_low.jsonl` | `collect_matches_recent.py --rank low` | Most recent 25k low-rank matches. Used by `volcano_plot.ipynb` with `RANK_MODE = 'low'`. |

### Collecting the data

Run each command from the repo root with the virtual environment active. An API key is optional but increases the rate limit significantly.

**Historical dataset** (for `final.ipynb`):
```bash
python scripts/collect_matches.py [--api-key YOUR_KEY]
python scripts/collect_matches_gap.py [--api-key YOUR_KEY]
```

**Recent dataset** (for `volcano_plot.ipynb`):
```bash
python scripts/collect_matches_recent.py --rank high [--api-key YOUR_KEY]
python scripts/collect_matches_recent.py --rank low  [--api-key YOUR_KEY]
```

Both `collect_matches_recent.py` runs are resumable — if interrupted, re-run the same command and it will continue from where it left off.

The lookup tables (`data/public_items.parquet`, `data/public_heroes.parquet`) are small and already included in the repository.

---

## Running the Analysis

Launch Jupyter from the repo root:

```bash
jupyter lab
```

### `final.ipynb` — Main analysis

Uses the **historical dataset** (`collected_matches.jsonl`). Requires no configuration changes before running.

Before running, set `FIGURES_DIR` at the top of the notebook to control where plots are saved:

| Data used | `FIGURES_DIR` setting |
|---|---|
| Historical (default) | `'figures'` |
| Recent high-rank | `'figures/recent_high'` |

Run: **Kernel → Restart Kernel and Run All Cells**

Covers: hold rate & win rate, item purchase rates by hero, core builder vs situational buyer analysis, and a full all-hero item effectiveness volcano plot.

### `volcano_plot.ipynb` — All-hero volcano plot

Uses the **recent dataset**. Set two variables at the top of the notebook before running:

| Variable | Options | Effect |
|---|---|---|
| `RANK_MODE` | `'high'` or `'low'` | Selects which recent dataset to load and which output folder to use |

```python
RANK_MODE = 'high'   # loads data/collected_matches_recent_high.jsonl
                     # saves to figures/recent_high/
```

Run once with `RANK_MODE = 'high'`, then again with `'low'` to produce both plots.

---

## Output figures

Figures are saved to the `figures/` directory (gitignored) when the notebooks are run:

```
figures/
├── 01_hold_rate_win_rate.png
├── 02_item_buy_rate_per_hero.png
├── 03_core_vs_situational.png
├── 04_volcano_high_rank.png
├── recent_high/
│   └── volcano_high_rank.png
└── recent_low/
    └── volcano_low_rank.png
```
