# Do Counter Items Actually Work? Testing Item Effectiveness in Valve's Deadlock

**ICS 604: Applied Data Science - Final Project**

---

## Abstract

<!-- ~100 words -->
<!-- Cover: what game, what question, what method, key finding (high vs low rank disparity), one-sentence conclusion -->

---

## 1. Introduction

Deadlock is a 6v6 competitive game by Valve in which players select a hero (a character with a unique ability kit) and purchase items throughout the match to strengthen their capabilities. Some items are widely considered "counter items" by the player community: purchases made specifically to neutralize a particular opposing hero. Players commonly buy movement-slowing items against mobile heroes, interrupt items against heroes with channeled abilities, and debuff-removal items against heroes that apply damage-over-time effects.

These conventions are rooted in player intuition and community consensus. An item's apparent value as a counter may reflect a real win rate advantage, or it may be an artifact of skilled players buying it regardless of matchup. This project applies formal hypothesis testing to separate those two explanations.

<!-- NOTE from proposal - original proposal studied 4 pairs (Knockdown/Dynamo, Disarming Hex/Haze, Slowing Hex/Mina, Dispel/Infernus). Final notebook studies 5: Metal Skin/Vyper was added to compare offensive vs reactive counter item types. Update this paragraph accordingly. -->

This project investigates five counter item pairings selected for their strong community consensus:

| Archetype | Character | Item |
|---|---|---|
| High Mobility | Mina | Slowing Hex |
| Debuff / Damage over time | Infernus | Dispel Magic |
| Channeled ability | Dynamo | Knockdown |
| Weapon (reactive) | Vyper | Metal Skin |
| Weapon (offensive) | Haze | Disarming Hex |

The Vyper and Haze pairings are both weapon-oriented, allowing a secondary comparison between offensive and reactive counter item archetypes.

**Research questions:**
1. Is each counter item purchased at a significantly higher rate when its target hero is on the enemy team, compared to matches where that hero is absent?
2. When a player buys a counter item against its target hero, is their win rate meaningfully higher than players who did not?
3. Do the results confirm, refute, or complicate the community consensus on each counter relationship?
4. Does counter item effectiveness differ across skill tiers?

---

## 2. Data & Methods

<!-- NOTE from proposal - proposal described four skill tiers (Initiate–Arcanist / Ritualist–Archon / Oracle–Ascendant / Eternus) and rolling 14-day averages for temporal analysis. The actual implementation uses two rank bands only (high: avg_badge ≥ 100 both teams; low: otherwise), and no temporal analysis was performed. The proposal also described a Two-Proportion Z-Test (Step 2) and Logistic Regression (Step 3) - neither appears in final.ipynb. Use the methods below which reflect what was actually done. -->

**Dataset.** Match data was collected from deadlock-api.com, an open-source platform providing bulk access to Deadlock match history sourced directly from Valve's match infrastructure. Approximately 25,000 recent matches were collected for each of two rank bands. Matches are classified as high-rank if the average badge score of both teams meets or exceeds 100 (the Oracle–Ascendant tier boundary); all other matches are classified as low-rank. The recent snapshot is used in place of a historical stratified sample to capture the current meta rather than patch-to-patch variation.

<!-- NOTE - confirm the exact match counts when writing. The collection scripts target 25k per band but actual totals may vary. Check the data files or notebook output. -->

**Situational buyer filter.** Heroes with a hold rate ≥ 20% for an item across all matchups are classified as core builders and excluded from the primary counter item analysis. Core builders are typically support-role heroes for whom the item is a role-appropriate purchase regardless of matchup - their consistent uplift reflects correct role execution, not counter-picking. Situational buyers are all other heroes, for whom the purchase represents a matchup-specific decision outside their expected role. Metal Skin is treated separately: as a defensive carry item, its core builders are carries rather than supports, making a situational purchase by non-carries even further outside its intended use case. This distinction reframes the filter not as noise removal but as a separation between role-appropriate and situational buying behavior.

<!-- NOTE - if space allows, Figure 02_item_buy_rate_per_hero.png directly visualizes which heroes are core builders per item and makes the support-role argument concrete. Currently cut for page limit but worth referencing in a sentence if the figure appears. -->

**Statistical tests.** For each hero × item pair, a 2×2 contingency table is constructed over situational buyer teams: item held (yes/no) × match outcome (win/loss), conditioned on the target hero being present on the enemy team. A chi-square test of independence (with Yates' continuity correction) is applied to test whether item possession is associated with winning. Effect size is reported as Cramér's V.

**Relative lift.** Raw win rates for item holders are confounded by reactive purchasing behavior - players buy counter items when they are already losing, depressing the baseline win rate of holders below 50%. The primary effectiveness metric is therefore relative lift: ΔWR(enemy present) − ΔWR(enemy absent), where ΔWR = win rate(held) − win rate(not held). This isolates the matchup-specific contribution of the item above any general baseline effect.

**All-hero volcano plot.** To contextualize the five studied pairs within the full item catalog, a chi-square test is run for every hero/item combination meeting minimum sample thresholds (≥ 20 games with item held, ≥ 5 without). Odds ratios and p-values are computed, with Benjamini-Hochberg FDR correction applied globally. Results are displayed as a volcano plot with log₂(OR) on the x-axis and −log₁₀(p_BH) on the y-axis. Pairs with p_BH < 0.05 and OR > 1.2 are classified as beneficial; OR < 1/1.2 as detrimental.

---

## 3. Results

### 3.1 Hold Rate & Win Rate

<!-- Takeaway: items ARE bought at elevated rates when the target hero is present (confirms RQ1).
Raw win rates are below 50% for item holders - framing: reactive "stem the bleeding" purchases, not proactive advantages.
Compare how hold rates and raw win rates differ between high and low rank. -->




When looking at the win rate and hold rate of each item in isolation, we see very different results. The effectiveness varies wildly, with weapon centric counter items putting players at an active disadvantage, slowing hex showing promise for situational buyers only, knockdown being potentially useful but negligible on winrate, and dispel magic showing minimal overall impact.

With hold rate and win rate, dispel magic stands out as the most interesting case study. At high and low ranks, the effect on winrate depending on infernus presence is minimal. At high ranks we see an improvement from -0.2% to +1.7%. The more interesting statistic though is the purchase rate. At high ranks, among heroes that frequently buy the item, the purchase of Dispel Magic jumps to 73.8%. As will be shown later, there are characters that overly buy these items, what will be termed as frequent buyers. The high prevalence of this item suggests that CC is very important to the metagame and balance, and that using dispel magic in the right moments is also imperative.

When looking at slowing hex we observe differing patterns across ranks. Buy rate drops from 57% at high rank to 40% at low rank, and the win rate delta shifts from -1.0% at high rank to +0.5% at low rank — negligible in both cases. This initially suggests that slowing hex does not have a big impact on the matchup, but we will discuss some further findings in the next section.

Both selected weapon counter items perform similarly poor in overall win rate/hold rate statistics. At high rank, Metal Skin holders win 8.5% less often than non-holders, and Disarming Hex holders win 9.2% less often. At low rank these deficits widen to -10.6% and -10.7% respectively. With high rank players we observe that these items are still ineffective and paint a picture of poor performance.

Finally, Knockdown is the most neutral of these items, with a negative delta of -3.4% at high rank and -3.5% at low rank, and a consistent purchase rate of ~22%. This purchase rate is lower than other items present in this analysis, suggesting that the item is reserved for specific hero synergies or hero counters. 



![Hold rate and win rate - high rank](figures/recent_high/01_hold_rate_win_rate.png)

![Hold rate and win rate - low rank](figures/recent_low/01_hold_rate_win_rate.png)

---

### 3.2 Core Builders vs Situational Buyers

<!-- Takeaway: core builders (supports in their intended role) show positive uplift - buying the item is part of their job and it shows in win rate.
Once they are removed, situational buyers show little or negative uplift - buying outside your role doesn't help.
This reframes the finding: counter items work when bought by the right hero archetype (support roles), not as a general matchup response.
Metal Skin exception: its core builders are carries, not supports. Situational Metal Skin buyers are non-carries buying a carry-item defensively - even further from intended use, reflected in the most negative relative lift of all five pairs.
Note: the buy rate by hero chart (02_item_buy_rate_per_hero.png) would visually reinforce this - it shows the specific heroes that are core builders. Consider including as a supplemental or inline reference if page budget allows. Otherwise, name the core builder heroes in prose (e.g. "heroes such as X and Y account for the bulk of Knockdown purchases regardless of matchup"). -->

With these counter items, I wanted to investigate whether the purchaser of the item is significantly important. As we've established, there are many character archetypes. At higher levels, support oriented characters like Paige (who shows up very frequenctly as a core buyer of many of the items being tested) buy counter items at a significantly higher rate. This could be because it synergizes well with their kits, which are inherently oriented around trapping and disabling opponents to give teammates advantages, and are therefore tailoring their purchases to best thwart enemy players. 


We see a particularly interesting trend with situational buyers when it comes to Slowing Hex and Mina. Among situational buyers, Slowing Hex holders facing Mina won 5.7% more frequently than non-holders (p = 0.029, V = 0.053). This suggests that core builders of slowing hex rely on the item to fill gaps in their abilities, and the presence of a character that exacerbates those weaknesses reduces winrate, even when the item is purchased. For situational buyers however, we see a notable increase in winrate of +5.7% when Mina is present, compared to no change when she is absent.

For Dispel Magic, we see a different story. When core builders are present, the winrate increases. This is in stark contrast to slowing hex, where core builders suffered due to character presence. Situational buyers also show a modest benefit from the purchase, improving by 4.5% when Infernus is present compared to when he is absent, though this result does not reach statistical significance (p = 0.951).

Again with both weapon counter items, we see a significant dropoff when comparing whether the enemy is present or not. This is compounded by the data point that there are no core builders of either of these items. Metal Skin holders see a -8.1% delta when Vyper is absent, worsening to -10.3% when she is present. Disarming Hex holders drop from -9.6% to -8.6% — a marginal improvement but still deeply negative. This suggests that purchasing these items might offer a slight edge at the margin, but they remain less effective than buying a higher tier counter item.

Knockdown presents the clearest case of community consensus unsupported by data. At high rank, situational buyers see a relative lift of -1.1% when Dynamo is present — not statistically significant (p = 0.135). At low rank the lift is essentially zero (+0.03%), and while statistically significant (p = 0.011), the effect size is negligible. Unlike Slowing Hex, where a clear situational signal emerged, Knockdown shows no meaningful counter effect at either rank. The data neither confirms nor refutes its community reputation — it simply finds no effect worth measuring.

![Core builders vs situational buyers](figures/recent_high/03_core_vs_situational.png)

---

### 3.3 Summary Statistics

<!-- Takeaway: show the table of Cramér's V, p-value, relative lift per pair.
Call out: which pairs are statistically significant at high rank vs low rank.
Note any p≈0 cases - explain these are floating-point underflow, not literally zero. -->

<table>
<tr>
<td>

**High Rank**

| Hero | Item | ΔWR (absent) | ΔWR (present) | Relative lift | Cramér's V | p-value |
|---|---|---|---|---|---|---|
| Mina | Slowing Hex | 0.0% | +5.7% | +5.7% | 0.053 | 0.029 |
| Infernus | Dispel Magic | -3.6% | +0.9% | +4.6% | 0.003 | 0.951 |
| Haze | Disarming Hex | -9.6% | -8.4% | +1.2% | 0.060 | 0.000 |
| Dynamo | Knockdown | -2.6% | -2.7% | -0.0% | 0.019 | 0.135 |
| Vyper | Metal Skin | -8.1% | -10.3% | -2.2% | 0.102 | 0.000 |

</td>
<td>

**Low Rank**

| Hero | Item | ΔWR (absent) | ΔWR (present) | Relative lift | Cramér's V | p-value |
|---|---|---|---|---|---|---|
| Dynamo | Knockdown | -3.9% | -3.9% | +0.0% | 0.031 | 0.011 |
| Infernus | Dispel Magic | -0.4% | -0.7% | -0.3% | 0.006 | 0.732 |
| Haze | Disarming Hex | -9.7% | -11.6% | -1.9% | 0.084 | 0.000 |
| Mina | Slowing Hex | -2.7% | -4.8% | -2.1% | 0.042 | 0.004 |
| Vyper | Metal Skin | -10.4% | -13.7% | -3.3% | 0.123 | 0.000 |

</td>
</tr>
</table>

---

### 3.4 High Rank vs Low Rank Disparity

<!-- CORE FINDING of the paper.
Takeaway: at low rank, counter items show stronger and more consistent lifts with lower p-values.
At high rank, signals shrink toward zero - counter items are less effective because skilled players already buy proactively or play around them, collapsing the measurable advantage.
This is the key contribution: counter items "work" but their measurable benefit is arbitraged away at higher skill levels. -->

When we compare high and low rank data, more interesting trends begin to form. 

![Cross-rank comparison](figures/05_cross_rank_comparison.png)

---

### 3.5 All-Hero Volcano Plot

In creating these individual chi square investigations, the larger picture should also be 
<!-- Takeaway: zooming out beyond the five studied pairs, the pattern holds - significant counter relationships exist across the item catalog.
Points above threshold line = statistically significant lift. Labeled top 10 by significance.
Discuss what the shape of the cloud says about item balance broadly. -->

![Volcano plot - high rank](figures/recent_high/04_volcano_high_rank.png)

---

## 4. Discussion

<!-- Cover:
- Why high-rank players show weaker counter-item signal: proactive buying, better game sense, playing around the item
- "Stem the bleeding" framing: item holders are already in a losing position (hence sub-50% raw WR), relative lift is the right lens
- From proposal classification criteria - which items are Confirmed / Overrated / Undiscovered:
    Confirmed: significant elevation in both purchase rate and conditional win rate
    Overrated: elevated purchase rate but no significant win rate lift
    Undiscovered: significant win rate lift but purchase rate not meaningfully elevated
- Limitations: observational data - can't establish causality; badge rank is a coarse proxy for skill; hero pool is small (5 of 38); Mina was released Aug 2025 so her sample history is shorter
- Metal Skin vs Vyper negative relative lift - anti-pattern or confounding hero selection?
- What this suggests for game design / player advice
-->

---

## 5. Conclusion

<!-- ~150 words -->
<!-- Restate the question, summarize the findings, land on the rank-disparity insight as the key takeaway.
Counter items show real but rank-dependent effectiveness - high-rank players have already priced in the counter, reducing measurable advantage. -->

---

## References

<!-- deadlock-api.com data source -->
<!-- Cramér's V citation -->
<!-- BH correction citation (Benjamini & Hochberg, 1995) -->
<!-- Any Deadlock patch notes or community sources cited -->
