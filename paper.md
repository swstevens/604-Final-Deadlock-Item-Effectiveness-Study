# Do Counter Items Actually Work? Testing Item Effectiveness in Valve's Deadlock

**ICS 604: Applied Data Science - Final Project**

---

## Abstract

Valve's Deadlock boasts a robust item system that provides players with opportunities to tailor their builds with counter items in order to gain advantages against enemy players. In this paper, we observe that tier 3 counter items, which are typically bought in the midgame, have mixed outcomes for those who buy them, depending on rank. High rank players have consistently higher winrates when buying items like Slowing Hex and Disarming Hex, while other counter items, which are potentially dwarfed by tier 4 items of a hybrid nature, have a neutral or even negative correlation with winrate. For low rank players, purchasing and holding tier 3 counter items without upgrading is generally a detriment over a benefit for all case studies. 

---

## 1. Introduction

Deadlock is a 6v6 competitive game by Valve in which players select a hero (a character with a unique ability kit) and purchase items throughout the match to strengthen their capabilities. Some items are widely considered "counter items" by the player community: purchases made specifically to neutralize a particular opposing hero. Players commonly buy movement-slowing items against mobile heroes, interrupt items against heroes with channeled abilities, and debuff-removal items against heroes that apply damage-over-time effects.

These conventions are rooted in player intuition and community consensus. An item's apparent value as a counter may reflect a real win rate advantage, or it may be an artifact of skilled players buying it regardless of matchup. This project applies formal hypothesis testing to separate those two explanations.

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

**Dataset.** Match data was collected from deadlock-api.com, an open-source platform providing bulk access to Deadlock match history sourced directly from Valve's match infrastructure. Approximately 25,000 recent matches were collected for each of two rank bands. Each match record contains player-level fields including hero selection, team assignment, end-of-game item inventory, sale timestamps, and match outcome. Matches are classified as high-rank if the average badge score of both teams meets or exceeds 100 (the Oracle–Ascendant tier boundary); all other matches are classified as low-rank. Items flagged as sold before match end (via non-zero `sold_time_s`) are excluded from hold counts. The recent snapshot is used in place of a historical stratified sample to capture the current meta rather than patch-to-patch variation.

**Situational buyer filter.** Heroes with a hold rate ≥ 20% for an item across all matchups are classified as core builders and excluded from the primary counter item analysis. Core builders are typically support-role heroes for whom the item is a role-appropriate purchase regardless of matchup - their consistent uplift reflects correct role execution, not counter-picking. Situational buyers are all other heroes, for whom the purchase represents a matchup-specific decision outside their expected role. Metal Skin is treated separately: as a defensive carry item, its core builders are carries rather than supports, making a situational purchase by non-carries even further outside its intended use case. This distinction reframes the filter not as noise removal but as a separation between role-appropriate and situational buying behavior.

**Statistical tests.** For each hero × item pair, a 2×2 contingency table is constructed over situational buyer teams: item held (yes/no) × match outcome (win/loss), conditioned on the target hero being present on the enemy team. A chi-square test of independence (with Yates' continuity correction) is applied to test whether item possession is associated with winning. Effect size is reported as Cramér's V.

**Relative lift.** Raw win rates for item holders are confounded by reactive purchasing behavior - players buy counter items when they are already losing, depressing the baseline win rate of holders below 50%. The primary effectiveness metric is therefore relative lift: ΔWR(enemy present) − ΔWR(enemy absent), where ΔWR = win rate(held) − win rate(not held). This isolates the matchup-specific contribution of the item above any general baseline effect.

**All-hero volcano plot.** To contextualize the five studied pairs within the full item catalog, a chi-square test is run for every hero/item combination meeting minimum sample thresholds (≥ 20 games with item held, ≥ 5 without). Odds ratios and p-values are computed, with Benjamini-Hochberg FDR correction applied globally. Results are displayed as a volcano plot with log₂(OR) on the x-axis and −log₁₀(p_BH) on the y-axis. Pairs with p_BH < 0.05 and OR > 1.2 are classified as beneficial; OR < 1/1.2 as detrimental.

---

## 3. Results

### 3.1 Hold Rate & Win Rate


When looking at the win rate and hold rate of each item in isolation, we see very different results. The effectiveness varies wildly, with weapon centric counter items putting players at an active disadvantage, slowing hex showing promise for situational buyers only, knockdown being potentially useful but negligible on winrate, and dispel magic showing minimal overall impact.

With hold rate and win rate, dispel magic stands out as the most interesting case study. At high and low ranks, the effect on winrate depending on infernus presence is minimal. At high ranks we see an improvement from -0.2% to +1.7%. The more interesting statistic though is the purchase rate. At high ranks, among heroes that frequently buy the item, the purchase of Dispel Magic jumps to 73.8%. As will be shown later, there are characters that overly buy these items, what will be termed as frequent buyers. The high prevalence of this item suggests that crowd control (CC) is very important to the metagame and balance, and that using dispel magic in the right moments is also imperative.

When looking at slowing hex we observe differing patterns across ranks. Buy rate drops from 57% at high rank to 40% at low rank, and the win rate delta shifts from -1.0% at high rank to +0.5% at low rank — negligible in both cases. This initially suggests that slowing hex does not have a big impact on the matchup, but we will discuss some further findings in the next section.

Both selected weapon counter items perform similarly poorly in overall win rate/hold rate statistics. At high rank, Metal Skin holders win 8.5% less often than non-holders, and Disarming Hex holders win 9.2% less often. At low rank these deficits widen to -10.6% and -10.7% respectively. With high rank players we observe that these items are still ineffective and paint a picture of poor performance.

Finally, Knockdown is the most neutral of these items, with a negative delta of -3.4% at high rank and -3.5% at low rank, and a consistent purchase rate of ~22%. This purchase rate is lower than other items present in this analysis, suggesting that the item is reserved for specific hero synergies or hero counters. 



![Hold rate and win rate - high rank](figures/recent_high/01_hold_rate_win_rate.png)
**Figure 1:** Hold rate (top) and win rate delta vs non-holders (bottom) for each counter item across all matchups at high rank.

![Hold rate and win rate - low rank](figures/recent_low/01_hold_rate_win_rate.png)
**Figure 2:** Hold rate and win rate delta at low rank.

---

### 3.2 Core Builders vs Situational Buyers

With these counter items, we sought to investigate whether the identity of the purchaser matters. As we've established, there are many character archetypes. At higher levels, support-oriented characters like Paige (who shows up very frequently as a core buyer of many of the items being tested) buy counter items at a significantly higher rate. This could be because it synergizes well with their kits, which are inherently oriented around trapping and disabling opponents to give teammates advantages, and are therefore tailoring their purchases to best thwart enemy players. 


We see a particularly interesting trend with situational buyers when it comes to Slowing Hex and Mina. Among situational buyers, Slowing Hex holders facing Mina won 5.7% more frequently than non-holders (p = 0.029, V = 0.053). This suggests that core builders of slowing hex rely on the item to fill gaps in their abilities, and the presence of a character that exacerbates those weaknesses reduces winrate, even when the item is purchased. For situational buyers however, we see a notable increase in winrate of +5.7% when Mina is present, compared to no change when she is absent.

For Dispel Magic, we see a different story. When core builders are present, the winrate increases. This is in stark contrast to slowing hex, where core builders suffered due to character presence. Situational buyers also show a modest benefit from the purchase, improving by 4.6% when Infernus is present compared to when he is absent, though this result does not reach statistical significance (p = 0.951).

Again with both weapon counter items, we see a significant dropoff when comparing whether the enemy is present or not. This is compounded by the data point that there are no core builders of either of these items. Metal Skin holders see a -8.1% delta when Vyper is absent, worsening to -10.3% when she is present. Disarming Hex holders drop from -9.6% to -8.4% — a marginal improvement but still deeply negative. This suggests that purchasing these items might offer a slight edge at the margin, but they remain less effective than buying a higher tier counter item.

Knockdown presents the clearest case of community consensus unsupported by data. At high rank, situational buyers see a relative lift of essentially zero (-0.0%) when Dynamo is present — not statistically significant (p = 0.135). At low rank the lift is equally negligible (+0.0%), and while statistically significant (p = 0.011), the effect size is negligible. Unlike Slowing Hex, where a clear situational signal emerged, Knockdown shows no meaningful counter effect at either rank. The data neither confirms nor refutes its community reputation — it simply finds no effect worth measuring.

![Core builders vs situational buyers](figures/recent_high/03_core_vs_situational.png)
**Figure 3:** Win rate for core builder teams (top row) vs situational buyer teams (bottom row), conditioned on target hero presence, at high rank.

---

### 3.3 Summary Statistics

When we calculate p-value and Cramér's V for the different hero/item pairings, our initial observations become clearer. We also see clearer distinctions between high and low ranks.

---

At low rank, no item shows a positive counter effect. Slowing Hex sits at -2.1% (p = 0.004) and the weapon items carry their deepest negative correlations. Moving to high rank, two items emerge with positive lifts: Slowing Hex rises to +5.7% (p = 0.029), confirming a genuine counter effect for situational buyers, and Disarming Hex reaches +1.2% — statistically significant but practically marginal against a -8.4% baseline. Dispel Magic shows a large positive delta at high rank but an effectively null p-value, making it inconclusive. Metal Skin and Knockdown show no meaningful counter effect at either rank.

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
| Mina | Slowing Hex | -2.7% | -4.8% | -2.1% | 0.042 | 0.004 |
| Infernus | Dispel Magic | -0.4% | -0.7% | -0.3% | 0.006 | 0.732 |
| Haze | Disarming Hex | -9.7% | -11.6% | -1.9% | 0.084 | 0.000 |
| Dynamo | Knockdown | -3.9% | -3.9% | +0.0% | 0.031 | 0.011 |
| Vyper | Metal Skin | -10.4% | -13.7% | -3.3% | 0.123 | 0.000 |



</td>
</tr>
</table>

---

### 3.4 High Rank vs Low Rank Disparity

Across all five item pairings, counter item effectiveness trends upwards for high rank players. As we see with Disarming Hex, in some cases the effect even reverses.

Slowing Hex and Disarming Hex are the clearest examples of this. At high ranks, players see a +5.7% lift when purchasing Slowing Hex against Mina (p = 0.029). At low rank, the same matchup shows a -2.1% lift (p = 0.004), which is a significant result in the opposite direction. In a similar fashion with Disarming Hex, we see a statistically real signal against a deeply negative baseline of 8.4%. This is potentially compounded by Haze's general strength in the window used for statistics calculation. Metal Skin winrate improves from -3.3% to -2.2% when moving from low to high ranks. Knockdown is a notable exception. Its relative lift is effectively zero at both high and low ranks, suggesting rank has no bearing on the item's counter effectiveness or lack thereof.

The mechanisms behind this disparity cannot be observed from these data slices alone. Possible explanations include differences in active item execution between skill tiers, variation in purchasing intent, or the degree to which players capitalize on items once acquired. Disentangling these would require more granular data exploration such as correlation between item activations and takedowns, which is outside the scope of this analysis.

![Cross-rank comparison](figures/05_cross_rank_comparison.png)
**Figure 4:** Relative lift (ΔWR present − ΔWR absent) for each counter item pairing at low rank (left, red) and high rank (right, blue).

---

### 3.5 All-Hero Volcano Plot

Looking at the larger picture of item/hero matchups, we can observe where positive correlations begin to form. The standout examples generally focus around damage output. Items like Boundless Spirit, which amplifies spirit damage, and Silencer, which disables enemy weapon attacks, are direct ways that players can increase offensive output or neutralize opponents. All items appearing among the highest positive correlations in the volcano graph are tier 4 items. Our study focuses on tier 3. Of the tier 4 items that function as counter items, the two we observe in the volcano graph are dual purpose: they provide a proactive way to counter opponents (Boundless Spirit suppresses healing via spirit damage; Silencer functions like a weapon-applied silence, disabling enemy attacks) while still increasing offensive capabilities. This would suggest that tier 3 counter items are intended to bridge the gap during the midgame toward these higher-tier options. 
![Volcano plot - high rank](figures/recent_high/04_volcano_high_rank.png)
**Figure 5:** All-hero item effectiveness volcano plot at high rank. Blue = beneficial (OR > 1.2, p_BH < 0.05); red = detrimental; orange = significant but small effect. Top 10 pairs by statistical significance are labeled.

---

## 4. Discussion

Counter items in Deadlock show rank-dependent effectiveness that challenges the simplicity of community assumptions. Slowing Hex against Mina is the clearest confirmed counter relationship in this study as a statistically significant and practically meaningful effect that exists only for deliberate, situational purchases. Disarming Hex shows a real but marginal signal. The remaining three items show no positive counter effect at any rank.

The broader pattern suggests that counter items function less as guaranteed advantages and more as skill expression tools. Their benefit is only measurable when purchased intentionally by the right player in the right matchups. This aligns with the volcano plot finding that damage-oriented items dominate the significant positive correlations across the full item catalog, suggesting that raw offensive output may be more reliably impactful than reactive itemization depending on character archetype.

Several limitations apply. This analysis is limited to a single tier of each counter item and does not account for players who sold the studied items in favor of a higher-tier equivalent. Match outcome is an imperfect proxy for item effectiveness, as a single purchase cannot be isolated from team composition and overall game state. Badge rank is a coarse skill proxy, and the case studies cover only 5 of 38 available hero item combinations. Future work could correlate item activation logs with takedowns to test whether execution, not just ownership, drives the rank disparity observed here.

---

## 5. Conclusion

This study examined whether purchasing counter items in Deadlock meaningfully improved win rate when facing specific enemy heroes across two rank brackets. We found that counter item effectiveness is neither consistent nor universal. Slowing Hex against Mina was the only pairing to show a statistically significant positive relationship in high rank play (+5.7%), while Disarming Hex against Haze showed a modest but reliable effect. The remaining items produced no meaningful change or were associated with worse outcomes, likely reflecting reactive purchases from losing positions. The most notable finding is that these effects are stronger at higher ranks, suggesting that effective counter-itemization is a form of skill expression rather than a general-purpose tool. Future work should examine a broader item pool, control for team composition, and explore counter-item purchase times and usage over the course of the match. 

---

## References

[1] deadlock-api.com. *Deadlock Match History API*. https://deadlock-api.com. Accessed April–May 2026.

[2] Cramér, H. (1946). *Mathematical Methods of Statistics*. Princeton University Press.

[3] Benjamini, Y., & Hochberg, Y. (1995). Controlling the false discovery rate: A practical and powerful approach to multiple testing. *Journal of the Royal Statistical Society: Series B*, 57(1), 289–300.

---

## AI Usage Statement

This project was developed collaboratively with Claude, used as a technical partner throughout the analysis. Analytical direction, statistical interpretations, and narrative framing were guided by me based on empirical findings from the data and my knowledge of the game as a high rank player. Claude assisted with code generation, figure iteration, and proofreading at the author's request, but generation was intentional and proofread to maintain quality. The paper text is human-written, with small amounts of feedback from Claude on structure and typo fixing.
