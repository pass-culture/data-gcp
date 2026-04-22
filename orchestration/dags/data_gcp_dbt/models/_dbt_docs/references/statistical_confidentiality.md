---
title: Statistical confidentiality
description: How exported data is protected using the Cell Key Perturbation method.
---

## Statistical confidentiality

### Why perturbation?

When publishing aggregated statistics at fine geographic or demographic levels, some cells may contain very small counts (e.g. 1-5 individuals). Publishing exact values could allow re-identification of individuals. Rather than suppressing these cells (which breaks additivity and enables subtraction attacks), we apply a **perturbative method** that adds small random noise to all counts.

### Method: Cell Key Perturbation

We use the **Cell Key Method**, the current standard recommended by [INSEE](https://blog.insee.fr/qpv-nouvelle-methode-secret-statistique/) and [Eurostat](https://ec.europa.eu/eurostat/web/products-manuals-and-guidelines/w/ks-01-24-014) for census and demographic data dissemination.

The method works in three steps:

#### 1. Record key assignment

Each individual (beneficiary) is assigned a deterministic numeric key derived from their identifier using a hash function. This key is stable: the same individual always produces the same key, regardless of which table they appear in.

#### 2. Cell key computation

When individuals are aggregated into a cell (e.g. "Ile-de-France, age 18, QPV"), their record keys are summed modulo 256 to produce a **cell key** — a number between 0 and 255. The same set of individuals always produces the same cell key, ensuring consistency across all tables.

#### 3. Perturbation lookup

A fixed **perturbation table** maps each `(count range, cell key range)` pair to a perturbation value. The perturbation applied depends on the cell count:

| Count range | Perturbation behaviour | Published count |
|-------------|----------------------|-----------------|
| 0 | Never perturbed | Always 0 |
| 1-4 | **Always perturbed** — large noise to push below 0 or above 5 | 0 or 5+ |
| 5-10 | Often perturbed — moderate noise | 0 or 5+ |
| 11-20 | Sometimes perturbed — small noise (+/- 1) | Near original |
| 21+ | Rarely perturbed (+/- 1) | Near original |

The perturbation table for individual (beneficiary) data is defined in the `perturbation_table__individual` dbt model. A separate `perturbation_table__business` model will be used for business data (with a threshold of 3 entities and 80% revenue dominance rule).

### Key rule: no published count between 1 and 4

Following the INSEE standard, **a published count is always either 0 or >= 5**. This means:

- A consumer can never know if a count of 0 means "truly zero" or "a very small number perturbed down to zero"
- A consumer seeing 5 or 6 cannot tell if the original was 1, 2, 5, 6, or 10
- It is **impossible** to know with certainty whether a cell contains fewer than 5 individuals

This is enforced by the perturbation table: for original counts of 1-4, the noise is large enough to always push the result to 0 or to 5+.

### Properties

- **Small counts are always heavily modified**: original counts of 1-4 are pushed to 0 or >= 5.
- **Large counts are virtually exact**: counts above 20 are rarely changed, and at most by +/- 1.
- **Consistency across tables**: the same individuals in different tables always receive the same perturbation, because the cell key depends only on who is in the cell, not on which table is being built.
- **Zero preservation**: empty cells are never perturbed. A count of 0 means exactly 0.
- **No suppressed cells**: every cell is published, so consumers can aggregate freely without missing data.
- **Unbiased**: the perturbation has zero mean — no systematic upward or downward bias across cells.

### Impact on aggregations

When summing perturbed cells to compute a regional or national total, the individual perturbations tend to cancel out. The expected error on an aggregate of N cells grows as the square root of N, not linearly — so national totals are virtually unaffected.

| Aggregation | Typical cells summed | Expected error |
|---|---|---|
| 1 department | 1 | +/- 1-5 |
| 1 region (~10 depts) | ~10 | +/- 3-5 |
| National (~100 depts) | ~100 | +/- 10-15 |

### Impact on ratios and nested counts

The cell-key method adds **one perturbation per published count**, based on that count's cell key. Two counts always have different cell keys unless they cover exactly the same set of individuals, so their perturbations are **statistically independent** — even when the underlying cohorts are nested (e.g. "users who booked 3+ categories" ⊂ "users whose credit expired"), or when one is a numerator and the other a denominator of the same rate.

Practical consequences:

- **Nested invariants can break on a single cell.** If cohort A ⊂ B, the raw counts satisfy `count(A) ≤ count(B)`, but after independent perturbation `count(A) + noise_A ≤ count(B) + noise_B` is not guaranteed on small cells.
- **Ratio noise does not cancel.** A rate `count(A)/count(B)` gets independent noise on both sides, so per-cell ratios can be noticeably off on small cells.
- **Aggregation is the fix.** Sum numerator and denominator each over many cells (national, regional, quarterly) before dividing — the `O(sqrt(N))` error on each sum becomes negligible relative to the sum.
- **Additive consistency is not guaranteed.** `perturb(total) ≠ Σ perturb(sub-totals)`. Don't expect published breakdowns to sum exactly to a published total.

### Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| D (max noise) | 5 | Maximum absolute perturbation applied |
| js (threshold) | 5 | No published count between 1 and js-1 |
| Cell key range | 0-255 | Derived from `farm_fingerprint(user_id) mod 256` |

### References

- [INSEE — QPV, a new method for statistical confidentiality](https://blog.insee.fr/qpv-nouvelle-methode-secret-statistique/)
- [Eurostat — Guidelines for SDC methods for census data (2024)](https://ec.europa.eu/eurostat/web/products-manuals-and-guidelines/w/ks-01-24-014)
- [Eurostat — ptable R package for perturbation table generation](https://cran.r-project.org/web/packages/ptable/vignettes/introduction.html)
- [Scotland Census 2022 — Cell Key Perturbation](https://www.scotlandscensus.gov.uk/media/d1yn5gu3/pmp017-cell-key-perturbation-emap-5940.pdf)
- [SDC Handbook on Statistical Disclosure Control](https://sdctools.github.io/HandbookSDC/Handbook-on-Statistical-Disclosure-Control.pdf)
