# Duplicate vendor matcher: evaluation

## Setup

- **Population.** Every vendor record from Department of the Interior awards, FY2025 and FY2026
  (USAspending.gov): 20,150 records, one per distinct (UEI, raw name, address). No sampling or
  oversampling.
- **What the matcher sees.** The SAP-shaped vendor extract only: ADRC-NAME1 (40 characters, truncated
  as SAP stores it), street or PO box, city, region and ZIP. Employee vendors are excluded. It never
  sees the UEI.
- **Answer key.** Two records sharing a UEI are a true duplicate pair (2,945 pairs). Candidate pairs
  with different UEIs and no parent link are negatives. Pairs under the same parent UEI (or parent and
  child) are distinct but related companies. They count as neither and are reported apart. Random pairs
  are never sampled, because nearly all of them would be easy non-matches and precision would look
  better than it is.
- **No tuning on the test data.** Pairs are split 50/50 by a hash of the pair. The name scorer, name
  weight and threshold are chosen on the dev half. All headline numbers come from the test half.
- **Reproduce.** `python -m matching` writes `data/matching/metrics.json`, the curve data, the error
  samples and this page's chart.

## Blocking

| Pass | Key | Candidate pairs |
|---|---|---|
| 1 | state + first 3 ZIP digits | 749,281 |
| 2 | first token of normalized name | 154,695 |
| Union (deduplicated) | | **895,497** |

That is 0.44% of the 203 million possible pairs. Blocking keeps **2,927 of 2,945 true pairs (99.4%)**.
The 18 it loses count against recall below. No block hit the 1,000-record size cap.

## Results (test half)

| Method | Name weight | Threshold | Precision | Recall | F1 |
|---|---|---|---|---|---|
| **WRatio name + address (chosen)** | 0.9 | 91 | **0.778** | **0.865** | **0.819** |
| token_set_ratio name + address (baseline) | 0.9 | 91 | 0.694 | 0.882 | 0.777 |

Score = name weight × name similarity + (1 − name weight) × address similarity (token_set_ratio), on
a 0 to 100 scale. On the dev half the chosen method scored P 0.787, R 0.879, F1 0.830, so there is
no sign of overfitting.

**Why WRatio.** `token_set_ratio` scores 100 whenever one name's words are a subset of the other's,
so "RED INC" matches "RED SKY INC" and "PRECISION LLC" matches "PRECISION INTEGRATED PROGRAMS LLC".
WRatio blends several ratios and penalizes that case. It cut test false positives from 585 to 372 and
cost about 2 points of recall.

**Related companies.** 915 candidate pairs are same-parent or parent-child. 199 of them score above the
threshold. They are excluded from precision, but a reviewer would see them in the review queue.

![Precision-recall curve on the test half](pr_curve.png)

## How far to trust precision

The UEI answer key is not perfect. Of the 372 test false positives, **343 (92%) have identical
normalized names**: 6 at the same address and 337 at different addresses. These are entities that hold
more than one UEI, often one per location. In AP terms some are separate remit-to vendors and some
are real duplicates. Only a person can tell which, so strict precision (0.778) is a floor. If every
identical-name pair were a true duplicate, precision would be (1,301 + 343) / 1,673 = 0.98. That is an
upper bound, not a claim. Hand-labeling the borderline pairs, which is deferred, would settle it.

## Error analysis

### Five false positives (scored as duplicates, different UEIs)

| # | Record A | Record B | Why it happened |
|---|---|---|---|
| 1 | HAROLD LEMAY ENTERPRISES, INCORPORATED, 4111 192ND ST E | same name, same address | The same company registered under two UEIs. The label calls it a non-match, but AP would want it flagged. |
| 2 | LEIDOS, INC., 1750 PRESIDENTS ST FL 6 | LEIDOS, INC., 1750 PRESIDENTS ST | Same company and building, one record with the floor. Two UEIs again. Label noise, not a matcher error. |
| 3 | WEST VIRGINIA UNIVERSITY, 1500 UNIVERSITY AVE | WEST VIRGINIA STATE UNIVERSITY, 5000 FAIRLAWN AVE | A genuine error. These are different institutions, and one inserted word barely moves WRatio. The different address has only 10% weight, not enough to pull the score down. |
| 4 | DIAMOND N I LLC, 120 MOUNTAIN AVE | DIAMOND N II LLC, same address | Series entities with separate legal status, differing by one character at one address. Needs a rule that treats differing trailing numbers (I/II, 1/2) as a mismatch. |
| 5 | FLORIDA ATLANTIC UNIVERSITY RESEARCH CORP, 777 GLADES RD | FLORIDA ATLANTIC UNIVERSITY, same address | An affiliated but separate entity. No parent UEI links them, so they land as a negative instead of "related". A gap in the answer key more than in the matcher. |

### Five false negatives (true duplicates the matcher missed)

| # | Record A | Record B | Why it happened |
|---|---|---|---|
| 1 | RECYCLING SOLUTIONS OF ALASKA | SARAH ROBINSON | A sole proprietor filed once under the business name and once under the owner's name. The strings have nothing in common. |
| 2 | COWBOY SIMPLE, 17658 HWY 93, AZ | JACK KRUMBHOLZ, 17658 SOUTH HIGHWAY 93, AZ | A trade name against the owner's name at the same address. Address similarity is 100, but with name weight 0.9 the name dominates. A "same address, different name" review queue would catch it. |
| 3 | PERKINELMER HEALTH SCIENCES, INC, CT | REVVITY HEALTH SCIENCES, INC., MA | A company rename (the PerkinElmer business became Revvity in 2023) plus a move. Blocking missed it (different state and first token), and no name matcher could link them. |
| 4 | CALIFORNIA DEPARTMENT OF FORESTRY AND FI(RE) | FORESTRY AND FIRE PROTECTION, CALIFORNIA | Inverted government naming, a 40-character truncation, and two different addresses. Blocking missed it: different first token and ZIP area. |
| 5 | N C WILDLIFE FEDERATION | NORTH CAROLINA WILDLIFE FEDERATION, INC. | A state abbreviation. The first tokens (N vs NORTH) and ZIP areas differ, so blocking never compared them. |

## What would improve it

1. Expand state and common abbreviations (N C → NORTH CAROLINA) and reorder inverted government
   names ("FORESTRY AND FIRE PROTECTION, CALIFORNIA") during normalization. This helps blocking and
   scoring.
2. Treat differing trailing numbers or Roman numerals as a strong mismatch signal.
3. Add a review queue for the same address with a different name, which catches trade name vs owner
   pairs that name similarity misses.
4. Try Splink with term-frequency adjustment on names (deferred). It would learn that "UNIVERSITY" or
   "DEPARTMENT OF" carry little weight.
5. Hand-label the identical-name, different-UEI pairs (deferred) to replace the precision range
   (0.78 to 0.98) with one number.
