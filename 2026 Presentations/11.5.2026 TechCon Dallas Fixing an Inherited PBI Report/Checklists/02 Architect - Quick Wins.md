# Phase 2: Architect, quick wins

Changes that usually pay off in the first day or two, before any real redesign. They buy you trust and time.

**How to use this with an AI helper:**

> Run `Checklists/02 Architect - Quick Wins.md` against this project. For each item, mark it ✅ already fine, ❌ needs work, or ❓ needs a human, and give the evidence (file and line). Then rank the ❌ items by effort and impact. Don't change any files yet.

## Model

| # | Quick win | AI check |
|---|---|---|
| 1 | Remove unused tables and columns | Cross-reference every table and column against visuals, measures, relationships, sort-by columns, and filters |
| 2 | Turn off Auto date/time | `model.tmdl` has `annotation __PBI_TimeIntelligenceEnabled = 1`, or there are `LocalDateTable_*.tmdl` / `DateTableTemplate_*.tmdl` files |
| 3 | Move calculated tables to Power Query or upstream | Any `partition ... = calculated`. Say whether each one is used, and whether the existing star schema already covers it |
| 4 | Fix bidirectional and many-to-many relationships | `crossFilteringBehavior: bothDirections` or `fromCardinality: many` / `toCardinality: many` in `relationships.tmdl` |
| 5 | Split high-cardinality datetime and text columns | Datetime columns that carry a time, and text columns that are unique per row (descriptions, concatenated keys, formatted numbers) |

## Report

| # | Quick win | AI check |
|---|---|---|
| 6 | Run Performance Analyzer to find the slowest visuals | **Human** (in Desktop). The agent can then explain the slow visual's measures |
| 7 | Cut each page to 6–10 insights | Count the data visuals per page in `pages/*/visuals/` |
| 8 | Swap pies and donuts for bars | `"visualType": "pieChart"` or `"donutChart"`, especially with a high-cardinality category such as Model or Product |
| 9 | Delete hidden and unused pages and visuals | Pages with `"visibility": "HiddenInViewMode"`, and visuals with no query that aren't text, images, or shapes |
| 10 | Use explicit measures, not implicit aggregations | Visual projections that use `Aggregation` on a column instead of a `Measure` reference |
