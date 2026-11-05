# Phases 2 and 3: Architect and Build — results (11.5.26)

Checklists: `02 Architect - Quick Wins.md`, `03 Architect - Visual and Model Best Practices.md`, and `04 Build - Rebuild and Verify.md`. Backup of the PBIP before these changes: the session scratchpad, `backup_before_rebuild`. Git still has the pre-demo version, and `Terrible Report DEMO RESET 11052026.zip` resets the demo.

## Target star schema (built)

```
                 Product (397)
                     |
Customer (18,485) - Sales (121,253) - Sales Territory (11)
                     |
                  Date (1,461, marked as date table)
```

- **Fact:** `Sales`. It holds the keys plus Order Quantity, Sales Amount, and Total Product Cost, all hidden because measures aggregate them.
- **Dimensions:** `Product` (Category > Subcategory > Model > Product), `Sales Territory` (Region, Country, Group), `Date` (Date, Fiscal Year, Fiscal Quarter), and `Customer` (State-Province, Country-Region).
- **Relationships:** 4, all one-to-many, single direction, integer keys.
- **Measures:** all 11 are in `Calculations`, with display folders Sales, Sales\Last Year, Profitability, Profitability\Last Year, and Volume.

## Checklist 02: quick wins

| # | Quick win | Status | What was done |
|---|---|---|---|
| 1 | Remove unused tables and columns | ✅ | Removed `Reseller` and `Sales Order` (Measure Killer had left only their keys), `Sales[SalesOrderLineKey]` and `[ResellerKey]`, and the `Date` helper columns that only fed auto date tables. `Customer` is kept but isn't on any visual. Cut it if nobody asks for customer geography. |
| 2 | Turn off Auto date/time | ✅ | `__PBI_TimeIntelligenceEnabled = 0`. The 4 `LocalDateTable_*` tables, `DateTableTemplate_*`, their 4 relationships, and the `variation` blocks on `Date` are gone. |
| 3 | Calculated tables | ✅ | All removed: `Sales Data FINAL v2`, `_Measures`, `Product List` (plus `Date Copy` and `Model Profit Summary`, removed earlier in the demo). The star schema already covered all of them. |
| 4 | Bidirectional / many-to-many | ✅ | The bidirectional `Sales Order` relationship went with the table. |
| 5 | High-cardinality columns | ✅ | `Order Line Description`, `Revenue Text`, `Order Date Text`, and `SalesOrderLineKey` are gone. |
| 6 | Performance Analyzer | ❓ | Human, in Desktop. DAX timings are below. |
| 7 | 6–10 insights per page | ✅ | 7 data visuals plus 2 slicers. |
| 8 | Pies to bars | ✅ | Pie cut. Revenue by territory is a bar chart. |
| 9 | Hidden / unused pages | ❓ | `Great Report Sample` is still hidden. It's your demo answer key, so I fixed it instead of deleting it. |
| 10 | Explicit measures | ✅ | Every value is a `Calculations` measure, and `discourageImplicitMeasures` is on. |

## Checklist 03: best practices

| # | Practice | Status | Evidence |
|---|---|---|---|
| V1 | 6–10 insights | ✅ | See above. |
| V2 | Right visual | ✅ | No pie, funnel, or scatter. |
| V3 | Trend visuals | ✅ | Revenue and Profit Margin % by fiscal quarter, this year vs last year (Dana). |
| V4 | Group related metrics | ✅ | KPI row, then the two trends side by side, then product detail and territory detail. |
| V5 | Readable sizes | ✅ | Smallest data visual is 520 × 282. |
| V6 | Consistent palette | ✅ | Theme colors only. The matrix data bars keep their original hex colors. |
| V7 | Titles | ✅ | Every chart and the matrix have a title that says what they show. |
| V8 | Format strings | ✅ | All 11 measures. |
| V9 | Newspaper layout | ✅ | Title and slicers on top, then the KPI cards, then the trends. The top half can be screenshotted for the CFO. |
| M1 | Everything feeds a visual | ✅ / ❓ | Except `Customer` (see above). |
| M2 | Star schema | ✅ | See diagram. |
| M3 | No many-to-many | ✅ | |
| M4 | Metrics trusted by stakeholders | ❓ | They match the original exactly (below). They still need to tie to Priya's spreadsheet by territory. |
| M5 | Relationship direction | ✅ | |
| M6 | Modeling view layout | ❓ | Human. `diagramLayout.json` wasn't touched. |
| M7 | Measures table + folders | ✅ | |
| M8 | Build from scratch? | ✅ | The model was salvageable. Only the calculated tables and `_Measures` were cut. |

## Measures

| Measure | Was | DAX |
|---|---|---|
| Revenue | `Total Revenue` | `SUM ( Sales[Sales Amount] )` |
| Cost | `Std Cost` | `SUM ( Sales[Total Product Cost] )` |
| Profit | `Profit Margin` (renamed: it's dollars) | `[Revenue] - [Cost]` |
| Profit Margin % | new | `DIVIDE ( [Profit], [Revenue] )` |
| Profit per Unit | `Avg Profit`, `Avg Profit (Pie)`, `Average Profit` | `DIVIDE ( [Profit], [Quantity Ordered] )` |
| Quantity Ordered | `Qty Ordered` | `SUM ( Sales[Order Quantity] )` |
| Product Count | unchanged (sample page only) | `DISTINCTCOUNT ( 'Product'[ProductKey] )` |
| Revenue LY, Profit Margin % LY | new | `CALCULATE ( …, SAMEPERIODLASTYEAR ( 'Date'[Date] ) )` |
| Revenue YoY %, Profit Margin % Change | new | change vs LY, blank when LY is blank |

Descriptions are in the model (`///` lines) and in `Report Notes.md`.

## Report page (was "Terrible Report", now "Sales Performance")

| Visual | Status | Fields |
|---|---|---|
| Fiscal Year buttons | Kept | `'Date'[Fiscal Year]` |
| Sales Territory dropdown | **New** (Marcus) | `'Sales Territory'[Region]` |
| KPI cards | **New** (Dana) | Revenue, Revenue YoY %, Profit Margin %, Profit Margin % Change, Quantity Ordered |
| Revenue trend, TY vs LY | **New** (Dana) | Fiscal Year > Fiscal Quarter; Revenue, Revenue LY |
| Margin % trend, TY vs LY | **New** (Dana) | Fiscal Year > Fiscal Quarter; Profit Margin %, Profit Margin % LY |
| Product matrix | **Kept, repointed** (Marcus) | `Product` Subcategory > Model > Product; Quantity Ordered, Revenue, Profit Margin %, Profit per Unit |
| Revenue by Sales Territory | **Replaces funnel** (Dana, Priya) | `'Sales Territory'[Region]`, Revenue |
| Scatter, pie, Product Count card, cost table, funnel | **Cut** | |

New visuals were copied from working visuals in this report (the slicer, the sample page's card, line chart, and bar chart) and then repointed. The hidden sample page's matrix pointed at `_Measures` measures that didn't exist; it now uses `Calculations`.

## Checklist 04: verify the numbers

| # | Step | Status | Evidence |
|---|---|---|---|
| R1 | Work on a copy | ✅ | Git, plus the scratchpad backup and the reset zip. |
| R2 | Rules for AI | ✅ | `Sample CLAUDE.md`, plus the global Power BI safety rules. |
| R3 | Small batches | ❌ | Done as one batch, on request. Desktop was closed for every file edit and reopened once afterward. TMDL lint was clean against a control model. |
| R4 | You approve | ❓ | Review the git diff. |
| V1 | Snapshot old numbers | ✅ | Ran against the original model before any change: `old_total`, `old_by_fy`, `old_by_product` (FY × Subcategory × Model × Product, 441 rows). |
| V2 | Rerun on new model | ✅ | Same queries, new measures and dimensions. |
| V3 | Diff and explain | ✅ | **0 differences** in Quantity, Revenue, Cost, products sold, and Profit per Unit, at every grain (tolerance half a cent). |

| Fiscal Year | Quantity | Revenue | Cost | Profit per Unit |
|---|---|---|---|---|
| FY2018 | 24,349 | $23,860,891.17 | $20,824,957.60 | $124.68 |
| FY2019 | 83,339 | $34,070,108.50 | $30,362,108.34 | $44.49 |
| FY2020 | 167,088 | $51,878,274.54 | $46,070,842.02 | $34.76 |
| **Total** | **274,776** | **$109,809,274.20** | **$97,257,907.95** | **$45.68** |

Territory revenue adds back to each year's total exactly.

**Not compared:** `Avg Profit (Pie)` timed out after 10 minutes in the by-year query. The demo notes already show it equals `Avg Profit`, and `Avg Profit` matches.

### Speed (DAX query time, same machine)

| Query | Before | After |
|---|---|---|
| Totals (Qty, Revenue, Cost, Products, Avg Profit) | 12.2 s | 0.06 s |
| By fiscal year | 12.7 s (without the pie measure); over 10 min with it | 0.06 s |
| By year × product (Marcus's matrix grain) | 0.9 s | 0.1 s |

## Findings and open items

1. **FY2021 last-year values (fixed).** `Date` runs to 6/30/2021, but sales end 6/15/2020, so the trend charts showed an FY2021 with last-year lines and Revenue YoY % of −100%. `Revenue LY` and `Profit Margin % LY` now return a value only when there's current revenue: `IF ( NOT ISBLANK ( [Revenue] ), CALCULATE ( …, SAMEPERIODLASTYEAR ( 'Date'[Date] ) ) )`. Rerun: 12 quarters (FY2018–FY2020), no FY2021, and every other value unchanged (`verification/new_visuals_fixed.csv`).
2. **The "(Blank)" fiscal year is gone.** No `Date` row has a blank fiscal year, and every sales row matches a date and a territory. Ask Priya whether she still sees blanks in her source.
3. **The $97.26M cost is real:** it's total standard cost × quantity across all three years with no year selected. It still needs to tie to Priya's ERP cost.
4. **Needs a human:** look over the page in Desktop, run Performance Analyzer, tie out Priya's spreadsheet by territory, and tidy the modeling view.
