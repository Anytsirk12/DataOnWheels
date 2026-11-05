# Terrible Report: what's broken on purpose

These are the deliberately bad changes made to `Terrible Report to Great Report.pbip` on 11.5.26 for the TechCon Dallas demo. Every bad measure returns the right number, but each one is slow, hard to read, or unused. The original `.pbix` was left untouched, and a copy of the PBIP from before the changes is in the session scratchpad.

## What was added

### Calculated tables (5)

| Table | Used by visuals? | What's wrong with it |
|---|---|---|
| `Sales Data FINAL v2` | Yes, every visual except the Fiscal Year slicer | A 28-column copy of `Sales` with columns pulled in from `Product`, `Customer`, and `Date` using RELATED. It adds high-cardinality text columns (`Order Line Description` is unique on every row; `Revenue Text` and `Order Date Text` are formatted text) and duplicates the whole fact table. It has no relationships, so it's an island. |
| `_Measures` | Yes (holds the measures) | Built as a calculated table with a hidden placeholder column. |
| `Product List` | No | SUMMARIZE of the big table. Unused. |
| `Model Profit Summary` | No | Pre-aggregated profit by model. Unused, and it goes stale against filters. |
| `Date Copy` | No | A duplicate of `Date`. Unused. |

**Auto date/time is turned on** (`__PBI_TimeIntelligenceEnabled = 1`). After a refresh, Desktop builds hidden local date tables for every date column, including the ones in the calculated tables.

### Measures in `_Measures` (23: 6 used, 17 unused)

| Measure | Used by | Bad practice | Clean equivalent |
|---|---|---|---|
| `Qty Ordered` | Quantity card, matrix, scatter | Iterates over a FILTER of the whole table and uses `IN VALUES('Date'[Fiscal Year])` as a fake relationship | `SUM(Sales[Order Quantity])` |
| `Total Revenue` | Revenue by state funnel | FILTER of the table as a CALCULATE argument, SUMX on a single column | `SUM(Sales[Sales Amount])` |
| `# Products` | Product count card, matrix, scatter, table | COUNTROWS of DISTINCT of SELECTCOLUMNS of FILTER | `DISTINCTCOUNT(Sales[ProductKey])` |
| `Avg Profit` | Matrix, scatter | Context transition on every row (CALCULATE inside SUMX), `/` instead of DIVIDE, and IFERROR. The engine optimizes this well at 120K rows, so it's ugly but not slow. | `DIVIDE([Revenue] - [Cost], [Quantity Ordered], 0)` |
| `Avg Profit (Pie)` | Pie only | **A duplicate of `Avg Profit` that's painfully slow.** For every row, it counts the rows in the same slice with a `<=` comparison, then multiplies the profit by `MIN(1, that count)`, which is always 1. A `<=` comparison can't be turned into a join or cached, so the work grows with the square of each slice's row count, every time. Measured on 11.5.26: **~22 s** for the pie with no year selected and **~12 s** for FY2020 (the current `Avg Profit` takes ~240 ms), with identical results for every model. It's only used in the pie, because on the matrix grand total or the scatter it would run for minutes. | Same as `Avg Profit` |
| `Std Cost` | Model/product cost table (bottom right) | **Painfully slow.** For every one of the 121K rows, it scans all 266 rows of `Product List` with a condition that depends on that row's unique `SalesOrderLineKey`, so the engine can't reuse anything. It then multiplies the cost by `MIN(1, count + 1)`, which is always 1. The cost per row is fixed, so the grand total stays manageable. Measured on 11.5.26: **~9.5 s** for the table with no year selected, the same on repeat runs (the current version took ~0.1 s), with all 267 rows matching. | `SUM(Sales[Total Product Cost])` |

Unused: `Total Revenue OLD`, `Total Revenue (2)`, `Revenue LY`, `Revenue YTD`, `Profit %`, `Margin TEMP`, `Greg's Profit`, `Avg Price`, `Order Count`, `Customer Count`, `Bike Revenue`, `% of Total Revenue`, `Revenue Rank`, `Revenue Text Measure`, `Test`, `Measure`, and `Measure 2`. They have no format strings, no descriptions, and no display folders. Several of them use `/` instead of DIVIDE, IFERROR, or FORMAT, which returns text.

### Report changes

The Terrible Report page's visuals now read their columns from `Sales Data FINAL v2` and their measures from `_Measures`. The Fiscal Year slicer still uses `'Date'[Fiscal Year]`, so every measure has to apply the year itself. The hidden Great Report Sample page was not changed, and still uses the star schema and the `Calculations` measures.

## The fix, step by step (demo script)

1. Save as PBIP (already done) and run Measure Killer, or ask AI for an inventory. Expect it to find 17 unused measures and 3 unused calculated tables.
2. Delete the unused measures and the `Product List`, `Model Profit Summary`, and `Date Copy` tables.
3. Turn off Auto date/time.
4. Repoint the visuals from `Sales Data FINAL v2` to `Product` and `Customer`, and from `_Measures` to `Calculations` (or rewrite the five used measures as shown in the clean column above).
5. Delete `Sales Data FINAL v2` and `_Measures`.
6. Compare before and after with Performance Analyzer or DAX Studio (Server Timings), and compare file size or VertiPaq Analyzer output.

## Trust, but verify: compare the numbers

Run this in DAX query view **before** step 5. Each Bad/Clean pair should match on every row.

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Fiscal Year],
    "Bad Qty", [Qty Ordered],
    "Clean Qty", [Quantity Ordered],
    "Bad Revenue", [Total Revenue],
    "Clean Revenue", [Revenue],
    "Bad Cost", [Std Cost],
    "Clean Cost", [Cost],
    "Bad Avg Profit", [Avg Profit],
    "Bad Avg Profit (Pie)", [Avg Profit (Pie)],
    "Clean Avg Profit", [Average Profit],
    "Bad Products", [# Products],
    "Clean Products", DISTINCTCOUNT(Sales[ProductKey])
)
ORDER BY 'Date'[Fiscal Year]
```

`# Products` counts products that sold, so it moves with the year slicer. The original `Calculations[Product Count]` counts every product in the `Product` table (397), which is why Marcus says in the transcript that it never changes.

## Before you open it

- **Opened, refreshed, and saved in Desktop on 11.5.26.** Desktop kept the calculated tables and measures as written. Auto date/time added four hidden `LocalDateTable_*` tables and a `DateTableTemplate_*` table, which are worth pointing out during the Auto date/time quick win.
- **The `Calculations` partition is now `Source = ""`.** The leftover `Table.RemoveColumns` step from Measure Killer is gone, so that table refreshes cleanly.
- **To reset the demo,** close Desktop, delete the `.pbip` file and its `.Report` and `.SemanticModel` folders, and extract `Terrible Report DEMO RESET 11052026.zip` into this folder. It holds the terrible version as refreshed on 11.5.26, including the data cache, so it opens without a refresh.
