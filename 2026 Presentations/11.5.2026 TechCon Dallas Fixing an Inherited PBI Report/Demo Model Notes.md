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

### Measures in `_Measures` (22: 5 used, 17 unused)

| Measure | Used by | Bad practice | Clean equivalent |
|---|---|---|---|
| `Qty Ordered` | Quantity card, matrix, scatter | Iterates over a FILTER of the whole table and uses `IN VALUES('Date'[Fiscal Year])` as a fake relationship | `SUM(Sales[Order Quantity])` |
| `Total Revenue` | Revenue by state funnel | FILTER of the table as a CALCULATE argument, SUMX on a single column | `SUM(Sales[Sales Amount])` |
| `# Products` | Product count card, matrix, scatter, table | COUNTROWS of DISTINCT of SELECTCOLUMNS of FILTER | `DISTINCTCOUNT(Sales[ProductKey])` |
| `Avg Profit` | Pie, matrix, scatter | **Context transition on every row** (CALCULATE inside SUMX over ~120K rows), `/` instead of DIVIDE, and IFERROR. This is the one that makes the page painfully slow. | `DIVIDE([Revenue] - [Cost], [Quantity Ordered], 0)` |
| `Std Cost` | Model/product cost table | FILTER iterator, multiplies by 1 | `SUM(Sales[Total Product Cost])` |

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
    "Clean Avg Profit", [Average Profit],
    "Bad Products", [# Products],
    "Clean Products", DISTINCTCOUNT(Sales[ProductKey])
)
ORDER BY 'Date'[Fiscal Year]
```

`# Products` counts products that sold, so it moves with the year slicer. The original `Calculations[Product Count]` counts every product in the `Product` table (397), which is why Marcus says in the transcript that it never changes.

## Before you open it

- **Refresh once in Desktop** so the calculated tables get built. Until then, the Terrible page visuals will show errors.
- **The `Calculations` table's M partition looks broken.** It reads `Source = ""` and then calls `Table.RemoveColumns` on that text, which Measure Killer left behind. If the refresh fails on `Calculations`, that's the cause, not the new tables.
- **The calculated-table TMDL is untested.** It follows the TMDL Doctor rules (every calculated-table column has `sourceColumn: [Name]`) and passes the TMDL Doctor linter, but there was no working calculated table in your models to copy from. If Desktop refuses to open the project, the error message will name the table.
