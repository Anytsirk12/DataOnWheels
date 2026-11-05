# Phase 1: Inspect — results (11.5.26)

Checklist: `Checklists/01 Inspect - First Steps and Stakeholders.md`. Sources: the PBIP files, `Stakeholder Feedback Transcript - Terrible Report.txt`, and `Demo Model Notes.md`. No files were changed.

Item identity confirmed from `.platform`: Report and SemanticModel are both `displayName: Terrible Report to Great Report` (report `2b57c885…`, model `30f9def6…`).

## First steps

| # | Item | Status | Evidence |
|---|---|---|---|
| 1 | Save a copy before changing anything | ✅ | Git repo on `main`, last commit `f180dea`. Only `.pbi/localSettings.json` files are modified. Also `Terrible Report DEMO RESET 11052026.zip`. |
| 2 | Save as PBIP | ✅ | `definition/tables/*.tmdl` (18 tables) and `.Report/definition/pages/` (PBIR) exist. |
| 3 | Meet with stakeholders | ✅ | Transcript: Dana Whitfield (VP Sales, owner), Marcus Lee (Regional Mgr, Southwest), Priya Shah (Sr. Financial Analyst). |
| 4 | Break issues into tasks | ✅ | See Keep / Fix / Cut below. |
| 5 | Define "done" | ✅ | Dana: "loads in a few seconds… revenue, margin, and how we compare to last year, by territory." Priya: revenue, cost, and margin tie to her spreadsheet for last FY "to the dollar, by territory." Marcus: "filter to the Southwest and still drill into my bike models." |
| 6 | Accept what can't be fixed | ❓ | Priya flagged a "(Blank)" fiscal year (orders with no matching date). Needs a query against the data (below). Phase two items (CRM pipeline, forecasts) are out of scope. |

## Stakeholder questions

| # | Question | Answer |
|---|---|---|
| 1 | Decisions | Which product lines make money, not just volume; where regions are growing or slipping, to place reps and promo budget (Dana). |
| 2 | Money metrics | Revenue, profit margin %, and both vs same period last year (Dana). |
| 3 | Grain and scope | Sales territory, not customer state (Dana, Priya). Product: Subcategory → Model → Product (Marcus). Quarterly trend (Dana). |
| 4 | Users and slicing | Dana weekly; 5 regional managers daily, filtered to their territory; Priya (finance); CFO via screenshots of the top half (Dana, Marcus). |
| 5 | Context dimensions | Fiscal year, sales territory, product hierarchy, quarter. |
| 6 | Where the data lives | The sales data warehouse, same tables as Priya's spreadsheet (Priya). |
| 7 | Data quality landmines | Blank fiscal year (Priya); the $97.2M cost total doesn't tie to the ERP (Priya); customer state ≠ managing territory (Priya). |
| 8 | Frequency | Daily for managers, weekly (Monday) for Dana; data refreshes overnight. |

**Unanswered:** who owns the blank-date fix upstream, and which fiscal year is "last fiscal year" for the tie-out (Priya is sending last Monday's spreadsheet as the answer key).

## Dissect the code

| # | Item | Status | Evidence |
|---|---|---|---|
| 1 | Inventory | ❌ | 18 tables: 7 import (star), 11 calculated (`Sales Data FINAL v2`, `_Measures`, `Product List`, `Model Profit Summary`, `Date Copy`, plus 4 `LocalDateTable_*` and 1 `DateTableTemplate_*` from Auto date/time). 29 measures (23 in `_Measures`, 6 in `Calculations`). |
| 2 | Visual → field map | ✅ | See next section. |
| 3 | Unused objects | ❌ | 17 unused measures, 3 unused calc tables, and many unused columns (see below). |
| 4 | Report-level measures | ✅ | No `reportExtensions.json`, so none. |
| 5 | Risky relationships | ❌ | `'Sales Order'[SalesOrderLineKey]` ↔ `Sales[SalesOrderLineKey]` is one-to-one and `bothDirections` (relationships.tmdl, `fedea08d…`). `Sales Data FINAL v2` has no relationships (island). Auto date/time is on (`model.tmdl:12`). `Date` is not marked as a date table (no `dataCategory: Time`). |
| 6 | Used measures explained | ❌ | See "Used measures" below. |

### Terrible Report page: what each visual uses

| Visual | Type | Fields | Measure speed |
|---|---|---|---|
| Fiscal Year buttons | slicer | `'Date'[Fiscal Year]` | — |
| 275K card | card | `[Qty Ordered]` | OK |
| 397 card | card | `[# Products]` | OK |
| Top-left dots | scatter | `SDF[Category]`, `[# Products]`, `[Qty Ordered]`, `[Avg Profit]` | OK |
| Revenue by State-Province | funnel | `SDF[State-Province]`, `[Total Revenue]` | OK |
| Avg profit by model | pie | `SDF[Model]`, `[Avg Profit (Pie)]` | **~22 s** (all years), ~12 s (FY2020) |
| Marcus's table | matrix | `SDF[Subcategory] > [Model] > [Product]`, `[# Products]`, `[Qty Ordered]`, `[Avg Profit]` | OK |
| Model/Product/Cost | table | `SDF[Model]`, `SDF[Product]`, `[# Products]`, `[Std Cost]` | **~9.5 s** |

SDF = `Sales Data FINAL v2`. Every visual except the slicer reads from it, and since it has no relationships, every measure re-applies the year with `[Fiscal Year] IN VALUES('Date'[Fiscal Year])`. The two slow measures explain Marcus's "fifteen, twenty seconds" and "the pie just spins."

### Used measures, in plain English

| Measure | What it really is | Problems |
|---|---|---|
| `Qty Ordered` | Sum of order quantity | FILTER over whole table, fake relationship via `IN VALUES()` |
| `Total Revenue` | Sum of sales amount | FILTER as CALCULATE argument, SUMX on one column |
| `# Products` | Distinct products **that sold** | COUNTROWS(DISTINCT(SELECTCOLUMNS(FILTER()))) |
| `Avg Profit` | **Profit per unit** (profit ÷ qty), not an average | Context transition per row, `/`, IFERROR. Name misleads (Dana, Priya) |
| `Avg Profit (Pie)` | Same as `Avg Profit` | Duplicate, O(n²) `<=` self-filter, multiplies by an always-1 value |
| `Std Cost` | Sum of `Total Product Cost` (standard cost × qty) | Scans `Product List` for each of ~121K rows, multiplies by an always-1 value |

Also in `Calculations` (used only by the hidden sample page): `Profit Margin` is **profit in dollars**, not a %. That's the name Priya says she can't use.

### Things the demo notes don't mention

- **`Product List` is not truly unused.** No visual uses it, but `Std Cost` references `ALL('Product List')`. Rewrite or repoint `Std Cost` before deleting that table, or the cost table breaks.
- **The hidden "Great Report Sample" matrix is broken.** It points at `_Measures.Product Count`, `_Measures.Quantity Ordered`, and `_Measures.Average Profit`, which don't exist in `_Measures` (they live in `Calculations`). Its `Cost` column correctly uses `Calculations`.
- `# Products` (sold products, moves with the year) and `Calculations[Product Count]` (all 397 products, never moves) aren't the same measure. The 397 card on the terrible page now shows sold products, so it may not read 397 anymore.

### Unused objects

- **Measures (17, `_Measures`):** `Total Revenue OLD`, `Total Revenue (2)`, `Revenue LY`, `Revenue YTD`, `Profit %`, `Margin TEMP`, `Greg's Profit`, `Avg Price`, `Order Count`, `Customer Count`, `Bike Revenue`, `% of Total Revenue`, `Revenue Rank`, `Revenue Text Measure`, `Test`, `Measure`, `Measure 2`. None has a format string, description, or folder.
- **Calc tables:** `Model Profit Summary`, `Date Copy` (unused); `Product List` (only `Std Cost`, see above).
- **Columns:** most of the 28 in `Sales Data FINAL v2` once it's gone. In the star tables, unreferenced today: Sales 9 of 18 (`Unit Price`, `Extended Amount`, `DueDateKey`, `ShipDateKey`, the `*Adjusted` keys, …), Date 27 of 35, Reseller 7 of 8, Customer 4 of 7, Product 4 of 9, Sales Order 3 of 4.
- **Don't cut** `'Sales Territory'[Region]`, `[Country]`, `[Group]`: unused today, but they're the territory slicer and revenue-by-territory visual the stakeholders asked for. Confirm the rest with Measure Killer before deleting.

## Keep / Fix / Cut

| Item | Call | Why (who said it) |
|---|---|---|
| Fiscal Year slicer | **Keep** | Marcus: "I pick my fiscal year up here with these buttons." Investigate "(Blank)" (Priya). |
| Subcategory → Model → Product matrix | **Keep, fix underneath** | Marcus: "the only reason I open the report… Don't touch my table." Repoint to `Product` and clean measures. Drop the `# Products` column (always 1 at product level). Rename `Avg Profit` to Profit per Unit or drop it (Priya). |
| Quantity Ordered card (275K) | **Keep** as supporting number | Marcus: "I glance at it." Dana: "I don't run the business on units." |
| Revenue by State-Province funnel | **Fix** → revenue by Sales Territory (bar or map) | Dana: "I manage by sales territory, not by state." Priya: customer state ≠ managing territory. |
| `Profit Margin` measure | **Fix**: rename to Profit, add real Profit Margin % | Priya: "the report's margin is dollars." Must match her spreadsheet. |
| Model/Product/Cost table ($97.2M) | **Cut** (after tracing the number) | Dana: "I'd rather have no number than a wrong one." Priya can't tie it out. It's the unfiltered sum of `Total Product Cost` (standard cost × qty) when no year is selected; confirm against her ERP cost. |
| Scatter chart | **Cut** | Marcus: "never once looked at it on purpose." Dana: "Cut it." |
| Pie chart | **Cut** | Marcus: 40-odd slices, unreadable legend. Also the 22 s visual. |
| Product Count card (397) | **Cut** | Marcus: "same number every time." Dana: swap for revenue and margin %. |
| 17 unused measures, `Model Profit Summary`, `Date Copy`, Auto date/time | **Cut** | Not used; slow the model. |
| `Sales Data FINAL v2`, `_Measures`, `Product List` | **Cut, after repointing** | Every visual depends on them today. |
| **Add:** Revenue and Margin % cards, quarterly trend vs last year, Territory slicer | **New** | Dana (cards, trend), Marcus (territory filter). Top half must be screenshot-ready for the CFO. |
| Email subscription, CRM pipeline, forecasting | **Phase two** | Marcus's wish list; Dana: "Let's get the core right first." |

## Still needs a human (❓)

1. **Blank fiscal year.** Run in DAX query view:
   ```dax
   EVALUATE
   SUMMARIZECOLUMNS('Date'[Fiscal Year], "Revenue", [Revenue], "Rows", COUNTROWS(Sales))
   ```
   A blank row means `Sales[OrderDateKey]` values with no match in `Date[DateKey]`.
2. **$97.2M cost.** Compare `[Cost]` by fiscal year with Priya's ERP cost to decide whether it's wrong or just unfiltered and mislabeled.
3. **Answer key.** Priya's spreadsheet for last fiscal year, by territory, before any rebuild.
4. **Baseline numbers.** Run the Bad vs Clean query from `Demo Model Notes.md` and save the output before Phase 2.
