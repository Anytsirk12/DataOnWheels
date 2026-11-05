# Report Notes: measure descriptions

Business descriptions for the measures in the rebuilt report, written from the 11.5.26 stakeholder session (Dana Whitfield, Marcus Lee, and Priya Shah). Each description is short enough to paste into the measure's Description box in Power BI Desktop, or above the measure as a `///` line in TMDL.

All measures respond to the Fiscal Year and Sales Territory slicers and to the product hierarchy (Subcategory, Model, and Product). Revenue, cost, and margin are reconciled to Priya's finance spreadsheet for last fiscal year, by territory. That's the definition of done.

## Core measures (Monday numbers)

| Measure | Replaces | Display folder | Format | Description |
|---|---|---|---|---|
| Revenue | `Total Revenue`, `Calculations[Revenue]` | Sales | `$#,0` | Total sales amount for the selected fiscal year, territory, and products. One of Dana's three Monday numbers. Ties to finance's revenue to the dollar. |
| Cost | `Std Cost`, `Calculations[Cost]` | Sales | `$#,0` | Total product cost at standard cost (standard cost × quantity ordered) for the selected filters. Used to calculate Profit and Profit Margin %. Ties to finance's cost to the dollar. |
| Profit | `Calculations[Profit Margin]` (renamed) | Profitability | `$#,0` | Revenue minus Cost, in dollars. This was called "Profit Margin" in the old report, but it's a dollar amount, not a percentage. For the percentage, use Profit Margin %. |
| Profit Margin % | *New* (old `Profit %` and `Margin TEMP` were unused) | Profitability | `0.0%` | Profit as a percentage of Revenue. Shows which product lines actually make money, not just move volume. One of Dana's three Monday numbers. Must match finance's margin. |
| Quantity Ordered | `Qty Ordered`, `Calculations[Quantity Ordered]` | Volume | `#,0` | Total units ordered for the selected filters. A supporting number: the regional managers use it to see volume by bike model, but the business is run on revenue and margin, not units. |
| Profit per Unit | `Avg Profit`, `Avg Profit (Pie)`, `Calculations[Average Profit]` (renamed) | Profitability | `$#,0.00` | Profit divided by Quantity Ordered: the average dollars of profit on each unit sold. This was called "Average Profit" in the old report. It isn't an average of anything else. |

## Year-over-year measures (planned)

Dana asked for revenue and margin compared with the same period last year, and for a quarterly trend. These measures aren't built yet. They need `Date` marked as a date table first.

| Measure | Display folder | Format | Description |
|---|---|---|---|
| Revenue LY | Sales \ Last Year | `$#,0` | Revenue for the same period one fiscal year earlier. Used to compare this year with last year by quarter and by territory. |
| Revenue YoY % | Sales \ Last Year | `0.0%` | Change in Revenue compared with the same period last year, as a percentage. Shows where territories are growing or slipping. |
| Profit Margin % LY | Profitability \ Last Year | `0.0%` | Profit Margin % for the same period one fiscal year earlier. |
| Profit Margin % Change | Profitability \ Last Year | `+0.0%;-0.0%;0.0%` | Change in Profit Margin % compared with the same period last year, in percentage points. Shows whether a product line or territory is turning a corner. |

## Being cut

| Measure | Why |
|---|---|
| `# Products`, `Calculations[Product Count]` | The Product Count card shows the same number every time (Marcus), and in the matrix it's always 1 at the product level (Priya). |
| The 17 unused measures in `_Measures` | No visual, measure, or filter uses them. |

## Open questions before these descriptions are final

- **Cost:** the old cost table showed $97.2M, which Priya couldn't tie to the ERP. Confirm that standard cost × quantity is the cost finance uses before this description goes in.
- **Blank fiscal year:** some sales have order dates with no match in the date table. Decide whether those rows should be excluded or fixed upstream, and note that in the Revenue description.
- **Last fiscal year:** confirm which fiscal year Priya's answer key covers.
