# Phase 2: Architect, best practices

The blueprint for the rebuilt report. Use it to design the target, and again to review the result.

**How to use this with an AI helper:**

> Review this project against `Checklists/03 Architect - Visual and Model Best Practices.md`. For each item, mark it ✅, ❌, or ❓ and give the evidence. Then propose a target star schema: the fact tables, the dimension tables, and the relationships.

## Visual best practices

It's all about TRUST.

| # | Practice | AI check |
|---|---|---|
| 1 | About 6–10 insights per page | Count the data visuals per page |
| 2 | Choose the right visual for the job | No pies or donuts with more than about 5 slices. No funnels for categories that aren't stages. Scatter charts need a reason |
| 3 | Include trending visuals | Is there a line or column chart over a date column for the key metrics? |
| 4 | Group related metrics | Check that related visuals sit next to each other (by `position` x and y) |
| 5 | Size visuals to be readable | Flag visuals smaller than about 200 × 150 px, or legends that page |
| 6 | Use a consistent color palette | One theme file, and no hard-coded colors in `objects` that fight it |
| 7 | Add clear headings and labels | Every data visual has a title that says what it shows |
| 8 | Round numbers where it helps | Every measure has a `formatString`, and currency isn't shown to 10 decimal places |
| 9 | Lay it out to read top to bottom, left to right, like a newspaper | Key cards top-left, detail tables bottom-right |

## Model best practices

| # | Practice | AI check |
|---|---|---|
| 1 | Every column and table feeds a visual or a metric | Same cross-reference as the quick wins. List anything unused |
| 2 | STAR SCHEMA ALL THE THINGS | Facts hold the numbers and keys; dimensions hold descriptive columns. No flattened copies of the fact table, no snowflakes |
| 3 | No many-to-many relationships (rare exceptions) | `relationships.tmdl` |
| 4 | Metrics are tested and trusted by the stakeholders | **Human,** plus the verification query in checklist 04 |
| 5 | Relationships flow the right direction, on the right fields | One-to-many from dimension to fact, single direction, integer keys of the same type on both sides |
| 6 | Organize the modeling view | Facts in the middle, dimensions around them. **Human** in Desktop |
| 7 | House metrics in a measures table or display folders | Measures not scattered across dimension tables. Every measure has a `displayFolder` |
| 8 | Know when to build from scratch | If most tables and measures are on the Cut list, say so |

## Star schema in one line

The fact table holds the numbers (sales, revenue, inventory). Dimension tables give the context: who, what, when, and where.
