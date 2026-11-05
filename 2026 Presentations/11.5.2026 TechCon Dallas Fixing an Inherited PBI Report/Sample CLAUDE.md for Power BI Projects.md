# CLAUDE.md: Power BI project rules

<!--
Sample from "How to Fix an Inherited Report" (Kristyna Ferris, TechCon Dallas 2026).
How to use it: rename this file to CLAUDE.md and put it in the root folder of your Power BI Project (the folder that holds the .pbip file).
Claude Code reads it at the start of every session. Other agents read AGENTS.md or .github/copilot-instructions.md, and the same content works there.
Fill in the Project context section, delete the rules that don't apply to you, and add the ones your team learns the hard way.
-->

## Project context (fill this in)

- **Report:** <name>, owned by <new owner>. Inherited from <previous owner>.
- **What the report is for:** <the decisions it drives, in one or two sentences>
- **The metrics that matter:** <for example revenue, margin %, and year-over-year change>
- **Grain and key dimensions:** <for example daily sales by product, sales territory, and fiscal year>
- **Definition of done:** <what each stakeholder said "done" looks like>
- **Keep / fix / cut:** see `docs/stakeholder-notes.md` (the stakeholder transcript and the agreed list)
- **Answer key:** <the spreadsheet or query that the rebuilt numbers must match>

## How to work in this project

1. **Never edit the live copy.** Work in a Git branch or a copy of the PBIP folder. Before any scripted change, back up the `.Report` and `.SemanticModel` folders and confirm Power BI Desktop has the project closed.
2. **Inventory before you change anything.** List the tables, columns, measures, calculated tables, and relationships, and which visuals use each one. Flag what's unused, but don't delete anything until I confirm it.
3. **Work in small batches.** Change one table, one group of measures, or one page at a time. Then stop so I can reopen the project in Desktop and check it.
4. **Show me the diff and explain it** in plain language before moving on.
5. **Prove the numbers didn't change.** Before a refactor, write a DAX query (EVALUATE SUMMARIZECOLUMNS) that returns the key measures by the key dimensions and save its output. Rerun it after the change. Every difference must be either an intended fix that you explain or a bug.
6. **Say when something is untested.** If a change uses a pattern you haven't seen work in this project, tell me before I open it.

## Safety rules for PBIP files

- **Identify items by `.platform`, not by folder name.** In Fabric Git integration, renaming an item changes `displayName` in `.platform`, not the folder name. Read `displayName` and `logicalId` before you edit anything.
- **Edit PBIR and TMDL as raw text.** Don't load JSON and write it back out with `json.load` and `json.dump`. That converts CRLF line endings to LF and reprints floating-point numbers, which buries a one-line change under thousands of diff lines. Save as UTF-8 without a BOM, and keep the existing line endings.
- **Copy new structure from a working example, never from memory.** Before adding a filter, visual, bookmark, or object property, find the same construct elsewhere in this report or a sibling report and copy its exact property names. Desktop validates PBIR against a strict schema and refuses to open a report with even one unknown property. Schemas: https://github.com/microsoft/json-schemas/tree/main/fabric
- **TMDL has no free-standing comments.** The only legal comment is a `///` description immediately above an object. `--`, `//`, and `/* */` at structure level are parse errors, although `--` is fine inside a DAX expression. Every column of a calculated table needs `sourceColumn: [Name]`.
- **Check for report-level measures before you rename or repoint measures.** Open `definition/reportExtensions.json`. A reference with `"Schema": "extension"` is a measure that lives in the report, even when it has the same name as a model measure.
- **When you swap a field in a visual, sweep every reference to it.** That includes `queryRef` keys and also plain string lists, such as matrix `expansionStates[].levels[].queryRefs` and bookmark copies, which hold `"Table.Column"` strings. Set `displayName` to the old name so visible headers don't change.
- **Don't hand-edit undocumented files:** `report.json` settings you don't understand, `mobileState.json`, `diagramLayout.json`, `semanticModelDiagramLayout.json`, or the auto date tables. Source: https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview
- **Never send write commands to Desktop's local Analysis Services engine** (for example a TMSL refresh over its local port). Read-only DAX queries are fine. Ask me to click Refresh instead.
- **Keep `cache.abf` and `.pbi/localSettings.json` out of Git.**

## Modeling standards

- **Star schema.** Fact tables hold the numbers, and dimension tables hold the context. No snowflaking, and no wide flattened copies of the fact table. https://learn.microsoft.com/en-us/power-bi/guidance/star-schema
- **Shape data in Power Query or upstream, not DAX.** Prefer source or Power Query columns over calculated columns, and keep calculated tables to an absolute minimum. https://learn.microsoft.com/en-us/power-bi/guidance/import-modeling-data-reduction
- **Remove unused tables, columns, and measures.** It's easier to add a column back later than to carry one nobody uses.
- **One date table, marked as a date table, and Auto date/time turned off.** https://learn.microsoft.com/en-us/power-bi/guidance/auto-date-time
- **Single-direction, one-to-many relationships.** Bidirectional or many-to-many relationships need a written reason. https://learn.microsoft.com/en-us/power-bi/guidance/relationships-bidirectional-filtering
- **Integer keys** with matching data types on both sides of a relationship. Use Fixed Decimal instead of floating point for currency.
- **Hide foreign keys** and any fact column that a measure already aggregates. Set Summarize By to None on numeric columns that aren't meant to be summed.
- **Split high-cardinality columns.** For example, split a datetime into a date and a time, and drop unique text columns that no visual uses.

## DAX standards

All DAX best-practice articles: https://learn.microsoft.com/en-us/dax/best-practices/

- **Use explicit measures only.** No implicit aggregations in visuals.
- **Use DIVIDE instead of `/`** for any denominator that can be zero.
- **Don't use IFERROR or ISERROR.** Handle the expected case directly.
- **Use VAR/RETURN** to name an expression once and reuse it.
- **Use Boolean filters in CALCULATE** (with KEEPFILTERS where needed), not FILTER over a whole table. Use FILTER only when comparing measures or several columns.
- **Avoid context transition inside large iterators** (CALCULATE or a measure reference inside SUMX over a fact table) unless you've measured that it's needed.
- **Use relationships, not virtual ones.** Don't fake a relationship inside every measure with TREATAS or `IN VALUES()` when a real relationship would do.
- **References:** qualify column references (`'Table'[Column]`), and never qualify measure references (`[Measure]`).
- **Format DAX the way DAX Formatter does.** https://docs.sqlbi.com/dax-style/dax-format
- **No duplicate measures.** If two measures have the same definition, keep one.
- **Change log:** when you edit an existing measure, add a comment line in the form `// M.D.YY <initials>: <what changed and why>`.

## Naming and documentation

- **Business-readable names** that the stakeholders would recognize, with spaces, title case, and no prefixes like `tbl_` or `fact_` on visible objects. https://docs.sqlbi.com/dax-style/dax-naming-conventions
- **Name a measure for what it calculates.** If "Profit Margin" returns dollars, rename it "Profit" and add a real "Profit Margin %".
- **Every measure gets a format string, a description, and a display folder.**
- **Keep measures in one place:** a measures table or display folders, not scattered across dimension tables.

## Report and visual standards

- **About 6–10 insights per page.** Push detail to drillthrough and tooltip pages. https://learn.microsoft.com/en-us/power-bi/guidance/power-bi-optimization
- **Choose the right visual for the job.** Use bar charts instead of pies or donuts with more than a few slices, and don't use funnels for categories that aren't stages.
- **Show a trend** for any metric whose change over time matters.
- **Lay pages out like a newspaper:** the most important numbers top-left, reading left to right and top to bottom.
- **Keep the report's purpose inside the report:** a hidden "About this report" page with the stakeholder answers, the definition of done, and the owner.

## Further reading

- Tabular Editor Best Practice Analyzer rules: https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules
- Microsoft's agent-instruction examples for Fabric: https://github.com/microsoft/skills-for-fabric
- Power BI agentic development plugins (Kurt Buhler): https://github.com/data-goblin/power-bi-agentic-development
