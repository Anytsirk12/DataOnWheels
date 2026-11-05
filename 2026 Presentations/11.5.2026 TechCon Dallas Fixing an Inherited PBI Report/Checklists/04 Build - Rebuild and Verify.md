# Phase 3: Build

Rebuild it right, with AI as a helper. AI is a fast junior developer: give it rules, small tasks, and a reviewer.

**How to use this with an AI helper:**

> We're starting the rebuild. Follow `Checklists/04 Build - Rebuild and Verify.md`. Before any change, write the verification query and tell me where you saved its output. Then do the first batch only, and stop so I can reopen the report in Desktop.

## Rebuild with AI, safely

| # | Rule | How |
|---|---|---|
| 1 | Work on a copy, never on live | PBIP in a Git repo, so every change is a diff you can review. Back up the `.Report` and `.SemanticModel` folders before any scripted change |
| 2 | Give AI your rules | A `CLAUDE.md` (or `AGENTS.md`) with your star schema, naming, and DAX standards. See the sample CLAUDE.md |
| 3 | Small batches | One table, measure group, or page at a time, then reopen in Desktop. Close Desktop before the agent edits files |
| 4 | You approve every change | AI drafts. You decide what ships |

## Trust, but verify the numbers

If the new report doesn't match the old one, nobody will trust either of them.

| # | Step | How |
|---|---|---|
| 1 | Snapshot the old numbers | Run a DAX query of the key measures by the key dimensions against the original, and save the output |
| 2 | Rerun on the new model | Same query, same filters, against the rebuilt model |
| 3 | Diff and explain | Every difference is either a fix you can explain or a bug |

Example verification query (run it in DAX query view or DAX Studio):

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Fiscal Year],
    "Old Revenue", [Old Revenue Measure],
    "New Revenue", [New Revenue Measure],
    "Difference", [New Revenue Measure] - [Old Revenue Measure]
)
ORDER BY 'Date'[Fiscal Year]
```

## Measure before and after

| What | Tool |
|---|---|
| Visual load time | Performance Analyzer in Desktop (clear the cache or reopen the file between runs) |
| Query time and storage vs. formula engine split | DAX Studio, Server Timings, with Clear Cache before each run |
| Model size | VertiPaq Analyzer in DAX Studio, or the size of `.pbi/cache.abf` |
| Unused objects and lineage | Measure Killer |
