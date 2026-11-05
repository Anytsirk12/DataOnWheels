# Phase 1: Inspect

Know what you inherited before you touch it.

**How to use this with an AI helper:** open the PBIP folder in Claude Code (or your agent of choice) and say:

> Work through `Checklists/01 Inspect - First Steps and Stakeholders.md` for this project. For each item, mark it ✅ done, ❌ not done, or ❓ needs a human, and give the evidence (file and line, or what you found). Don't change any files.

The **AI check** column says what the agent can verify from the files. Items marked **Human** need a person or a meeting.

## First steps

| # | Item | AI check |
|---|---|---|
| 1 | Save a copy of the report before changing anything | Is there a backup or Git history? Run `git status` and `git log`, or look for a backup folder |
| 2 | Save the report as a Power BI Project (PBIP) so the model and report are readable text | Do `*.SemanticModel/definition/*.tmdl` and `*.Report/definition/` exist? |
| 3 | Meet with the stakeholders to learn what's working and what isn't. The goal is trust. | **Human.** Then give the agent the meeting transcript or your notes |
| 4 | Break the issues down into manageable tasks | Agent drafts a task list from the transcript and the inventory below |
| 5 | Define "done" with the stakeholders, to avoid a development spiral | **Human.** The agent can pull each person's definition of done out of the transcript |
| 6 | Accept that some things can't be fixed (bad source data, missing history) | Agent lists data-quality issues it finds, such as blank keys or unmatched dates |

## Stakeholder questions

Be nosy. Ask TONS of questions. If you have a transcript, ask the agent to answer each question from it, quote who said what, and list the questions nobody answered.

| # | Question | Answer (with who said it) |
|---|---|---|
| 1 | What decisions will this data drive? | |
| 2 | What are the "make more money" metrics that matter most? | |
| 3 | What grain and scope does the data need? | |
| 4 | Who uses this report, and how do they slice it? | |
| 5 | What dimensions give context to the metrics? | |
| 6 | Where does the data live today? | |
| 7 | Are there data quality landmines waiting to explode? | |
| 8 | How often do these questions need answering? | |

**Pro tip:** email the answers to the stakeholders, and put them on a hidden "About this report" page inside the report, so the report's purpose travels with the report.

## Dissect the code

| # | Item | AI check |
|---|---|---|
| 1 | Inventory every table, column, measure, and calculated table | Read every `tables/*.tmdl`. Count each object type and flag calculated tables (`partition ... = calculated`) |
| 2 | Map which visuals use which fields | Read every `pages/*/visuals/*/visual.json` and list each visual's `Entity` and `Property` references |
| 3 | Find unused measures, columns, and tables | Anything that isn't referenced by a visual, a filter, another measure, a relationship, or a sort-by column. Confirm with Measure Killer before deleting |
| 4 | Check for report-level measures | Does `definition/reportExtensions.json` exist? List any `"Schema": "extension"` references |
| 5 | List relationships and flag the risky ones | Read `relationships.tmdl`. Flag `crossFilteringBehavior: bothDirections`, many-to-many, and inactive relationships |
| 6 | Explain each used measure in plain English and flag anything suspicious | Names that don't match the math (a "margin" that returns dollars), IFERROR, `/` instead of DIVIDE, FILTER over whole tables |

## What's working, what's not

Give the agent the stakeholder transcript **and** the PBIP together:

> Here's the stakeholder transcript and the PBIP. Sort every visual and measure into Keep, Fix, or Cut, and quote who said so.

| Keep | Fix | Cut |
|---|---|---|
| | | |
