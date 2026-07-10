# Activator Cost Demo Runbook

**Purpose:** run an end to end demo that shows what a Microsoft Fabric Activator actually
costs, framed as a Lord of the Rings story. A cheerful little rule (the One Ring) quietly
drains capacity in the background, and we hunt it down and put a number on it.

**The one line the audience should leave with:** the cost of an Activator is mostly the rent
its listener charges for standing watch, not the work it does when something happens. Pausing
or stopping a rule does not stop that drain. Only deleting the rule does.

---

## The story arc

- **The temptation.** A rule is trivial to create and feels harmless. Precious, even.
- **The corruption.** Its event listener bills every hour, whether or not any events arrive.
- **The reveal.** We break the cost down by operation and show how much is pure uptime.
- **Destroying the Ring.** The only ways to stop the bleed are to delete the rule or pause
  the whole capacity.

Two Activators carry the story. The **Eye of Sauron** is the villain and the pivot into the
cost reveal. The **Palantir** is an optional shorter follow-on that teaches "every rule is
another meter."

---

## 1. What you need

**Environment**

- A Fabric workspace on a capacity. Eventstreams want at least an F4.
- A Lakehouse attached to the cost notebook (for the history table).
- The Fabric Capacity Metrics app installed, and access to the workspace it lives in.
- Contributor or higher on the workspace where the Activator will live.

**Demo assets (share these alongside this runbook)**

| File | What it is |
| --- | --- |
| `01_ring_corruption_stream.ipynb` | Generates Ring corruption events and streams them in |
| `03_activator_rule_setup.md` | Exact rule definitions for the Eye of Sauron and the Palantir |
| `04_cost_of_watching.ipynb` | The two-act story notebook: Activator looks cheap, then the eventstream reveal. Runs on the working day-grain pull |
| `06_export_to_lakehouse.ipynb` | Manual fallback: loads a CSV you export from the app into a Lakehouse Delta table |
| `07_capacity_cu_by_operation.ipynb` | The automated pull: per-operation CU by day and hour into Delta tables, schedulable |

**Section 11 is the map of what is and is not queryable from code**, and it is worth reading
before the talk. Short version: the per-timepoint detail is sealed by Microsoft, but the
day-grain and hour-grain aggregates are queryable, which is what notebooks 04 and 07 run on.
Notebooks 02 and 05 were removed because they chased the sealed timepoint tables and never
worked; section 11 records why so nobody rebuilds them.

**Timing note:** Metrics data lags 10 to 15 minutes, and listener uptime bills hourly. Give a
new rule an hour or two of runtime before you expect clean numbers. Do a rehearsal run well
ahead of the live talk.

---

## 2. Build the Eventstream (the bridge to the Ring)

Your goal in this section is to get two values into the config cell of notebook 01:
`EVENTHUB_CONN_STR` and `EVENTHUB_NAME`.

1. **Create the Eventstream.** New item, choose Eventstream, name it `ring-corruption-stream`.
   Leave Enhanced capabilities on (the default). Fabric provisions the event hub behind it
   automatically, so there is no Azure setup to do.
2. **Add a custom endpoint source.** In edit mode, select Add source, then Custom endpoint.
   Name it `ring-sensor`. This reserves an Event Hub compatible endpoint your notebook pushes to.
3. **Publish and open the keys.** Select Publish. Switch to Live view, click the custom
   endpoint tile, and in the Details pane choose the **Event Hub** tab, then the
   **SAS Key Authentication** page (sometimes labeled Keys).
4. **Copy the two values into notebook 01:**
   - Connection string primary key goes into `EVENTHUB_CONN_STR`
   - Event hub name goes into `EVENTHUB_NAME`

   The connection string is in the default Event Hub format and works with the Azure Event
   Hubs SDK the notebook uses. It usually already ends with an `EntityPath=...` segment, which
   is the hub name baked in. Make sure the name you paste matches that value. An "entity path"
   error means the two disagree.

> **Security:** the connection string is a live secret. Treat the notebook like it holds a
> password and do not commit it to shared source control.

---

## 3. Run the Ring corruption stream

1. Open `01_ring_corruption_stream.ipynb` in the workspace.
2. Run it once with `DRY_RUN = True` to watch the corruption climb with no endpoint needed.
   Good for a dry rehearsal or a laptop with no connection.
3. When ready to go live, confirm the first cell installed `azure-eventhub`, set
   `DRY_RUN = False`, and run. You will see readings print as they send, one per tick.
4. **Verify before moving on.** Back in the Eventstream, open the Data preview tab. You should
   see rows arriving with `corruption_level`, `location`, and `nazgul_nearby`. If data lands
   here, the Ring is talking. If it does not, fix that before touching the Activator.

---

## 4. Wire the Eye of Sauron Activator

Full rule text is in `03_activator_rule_setup.md`. In short:

- **Source:** the `ring-corruption-stream` Eventstream. Add a Fabric Activator destination
  (needs Contributor on the target workspace).
- **Object id:** `ringbearer`. **Property watched:** `corruption_level`.
- **Rule "The Eye Turns":** when `corruption_level` becomes greater than **70**, then:
  1. Post a Teams message: *"The Eye of Sauron has turned toward Middle-earth. The Ring has
     been detected at {location}. Corruption reads {corruption_level}."*
  2. Start a Fabric item pointed at the cost notebook (04).

Use the "becomes greater than" style condition, not "is greater than," so it fires once on the
upward crossing rather than spamming the channel on every reading above the line.

> **Live-demo caveat:** because Metrics data lags, the notebook triggered in step 2 will not
> have fresh numbers during a live run. Pre-run it for the talk, show the trigger firing, then
> cut to your prepared results.

---

## 5. Optional: the Palantir escalation demo

This one needs its own stream that emits a `power_draw` metric climbing with gaze time. It is
notebook 01 relabeled. Ask for it if you want both demos wired.

Set three tiered rules on the same `power_draw` property (full text in guide 03):

- **Tier 1, A Shadow Stirs**, at `power_draw` > 40: a quiet Teams whisper.
- **Tier 2, The Stone Burns**, at `power_draw` > 70: a sharper alert.
- **Tier 3, Denethor Is Lost**, at `power_draw` > 90: trigger a flow or pipeline.

The teaching beat: this one demo now runs three listeners, each metered by the hour. That is
the cost lesson in a single screen.

---

## 6. Measure what it cost

There are three ways to get the numbers, and you no longer have to open the app for the main one.
Full detail on why some paths work and others do not is in section 11.

**The story, live (notebook 04).** Open `04_cost_of_watching.ipynb` in the Capacity Metrics
workspace with a Lakehouse attached, set the demo item names in the config cell, and run it. It
pulls per-operation CU from the day-grain aggregate table, then walks the two-act story: Act 1
shows the Activator alone looking cheap, Act 2 adds the eventstream and it dominates, broken down
into its standing-guard operations. It projects the observed daily rate out to a year and can draw
the eventstream-versus-Activator bar chart for a slide. This is the one to drive during the talk.

**The automated pull (notebook 07).** `07_capacity_cu_by_operation.ipynb` runs the same query on a
rolling window and writes per-operation CU into daily and hourly Delta tables, safe to rerun and
schedule. Use this to build a real trend over days rather than a single snapshot. Anchor it on a
schedule and the tables stay current on their own.

**The manual fallback (notebook 06).** If a Metrics app update ever breaks the query, or you would
rather point and click, `06_export_to_lakehouse.ipynb` loads a CSV you export from the app's
operations visual into a Delta table. It always works because it does not depend on the model
staying queryable. It is the belt-and-suspenders option.

The app itself is still the ultimate source of truth, so if a number ever looks off, check it
there. But for this demo the day-grain pull returns the real figures, so 04 and 07 are the
primary tools now.

---

## 7. Suggested talk track

1. **Hook.** "I always heard Activator was expensive. I wanted to know how expensive."
2. **Meet the watchman.** Fire the Eye of Sauron live. The Teams alert lands on screen. This is
   what an Activator does: it watches and it raises the alarm.
3. **The turn.** That same watchman never sleeps, and it bills for every hour it stands guard.
4. **The reveal.** Run notebook 04. Act 1 shows the Activator alone, almost nothing. Then add the
   eventstream and watch the number jump, with the eventstream's own operations, the hourly
   connector and the processor, doing most of the spending while nothing happens.
5. **Scale it.** Show the yearly projection. "This is one rule nobody deleted."
6. **The trend (if you have it).** Show the history chart climbing as test rules accumulate.
7. **Destroy the Ring.** Close on the fix: delete stale rules, and remember that pausing a rule
   does not stop its listener. Audit regularly.

---

## 8. Cleanup

After the talk, decide deliberately:

- To stop all consumption, **delete** the demo rules. Pausing or stopping them leaves the
  listeners running.
- Or leave one rule alive on purpose and let the history table keep growing, so your next
  telling has an even longer trend line.
- Pause the demo capacity if it is not otherwise in use.

---

## 9. Troubleshooting

### Data flow

- **No data in Eventstream preview:** `DRY_RUN` is still True, or the source was not published.
- **Entity path error from the notebook:** `EVENTHUB_NAME` does not match the EntityPath in the
  connection string.

### Connecting the Activator (this one bit hard, read it)

Wiring the eventstream to the Activator is the fussiest step. The errors below all look scary
but most are environmental, not problems with your build. Symptoms and fixes, in the order they
tend to appear:

- **"Failed to create data source... Failed to call reflex InitializeArtifact API... BadRequest...
  Request Too Long. HTTP Error 400. The size of the request headers is too long."**
  This is the big one, and it is **not** your setup, your capacity, or your Activator. It means
  your browser is sending oversized auth cookies, so the request is rejected before it runs. A
  brand new Activator fails identically because the cause is the browser, not the item. Fixes:
  clear cookies for the Microsoft sites (`powerbi.com`, `fabric.microsoft.com`, `microsoft.com`,
  `microsoftonline.com`, `windows.net`) and sign back in, or use a private/incognito window.
  Note that a long incognito session slowly refills its own cookies and the error can return, so
  do the wiring early in a fresh session. If it persists even in a truly clean session, the cause
  may be a large number of Entra security groups inflating your token, which is an admin
  conversation, not more clicking.

- **THE FIX THAT WORKED for us (use this path):** delete the failed Activator destination from the
  eventstream canvas, **Publish** the eventstream, open it in a fresh **incognito** window, and
  use the **Set alert** button in the ribbon rather than Add destination. Set alert runs a
  lighter call that avoids the header bloat and creates plus wires the Activator in one step.
  Building from the Activator side with **Get data** is a good alternative for the same reason.

- **"Please wait until the previous operation is completed before initiating the new call."**
  A race condition from clicking activate, toggle, or Activate all more than once. Stop clicking,
  wait a minute or two, Refresh once, and read the tile. Do not reactivate on top of a pending call.

- **Toast says "being activated" but the tile is still Inactive with no error message.**
  That is the in-between provisioning state, not a failure. Wait, Refresh, and it should flip to
  Active in roughly 10 to 30 seconds. If it sits past a minute or two, toggle the tile or use
  Activate all once. An empty Message field on the Details pane is the green light.

- **Activate the destination is seconds. Seeing cost is not.** Do not judge success by watching
  for CU numbers, which lag 10 to 15 minutes. Judge it by the tile going Active with an empty
  Message.

### Cost notebooks (04 and 07)

- **A column or table name errors (cannot be found):** a Metrics app update renamed something.
  Rerun the schema probe from section 11 (`EVALUATE INFO.VIEW.COLUMNS()`) to get your version's
  real names and adjust the query. Microsoft renames these between versions, which is why FUAM
  itself ships several variants.
- **"does not have permission to call the Discover method":** expected on some accounts. Do not
  use `fabric.list_tables` or `list_columns`, which use Discover. Use the INFO.VIEW queries
  instead, which run through the query engine that is open to you.
- **Zero rows for a day you know was busy:** the model's clock can run ahead of your wall time, so
  the date you picked may not be where the data sits. The notebooks anchor on the model's newest
  date to avoid this; widen the window if needed.
- **Numbers look tiny in the app's CU percentage view:** that view smooths background operations
  over 24 hours and understates short runs. Trust the CU seconds the query sums.

### Object id

- **"ringbearer isn't unique" worry:** the prereq wants a key that names which object an event
  belongs to (a grouping key), not a value that differs on every row. Every event carrying
  "Frodo Baggins" is correct: Activator treats it as one object and tracks its corruption over
  time, exactly as `PackageID` groups many temperature readings. Add more bearers only if you
  want multiple independent objects.

---

## 10. Accuracy notes for the stage

- Quote your own measured numbers, not fixed rates. Microsoft can change consumption rates with
  notice, so a live figure from your capacity is safer than a published rate.
- Only the listener projects cleanly by the hour. Filtering and delivery scale with event
  volume, so do not extrapolate those linearly from a short demo run.
- A full year is 8,760 hours only if the capacity never pauses. If you pause overnight, the real
  annual figure is lower, which is itself a nice reinforcement of the lesson.

---

## 11. The programmatic-CU investigation: what you can and cannot query

This section is the map, so the next person inherits it instead of the maze. The headline: the
per-timepoint detail is sealed by Microsoft, but the day-grain and hour-grain aggregates are
queryable, and that is enough to get per-operation CU from code. Notebooks 04 and 07 run on that
working path. Here is the whole picture, including the walls, so nobody re-digs the dead ends.

### What is sealed (and why 02 and 05 were removed)

- **The per-timepoint detail tables do not answer queries.** Tables like
  `Timepoint Background Detail` return zero rows even on an unfiltered `EVALUATE TOPN(5, ...)`, and
  zero rows across a 200-timepoint scan. They are materialized on demand, only while the app is
  rendering them, through a live connection to a shared Kusto (KQL) instance over a custom
  connector Microsoft does not expose. A community expert who tried the same, including heavy XMLA
  work, hit the identical dead end:
  https://community.fabric.microsoft.com/t5/Fabric-platform/API-programmatically-gather-storage-capacity-CU-metrics-for/m-p/4853169
- **The dynamic CU measures return blank cold.** The measures the app shows need its timepoint
  slicer context, so queried without it they come back 0 or blank.
- Old notebooks 02 and 05 chased exactly these sealed tables. They never worked and were removed.
  02 also used `fabric.list_tables`/`list_columns`, which call the Discover method that some
  accounts (including this demo's) are blocked from.

### What works: the day-grain and hour-grain aggregates

The Capacity Metrics model also holds pre-aggregated tables that are ordinary stored data, not the
live passthrough. These answer a plain DAX query. The two that matter:

- `Metrics By Item Operation And Day`
- `Metrics By Item Operation And Hour`

Both carry `Capacity Id`, `Date` (and `Datetime` for hourly), `Item Id`, `Operation name`,
`CU (s)`, `Duration (s)`, `Operations`, and more. This is exactly where Microsoft's FUAM
accelerator gets its per-operation numbers, in its `03_Transfer_CapacityMetricData_ItemOperation`
notebook. We replicated its approach and it returns real CU on a trial capacity.

The working query, run with `fabric.evaluate_dax(dataset, dax, workspace=...)`:

```
DEFINE
    MPARAMETER 'CapacitiesList' = { "<capacity-id>" }
    VAR core =
        SUMMARIZECOLUMNS(
            'Items'[Item name],
            'Items'[Item kind],
            'Metrics By Item Operation And Day'[Operation name],
            'Metrics By Item Operation And Day'[Date],
            FILTER(Capacities, Capacities[Capacity Id] = "<capacity-id>"),
            FILTER(
                KEEPFILTERS(VALUES('Metrics By Item Operation And Day'[Date])),
                'Metrics By Item Operation And Day'[Date] = DATE(2026, 7, 9)
            ),
            "CU_s",  SUM('Metrics By Item Operation And Day'[CU (s)]),
            "Dur_s", SUM('Metrics By Item Operation And Day'[Duration (s)]),
            "Ops",   SUM('Metrics By Item Operation And Day'[Operations])
        )
    EVALUATE FILTER(core, [CU_s] > 0) ORDER BY [CU_s] DESC
```

The two things that made it work, after a lot of dead ends: the `MPARAMETER 'CapacitiesList'`
parameter, which scopes the aggregate to your capacity, and filtering on a plain `[Date]`, not a
30-second timepoint. Aggregates do not need the live passthrough, so they return.

### The two gotchas that cost us the most time

- **Names drift between app versions.** Our version uses `Capacity Id` (space), `Item kind`
  (lowercase k), `Timepoint` on the `Timepoints` table, and the detail table with spaces. Yours
  may differ. FUAM itself ships five query variants (v37 through v53) keyed off a version probe
  for this reason.
- **The Discover method is blocked on this account.** So `fabric.list_tables` and `list_columns`
  fail. To read the schema, query it instead, which runs through the open path:

```
EVALUATE INFO.VIEW.COLUMNS()
```

  Group the result by table and you get every table and column name your version actually uses.
  This is how we recovered the real names after each "cannot be found" error.

### The clock offset

The model's newest timepoints read days ahead of wall-clock time (we saw July 18 stamps while it
was the 10th). So a wall-clock "last 3 days" can miss the data. Both notebooks anchor on the
model's own max date to sidestep this. If a day returns zero rows unexpectedly, widen the window.

### Honest caveats for the automated path

- This use of the Metrics model is not officially supported by Microsoft, and can break on an app
  update. That is why notebook 06 (manual CSV export) exists as a fallback.
- Reading the model over XMLA counts as an interactive operation, so scheduled pulls add a little
  CU of their own, a fitting footnote for a talk about the cost of watching.
- The community request to expose per-run CU properly is still open and worth an upvote:
  https://community.fabric.microsoft.com/t5/Fabric-Ideas/Understanding-CU-Consumption-for-Fabric-for-Fabric-Job-Runs/idi-p/5049936
- Microsoft's FUAM accelerator, which does all of this at tenant scale, lives in the fabric-toolbox
  repo: https://github.com/microsoft/fabric-toolbox/tree/main/monitoring/fabric-unified-admin-monitoring

### Other paths, for completeness

- **The Capacity Metrics app, read directly**, remains the source of truth if a number looks off.
- **The app's Export Data page** exports underlying data (sampling can occur); notebook 06 ingests
  that CSV.
- **Workspace Monitoring** gives a queryable KQL Eventhouse but logs activities and CPU time, not
  CU, so it does not replace this.
- **The Azure bill / Cost Management** is accurate but only at capacity level, not per operation.
- **The Fabric Monitoring Hub and Log Analytics export** are the roadmap direction for a fully
  supported version of this.

### Bottom line

Per-timepoint CU is app-only. Per-operation CU by day and by hour is reachable from code through
the aggregate tables, which is what notebooks 04 and 07 use. For the talk, that means the
eventstream-versus-Activator story runs on live numbers, and you can also just confirm against the
app. The good on-stage point stands: Fabric will bill you for a listener that never fires, the
finest-grained view is locked inside one app, and getting even the day-grain numbers out took
borrowing Microsoft's own accelerator technique.
