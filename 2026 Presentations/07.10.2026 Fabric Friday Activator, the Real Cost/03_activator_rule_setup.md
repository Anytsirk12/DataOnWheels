# Activator Rule Setup Guide

Two themed Activators for the capacity cost talk. The Eye of Sauron is your villain and
your pivot into the cost story. The Palantir is a shorter follow-on that teaches "every
rule is another meter."

A few notes before you start:

- Exact button labels shift between Fabric versions, so the conditions below are written
  in plain terms. Match them to whatever the UI calls them.
- Metrics data lags 10 to 15 minutes, so a rule that starts your cost notebook will not
  have fresh numbers in a live run. Pre-run the notebook for the talk and show the trigger
  firing, then cut to your prepared results.
- The event listener keeps billing until a rule is deleted. For the demo you can leave the
  rules running on purpose, because that drain is exactly what notebook 04 will reveal.

---

## Activator 1: The Eye of Sauron

The headline demo. Corruption climbs as the Ring is carried and the Eye turns when it
crosses the line.

**Source:** the Ring corruption Eventstream (from notebook 01)
**Object id:** `ringbearer`  (groups all of Frodo's readings together)
**Property watched:** `corruption_level`  (range 0 to 100)

### Rule: The Eye Turns

- **Condition:** when `corruption_level` becomes greater than **70**
  - Use the "becomes greater than" style condition so it fires on the upward crossing,
    not once per reading while it sits above 70. That keeps the demo clean.
- **Action 1: Teams message** to your demo channel
  - Message: *"The Eye of Sauron has turned toward Middle-earth. The Ring has been detected
    at {location}. Corruption reads {corruption_level}. Hide, you fools."*
  - Insert `{location}` and `{corruption_level}` using the message editor's property picker.
- **Action 2: Start a Fabric item** and point it at your cost-of-watching notebook (04)
  - This is the narrative pivot. The moment the Eye opens, it launches the investigation
    into what the Eye costs.

### Optional early-warning rule: A Shadow Grows

- **Condition:** when `corruption_level` becomes greater than **40**
- **Action:** a single quiet Teams message: *"A shadow grows in the east. The Ring stirs."*
- Purpose is pacing only. It gives you a beat of tension before the main alert. Skip it if
  you want a tighter demo, but note it is a second listener and a second meter, which you
  can call back to later.

---

## Activator 2: The Palantir

The follow-on. Denethor cannot look away, and the stone runs hotter the longer he stares.
Staring maps onto consuming capacity, and the three tiers below quietly plant the lesson:
three rules means three listeners, all metered.

> This needs its own stream. It is notebook 01 relabeled to emit a `power_draw` metric that
> climbs with gaze time. Ask me to build it and it is a five minute job.

**Source:** the Palantir power draw Eventstream
**Object id:** `seer`  (for example, "Denethor")
**Property watched:** `power_draw`  (range 0 to 100, rises the longer the gaze continues)

### Tier 1: A Shadow Stirs

- **Condition:** when `power_draw` becomes greater than **40**
- **Action:** quiet Teams message: *"A shadow stirs in the stone. Denethor has looked upon it."*

### Tier 2: The Stone Burns

- **Condition:** when `power_draw` becomes greater than **70**
- **Action:** sharper Teams message: *"The Palantir burns. Denethor has looked too long.
  Power draw at {power_draw}."*

### Tier 3: Denethor Is Lost

- **Condition:** when `power_draw` becomes greater than **90**
- **Action:** start a Power Automate flow or a Fabric pipeline as the full alarm, plus a
  Teams message: *"The Lord of Gondor is consumed. Bring wood and oil."*

### The teaching moment

Pause on the fact that this one demo now has three active rules. Each one runs its own
listener, and each listener bills by the hour whether or not anyone is staring into the
stone. That is the whole cost lesson in a single screen, and it sets up the reveal in
notebook 04 perfectly.

---

## Cleanup reminder

When the talk is over, remember the moral. Pausing or stopping these rules does not stop
the listeners. Delete the rules to end the drain. Or leave one running for a week and let
notebook 04 show the cost quietly climbing. That makes a stronger closing slide than any
static number.
