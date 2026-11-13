# Milestone 2 follow-ups

Planning material, like the rest of `docs/process/`. These are review findings from milestone 2 that were deferred on purpose, grouped by the milestone where each should land. None blocked the merge.

## Milestone 5

- moved to milestone 6: Stale week-4 histogram entries leak in `PopulationBaselines` when a bucket's hour gets no scored minute for a week. Sweep weeks below `w - 4` on each timer.
- done (milestone 5): No test drives the generator's minute-to-second seam loop. (the delay run crosses that seam.)

## Milestone 6

Decided in the milestone 6 plan's follow-up table (`docs/process/plans/2026-11-13-m6-readme-design.md`). What stays open is in `open-items.md`.

- The band panel has no per-signal units, and the cold-key lower line comes from `greatest()` with a NULL median.
- `demo.sh` counts its 15-minute give-up from the submit, not from the start, and prints the refusal on stdout.
