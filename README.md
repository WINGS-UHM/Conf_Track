# Academic Conference Tracker (Conf_Track)

A static academic conference tracker website powered by:
- Curated YAML config for Wireless/Communication conferences
- External conference deadlines from ccf-deadlines (as primary data for other areas)
- A single generated `data/conferences.json` consumed by the front-end

## Repository Layout

- `config/`
  - `tracks.yaml`: stable track definitions used by UI filters (e.g., Wireless & Communication, Security & Privacy, Networking & Systems)
  - `curated_comm_wireless.yaml`: curated conference series list for comm/wireless (keywords, aliases, optional source hints)
  - `sources.yaml`: external data source config (ccf-deadlines, etc.)

- `data/`
  - `conferences.json`: generated output consumed by the website (committed to the repo)
  - `sources/`: optional raw snapshots for debugging/diff

- `web/`
  - `index.html`: the tracker UI (based on conf-track template)

- `tools/`
  - reserved for future scripts (fetch/normalize/merge/validate/export)

## Data Contract (High-level)

The website reads `data/conferences.json` which is an array of conference "instances" (usually one entry per year/cycle) with:
- `name`
- `sub` (categories / tracks, array of strings)
- `Location`
- `Start Date`, `End Date`
- `Abstract Deadline`, `Submission Deadline`, `Notification`
- `link`
- optional: `source`, `series_id`, `ccf` etc.

Note:
- Tracks are defined in `config/tracks.yaml`
- For curated comm/wireless series, track assignment is explicit in YAML (no front-end keyword guessing)

## Local Preview

From repo root:

```bash
cd web
python -m http.server 8000
```

Then open:

http://localhost:8000

## How to Maintain

1. Update curated comm/wireless list:

- edit config/curated_comm_wireless.yaml

2. Update track definitions (rare):

- edit config/tracks.yaml

3. Regenerate data/conferences.json

- (to be added) a tool script will merge curated + ccf-deadlines and export the final JSON



<!-- academic-conference-tracker/
  README.md

  config/
    tracks.yaml                  # Conference Categories/Labels
    curated_comm_wireless.yaml   # comm/wireless conf
    sources.yaml                 # source for data (ccf-deadlines...）

  data/
    conferences.json             
    sources/
      ccf_deadlines.snapshot.json  # Optional
      curated.snapshot.json        # Optional

  web/
    index.html                   # old: conf-track.html
    assets/                      # Optional

  tools/
    README.md               
    schema/
      conferences.schema.md 

  .github/
    workflows/
      (placeholder)              
