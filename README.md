# BFILMY Data

Daily-refreshed movie box-office + advance-booking JSON.

## Modes

The builder runs in one of two modes:

- **FULL** — first run (empty cache) or after `--fresh` / `--force-full`.
  Fetches every day from 2023-01-01 up to today, plus the 45-day
  advance horizon. Skipped days are those already in `state.json`.

- **INCREMENTAL** — every subsequent run. Fetches only:
  - last **5 days** of `daily` (source can revise recent numbers)
  - **today → today + 45 days** of both `daily` and `advance`

In either mode, only movies whose data was written during the current
run get their JSON file rewritten.

## Output

One file per movie: `data/<slug>.json`

```json
{
  "movie": "Hubba",
  "slug": "hubba",
  "formats": ["2D"],
  "languages": ["Bengali"],
  "startdate": "2024-01-19",
  "lastdate": "2024-03-21",
  "versions": [{
    "format": "2D",
    "language": "Bengali",
    "boxoffice": {
      "daily": {
        "2024-01-19": [225972.0, 8.0, 81, 0, 0, 14832, 185400, 0]
      },
      "citywise": {
        "Kolkata": {
          "s": "West Bengal",
          "d": {"2024-01-19": [180233.0, 9.4, 42, 0, 0, 12800, 136400, 0]}
        }
      },
      "chainwise": {"PVR": {"2024-01-19": [140000.0, 8.1, 28, 0, 0, 9200, 113000, 0]}},
      "timewise": {"E": {"2024-01-19": [22, 0, 0, 5400, 89000.0, 8.7, 16.48]}}
    },
    "totals": {
      "boxoffice": [5589405.0, 8.2, 1832, 0, 0, 349290, 4252600, 5]
    }
  }],
  "summary": {
    "formatwise": {"2D": [5589405.0, 8.2, 1832, 0, 0, 349290, 4252600, 5]}
  }
}
