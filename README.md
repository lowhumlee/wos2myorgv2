# WoS → MyOrg v2 — UT-centric review

Second-generation affiliation ingestion tool for Medical University of Varna.

## What's new vs v1

| Feature | v1 | v2 |
|---|---|---|
| Review flow | Author-centric (all authors grouped by name) | **UT-centric (one publication at a time)** |
| Navigation | Single long scrollable list | Prev/Next + jump-to picker + sidebar mini-map |
| Locking | Save all at once | **Lock each publication individually** |
| Progress | Overall count | **Per-UT progress bar + locked indicator** |
| Undo | — | **Undo individual author decision** |

## Workflow

1. **Load** — upload WoS CSV, ResearcherAndDocument.csv, OrganizationHierarchy.csv
2. **Review** — work through publications one at a time:
   - Auto-confirmed (exact match) and already-in-MyOrg rows are collapsed
   - Each author needing a decision gets an identity picker + org selector
   - Use **Approve** or **Reject** per author
   - For fuzzy/initial matches, use "🔍 Not right? Search full list" to override
   - Once all authors decided → **Lock Publication**
3. **Export** — build upload-ready CSV + excluded rows + new persons list

## Files

| File | Purpose |
|---|---|
| `app.py` | Streamlit UI (v2) |
| `core.py` | Parsing, matching, export (shared with v1) |
| `initial_matching.py` | Initial-aware name matcher (shared with v1) |
| `config.json` | MUV affiliation patterns + thresholds |

## Run locally

```bash
pip install streamlit pandas openpyxl
streamlit run app.py
```

## Deploy to Streamlit Cloud

1. Push this folder to a new GitHub repo
2. Connect at share.streamlit.io → select `app.py`
3. Python 3.9+, no secrets needed
