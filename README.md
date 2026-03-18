# WoS → MyOrg v2

UT-centric review and live upload tool for Medical University of Varna.

## Setup

1. **Clone the repo**
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Add your API key** to `config.json`:
   ```json
   { "api_key": "YOUR-CLARIVATE-KEY-HERE", ... }
   ```
4. **Run**: `streamlit run app.py`

## Workflow

| Tab | What happens |
|-----|-------------|
| 📂 1 · Load Files | Upload WoS CSV + ResearcherAndDocument.csv + OrganizationHierarchy.csv, then click Process |
| 🔍 2 · Review & Upload | Work through publications one at a time. For each UT: resolve ambiguous authors, approve/reject, then **Confirm & Upload** — this uploads immediately to MyOrg API |
| 📋 3 · Upload Log | Global summary of all uploads, downloadable as CSV |

## Sidebar

- **API Key** — pre-loaded from `config.json`; can be overridden per session
- **Dry run** — enabled by default when no key is configured; uncheck to go live
- **Test connection** — verifies the key before any uploads
- **Publication list** — jump directly to any UT (🚀 uploaded · ✅ confirmed · ⏳ pending)

## Files

| File | Purpose |
|------|---------|
| `app.py` | Streamlit UI |
| `core.py` | WoS parsing, name matching, extraction |
| `initial_matching.py` | Initial-aware author name matcher |
| `myorg_api.py` | Clarivate MyOrg REST API client |
| `config.json` | MUV affiliation patterns + API key |
| `ResearcherAndDocument.csv` | Current MyOrg researcher roster |
| `OrganizationHierarchy.csv` | Organisation hierarchy |

## config.json reference

```json
{
  "api_key": "",                    ← Your Clarivate API key (keep secret)
  "muv_affiliation_patterns": [],   ← WoS affiliation patterns to match
  "fuzzy_threshold": 0.85           ← Name matching threshold
}
```

> **Security**: `config.json` is in `.gitignore` if you add `api_key`. Consider using a separate `secrets.toml` for Streamlit Cloud deployments.
