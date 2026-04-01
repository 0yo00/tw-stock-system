# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a **Taiwan Stock Market Short-Term Trading Analysis System** (台股短線系統 V178) — a single-process Python/Streamlit web application. There is no database; all persistence uses local JSON files. No API keys or secrets are required; all external data sources (Yahoo Finance, TWSE OpenAPI, TPEx) are public.

### Running the application

```
python3 -m streamlit run tw_stock_v31.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
```

Use `python3 -m streamlit` rather than bare `streamlit` — the latter may not be on `PATH` depending on the pip install location.

### Linting

No linter is configured in the repo. You can use `pyflakes` for basic checks:

```
python3 -m pyflakes tw_stock_v31.py candidate_engine.py
```

Pre-existing warnings (unused imports/variables) are expected and should not be "fixed" without explicit instruction.

### Testing

There are no automated tests in this repository. Manual testing is done by running the Streamlit app and interacting with it in the browser on port 8501.

### Key files

| File | Purpose |
|---|---|
| `tw_stock_v31.py` | Main application (~8600 lines) |
| `candidate_engine.py` | Stock analysis/scoring engine |
| `tw_stock_names.json` | Static stock code → name mapping |
| `requirements.txt` | Python dependencies |

### Notes

- The app UI is entirely in Traditional Chinese (繁體中文).
- The app fetches live market data from Yahoo Finance and Taiwan stock exchange APIs, so internet access is required.
- The devcontainer config (`.devcontainer/devcontainer.json`) targets Python 3.11, but the app works fine on Python 3.12+.
