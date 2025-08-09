# SEM Keyword Scraper + LLM Analysis

## 1) Prerequisites
- Python 3.8+
- Google Chrome or Chromium installed
- Gemini API key

## 2) Setup
```bash
git clone <your-repo-url>
cd SEM
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Install Chrome/Chromium (one-time):
- Ubuntu/Debian:
  - `sudo apt update && sudo apt install -y chromium-browser || sudo apt install -y chromium`
- Fedora/RHEL:
  - `sudo dnf install -y chromium`
- Arch:
  - `sudo pacman -S --noconfirm chromium`
- macOS (Homebrew):
  - `brew install --cask google-chrome`  (or `brew install --cask chromium`)
- Windows:
  - Install Chrome from https://www.google.com/chrome/

Driver note: ChromeDriver is auto-installed at runtime via `chromedriver-autoinstaller`. No manual driver setup needed.

## 3) Configure
Create `.env` in project root:
```env
GEMINI_API_KEY=your-gemini-api-key
```

Edit `config.yaml`:
```yaml
brand_website: "https://www.fastrack.in/"
competitor_website: "https://www.titan.co.in/"
service_locations:
  - "INDIA"

shopping_ads_budget: 5000
search_ads_budget: 8000
pmax_ads_budget: 3000

assumptions:
  ctr: 0.01
  conversion_rate: 0.02
  max_cpc_cap: 2.0
```

## 4) Run
```bash
python run_sem_analysis.py
```
On success, it prints a single line with the output folder name, e.g.:
```
sem_deliverables_YYYYMMDD_HHMMSS
```

List outputs:
```bash
ls -la sem_deliverables_YYYYMMDD_HHMMSS
```

## 5) Outputs
Inside the deliverables folder:
- `kw_YYYYMMDD_HHMMSS.csv` — scraped keywords (top-N per source)
- `search_YYYYMMDD_HHMMSS.csv` — Search campaign (LLM ad groups, intent, match types, suggested CPC)
- `pmax_YYYYMMDD_HHMMSS.csv` — PMax themes (LLM-generated)
- `shop_YYYYMMDD_HHMMSS.csv` — Shopping CPC bids

## 8) How it works
- Orchestrator (`run_sem_analysis.py`):
  - Creates a timestamped output folder.
  - Runs the scraper with `SEM_OUTPUT_DIR` so the keywords CSV is written into that folder.
  - Runs the analysis with `SEM_OUTPUT_DIR` and `SEM_KEYWORDS_FILE` pointing to the scraped CSV.
  - Only prints the output folder on success; emits concise error codes on failure.
- Scraper (`wordstream_scraper.py`):
  - Opens WordStream, inputs the brand/competitor URL, selects the country from `config.yaml`, submits the dialog.
  - Waits for the results table, extracts rows, keeps keywords with `search_volume ≥ 500`.
  - Tags rows with `source` (brand_website or competitor_website).
  - Keeps only top-N per source by `search_volume` (`SEM_TOP_N`, default 10).
  - Saves to `kw_YYYYMMDD_HHMMSS.csv` inside the output folder.
- Analysis (`sem_analysis.py`):
  - Loads the keywords CSV (from `SEM_KEYWORDS_FILE` or most recent `kw_*.csv`).
  - Initializes Gemini (`gemini-1.5-flash`) using `GEMINI_API_KEY`.
  - KPI pass computes volume and bid stats.
  - Ad group creation: batches keywords (15 per call) to the LLM with a JSON-only prompt; robustly parses JSON.
  - Search campaign: computes target CPC from `assumptions` (ctr, conversion_rate, max_cpc_cap) and suggests CPC per keyword by competition and avg bid.
  - PMax themes: sends top keywords to the LLM to return four theme lists; writes `pmax_*.csv`.
  - Shopping bids: budget-splits, estimates clicks/conversions, and recommends CPCs per keyword; writes `shop_*.csv`.
  - Output filenames are short (`search_*`, `pmax_*`, `shop_*`) and saved in the output folder.





