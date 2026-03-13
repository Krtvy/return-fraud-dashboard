# MagAsha Return Fraud Dashboard

Detects suspicious return patterns on TikTok Shop affiliate orders for Root Labs Liposomal Magnesium + Ashwagandha Gummies.

## Setup (One Time)

**Requires Python 3.8+**

```bash
pip install flask
```

## Run

```bash
cd "Return Dashboard"
python3 app.py
```

Then open **http://localhost:8080** in your browser.

## How to Use

1. Go to http://localhost:8080
2. Upload 3 CSV files exported from TikTok Shop:
   - **Return Raw** — from Seller Center > Returns
   - **Affiliate Raw** — from Affiliate Center > Orders
   - **All Orders Raw** — from Seller Center > All Orders
3. Click "Run Analysis"
4. Browse 5 views: All Returns, User Level, Address Level, Creator Level, Daily Stats

## Files

| File          | Purpose                                    |
| ------------- | ------------------------------------------ |
| `app.py`      | Flask web server (port 8080)               |
| `detector.py` | Fraud detection engine (16 weighted rules) |
| `database.py` | SQLite storage layer                       |
| `templates/`  | HTML views (Tailwind CSS)                  |
