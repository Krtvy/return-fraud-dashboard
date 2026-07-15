# Return Fraud Detection Dashboard

Detects coordinated return fraud on TikTok Shop affiliate orders, where buyers and affiliate
creators exploit the gap between when a commission is paid and when a return lands.

Built during a Business Intelligence internship at a wellness e-commerce brand, for a live
affiliate programme.

## The problem

Affiliate commission is paid on the sale. If the order is returned *after* the commission
clears, the brand eats the loss twice: refunded revenue plus paid-out commission. At scale this
is invisible in a spreadsheet — the signal only shows up when you correlate returns, affiliate
orders, and the full order book together.

## How it works

Upload three exports (Returns, Affiliate Orders, All Orders) and the engine scores every return
against **18 weighted fraud signals**, then surfaces the results across five views: All Returns,
User Level, Address Level, Creator Level, and Daily Stats.

Signals are grouped by the abuse pattern they catch:

| Pattern | Signals |
|---|---|
| **Affiliate self-dealing** | `creator_self_buy` (weight 40 — the strongest signal), `high_creator_return_rate`, `creator_state_concentration` |
| **Commission-window gaming** | `return_after_commission` (30), `commission_already_paid`, `return_near_commission_window` |
| **Serial returners** | `high_buyer_return_rate`, `repeat_returner`, `duplicate_return_attempt`, `same_day_return` |
| **Sock puppets** | `sock_puppet_phone` (30), `sock_puppet_address` |
| **Geographic rings** | `zip_return_hotspot`, `repeat_address_returns`, `missing_items_cluster_zip`, `state_return_anomaly` |
| **Other** | `suspicious_reason`, `bnpl_payment` |

Each return gets a cumulative risk score, a risk label, and a recommended action — so the
operations team gets a decision, not just a number.

## Stack

Python · Flask · SQLite · Tailwind CSS

## Files

| File | Purpose |
|---|---|
| `app.py` | Flask web server (port 8080) |
| `detector.py` | Scoring engine — 18 weighted rules |
| `database.py` | SQLite storage layer |
| `templates/` | Five HTML views |

## Run

```bash
pip install flask
cd "Return Dashboard"
python3 app.py       # http://localhost:8080
```

Upload the three CSVs exported from TikTok Shop (Seller Center → Returns, Affiliate Center →
Orders, Seller Center → All Orders), then click Run Analysis.

> **Note on data:** source only. No order, buyer, or creator data is included in this repository.
