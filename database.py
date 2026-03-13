"""
Database layer — SQLite persistence for fraud detection runs, results, and actions.
Stores history, creator profiles, and approve/reject decisions.
"""

import sqlite3
import os
import json
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fraud_dashboard.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date TEXT NOT NULL,
        total_returns_all INTEGER,
        mag_returns INTEGER,
        mag_affiliates INTEGER,
        mag_orders INTEGER,
        critical_count INTEGER,
        high_count INTEGER,
        medium_count INTEGER,
        low_count INTEGER,
        total_refund REAL,
        total_commission_risk REAL,
        at_risk_refund REAL,
        at_risk_commission REAL,
        overall_return_rate REAL,
        self_buy_count INTEGER,
        sock_puppet_count INTEGER
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS flagged_returns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        risk_level TEXT,
        fraud_score INTEGER,
        recommended_action TEXT,
        flags TEXT,
        return_order_id TEXT,
        order_id TEXT,
        buyer_username TEXT,
        product TEXT,
        return_reason TEXT,
        return_type TEXT,
        refund_amount REAL,
        order_amount REAL,
        commission_at_risk REAL,
        commission_paid TEXT,
        return_status TEXT,
        return_sub_status TEXT,
        time_requested TEXT,
        days_since_delivery TEXT,
        creator TEXT,
        state TEXT,
        city TEXT,
        zipcode TEXT,
        payment_method TEXT,
        buyer_note TEXT,
        buyer_unique_orders INTEGER,
        buyer_unique_returns INTEGER,
        buyer_return_rate TEXT,
        user_action TEXT DEFAULT NULL,
        user_notes TEXT DEFAULT NULL,
        action_date TEXT DEFAULT NULL,
        FOREIGN KEY (run_id) REFERENCES runs(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS creator_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_username TEXT UNIQUE NOT NULL,
        total_mag_orders INTEGER DEFAULT 0,
        total_mag_returns INTEGER DEFAULT 0,
        total_commission_at_risk REAL DEFAULT 0,
        self_buy_count INTEGER DEFAULT 0,
        highest_fraud_score INTEGER DEFAULT 0,
        avg_fraud_score REAL DEFAULT 0,
        risk_tier TEXT DEFAULT 'LOW',
        first_seen TEXT,
        last_updated TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS daily_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        orders INTEGER DEFAULT 0,
        returns INTEGER DEFAULT 0,
        refund REAL DEFAULT 0,
        commission REAL DEFAULT 0,
        FOREIGN KEY (run_id) REFERENCES runs(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS overview_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        data_json TEXT NOT NULL,
        FOREIGN KEY (run_id) REFERENCES runs(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flagged_return_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        order_id TEXT,
        buyer_username TEXT,
        old_action TEXT,
        new_action TEXT,
        notes TEXT,
        action_date TEXT NOT NULL,
        FOREIGN KEY (flagged_return_id) REFERENCES flagged_returns(id),
        FOREIGN KEY (run_id) REFERENCES runs(id)
    )""")

    conn.commit()
    conn.close()


def save_run(stats):
    conn = get_db()
    c = conn.cursor()
    c.execute("""INSERT INTO runs (
        run_date, total_returns_all, mag_returns, mag_affiliates, mag_orders,
        critical_count, high_count, medium_count, low_count,
        total_refund, total_commission_risk, at_risk_refund, at_risk_commission,
        overall_return_rate, self_buy_count, sock_puppet_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        stats["total_returns_all"],
        stats["mag_returns"],
        stats["mag_affiliates"],
        stats["mag_orders"],
        stats["critical_count"],
        stats["high_count"],
        stats["medium_count"],
        stats["low_count"],
        stats["total_refund"],
        stats["total_commission_risk"],
        stats["at_risk_refund"],
        stats["at_risk_commission"],
        stats["overall_return_rate"],
        stats["self_buy_count"],
        stats["sock_puppet_count"],
    ))
    run_id = c.lastrowid
    conn.commit()
    conn.close()
    return run_id


def save_results(run_id, results):
    conn = get_db()
    c = conn.cursor()
    for r in results:
        c.execute("""INSERT INTO flagged_returns (
            run_id, risk_level, fraud_score, recommended_action, flags,
            return_order_id, order_id, buyer_username, product,
            return_reason, return_type, refund_amount, order_amount,
            commission_at_risk, commission_paid, return_status, return_sub_status,
            time_requested, days_since_delivery, creator, state, city, zipcode,
            payment_method, buyer_note, buyer_unique_orders, buyer_unique_returns,
            buyer_return_rate
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            run_id,
            r["risk_level"], r["fraud_score"], r["recommended_action"], r["flags"],
            r["return_order_id"], r["order_id"], r["buyer_username"], r["product"],
            r["return_reason"], r["return_type"], r["refund_amount"], r["order_amount"],
            r["commission_at_risk"], r["commission_paid"], r["return_status"],
            r["return_sub_status"], r["time_requested"],
            str(r["days_since_delivery"]) if r["days_since_delivery"] != "" else "",
            r["creator"], r["state"], r["city"], r["zipcode"],
            r["payment_method"], r["buyer_note"],
            r["buyer_unique_orders"], r["buyer_unique_returns"], r["buyer_return_rate"],
        ))
    conn.commit()
    conn.close()


def update_creator_profiles(results):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    creator_data = {}
    for r in results:
        cr = r["creator"]
        if not cr:
            continue
        if cr not in creator_data:
            creator_data[cr] = {
                "returns": 0, "commission": 0.0,
                "self_buy": 0, "max_score": 0, "scores": []
            }
        creator_data[cr]["returns"] += 1
        creator_data[cr]["commission"] += r["commission_at_risk"]
        if "SELF-BUY" in r["flags"]:
            creator_data[cr]["self_buy"] += 1
        creator_data[cr]["max_score"] = max(creator_data[cr]["max_score"], r["fraud_score"])
        creator_data[cr]["scores"].append(r["fraud_score"])

    for creator, d in creator_data.items():
        avg_score = sum(d["scores"]) / len(d["scores"]) if d["scores"] else 0
        if d["max_score"] >= 60:
            tier = "CRITICAL"
        elif d["max_score"] >= 40:
            tier = "HIGH"
        elif d["max_score"] >= 20:
            tier = "MEDIUM"
        else:
            tier = "LOW"

        c.execute("""INSERT INTO creator_profiles (
            creator_username, total_mag_returns, total_commission_at_risk,
            self_buy_count, highest_fraud_score, avg_fraud_score, risk_tier,
            first_seen, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(creator_username) DO UPDATE SET
            total_mag_returns = total_mag_returns + excluded.total_mag_returns,
            total_commission_at_risk = total_commission_at_risk + excluded.total_commission_at_risk,
            self_buy_count = self_buy_count + excluded.self_buy_count,
            highest_fraud_score = MAX(highest_fraud_score, excluded.highest_fraud_score),
            avg_fraud_score = excluded.avg_fraud_score,
            risk_tier = excluded.risk_tier,
            last_updated = excluded.last_updated
        """, (creator, d["returns"], d["commission"], d["self_buy"],
              d["max_score"], avg_score, tier, now, now))

    conn.commit()
    conn.close()


def get_runs():
    conn = get_db()
    rows = conn.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_run(run_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_results(run_id, risk_filter=None, action_filter=None):
    conn = get_db()
    query = "SELECT * FROM flagged_returns WHERE run_id = ?"
    params = [run_id]

    if risk_filter and risk_filter != "ALL":
        query += " AND risk_level = ?"
        params.append(risk_filter)

    if action_filter and action_filter != "ALL":
        if action_filter == "PENDING":
            query += " AND user_action IS NULL"
        else:
            query += " AND user_action = ?"
            params.append(action_filter)

    query += " ORDER BY fraud_score DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    # Group by order_id — merge duplicates into one row
    from collections import OrderedDict
    order_groups = OrderedDict()
    for r in rows:
        d = dict(r)
        oid = d["order_id"]
        if oid not in order_groups:
            d["order_dupe_count"] = 1
            d["sub_returns"] = []
            order_groups[oid] = d
        else:
            order_groups[oid]["order_dupe_count"] += 1
            # Merge: keep highest score row as primary, collect others as sub_returns
            primary = order_groups[oid]
            sub = d
            if d["fraud_score"] > primary["fraud_score"]:
                # Swap — new row becomes primary
                old_primary = {k: v for k, v in primary.items() if k not in ("order_dupe_count", "sub_returns")}
                primary["sub_returns"].append(old_primary)
                for k, v in d.items():
                    if k not in ("order_dupe_count", "sub_returns"):
                        primary[k] = v
            else:
                primary["sub_returns"].append(sub)
            # Combine refund/commission totals
            primary["combined_refund"] = primary.get("combined_refund", primary["refund_amount"])
            primary["combined_refund"] += sub["refund_amount"] if d["fraud_score"] <= primary["fraud_score"] else 0
            primary["combined_commission"] = primary.get("combined_commission", primary["commission_at_risk"])
            primary["combined_commission"] += sub["commission_at_risk"] if d["fraud_score"] <= primary["fraud_score"] else 0
            # Merge reasons
            if sub["return_reason"] != primary["return_reason"]:
                primary["merged_reasons"] = primary.get("merged_reasons", primary["return_reason"])
                primary["merged_reasons"] += " + " + sub["return_reason"]
            # Merge statuses
            if sub["return_status"] != primary["return_status"]:
                primary["merged_statuses"] = primary.get("merged_statuses", primary["return_status"])
                primary["merged_statuses"] += " / " + sub["return_status"]

    # Finalize: set combined values for single rows too
    results = []
    for row in order_groups.values():
        if "combined_refund" not in row:
            row["combined_refund"] = row["refund_amount"]
        if "combined_commission" not in row:
            row["combined_commission"] = row["commission_at_risk"]
        if "merged_reasons" not in row:
            row["merged_reasons"] = row["return_reason"]
        if "merged_statuses" not in row:
            row["merged_statuses"] = row["return_status"]
        results.append(row)

    return results


def get_results_summary(run_id):
    """Get unique vs total return counts for a run."""
    conn = get_db()
    total = conn.execute(
        "SELECT COUNT(*) as cnt FROM flagged_returns WHERE run_id = ?", (run_id,)
    ).fetchone()
    unique_orders = conn.execute(
        "SELECT COUNT(DISTINCT order_id) as cnt FROM flagged_returns WHERE run_id = ?", (run_id,)
    ).fetchone()
    dupe_orders = conn.execute(
        """SELECT order_id, COUNT(*) as cnt FROM flagged_returns
           WHERE run_id = ? GROUP BY order_id HAVING cnt > 1""", (run_id,)
    ).fetchall()
    conn.close()
    return {
        "total_rows": total["cnt"] if total else 0,
        "unique_orders": unique_orders["cnt"] if unique_orders else 0,
        "duplicate_orders": len(dupe_orders),
    }


def update_action(flagged_return_id, action, notes=""):
    conn = get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get current state for audit log
    current = conn.execute(
        "SELECT run_id, order_id, buyer_username, user_action FROM flagged_returns WHERE id = ?",
        (flagged_return_id,)
    ).fetchone()

    if current:
        conn.execute("""INSERT INTO action_log (
            flagged_return_id, run_id, order_id, buyer_username,
            old_action, new_action, notes, action_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (
            flagged_return_id, current["run_id"], current["order_id"],
            current["buyer_username"], current["user_action"],
            action, notes, now
        ))

    conn.execute("""UPDATE flagged_returns
        SET user_action = ?, user_notes = ?, action_date = ?
        WHERE id = ?""", (action, notes, now, flagged_return_id))

    conn.commit()
    conn.close()


def get_creator_profiles():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM creator_profiles ORDER BY highest_fraud_score DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_action_log(run_id=None):
    conn = get_db()
    if run_id:
        rows = conn.execute(
            "SELECT * FROM action_log WHERE run_id = ? ORDER BY action_date DESC",
            (run_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM action_log ORDER BY action_date DESC LIMIT 100"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_daily_stats(run_id, daily_stats):
    conn = get_db()
    c = conn.cursor()
    for d in daily_stats:
        c.execute("""INSERT INTO daily_stats (run_id, date, orders, returns, refund, commission)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, d["date"], d["orders"], d["returns"], d["refund"], d["commission"]))
    conn.commit()
    conn.close()


def get_daily_stats(run_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM daily_stats WHERE run_id = ? ORDER BY date", (run_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_user_aggregation(run_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT
            buyer_username,
            COUNT(*) as return_count,
            SUM(refund_amount) as total_refund,
            SUM(commission_at_risk) as total_commission,
            MAX(fraud_score) as max_score,
            MIN(fraud_score) as min_score,
            ROUND(AVG(fraud_score), 0) as avg_score,
            buyer_unique_orders,
            buyer_unique_returns,
            buyer_return_rate,
            GROUP_CONCAT(DISTINCT return_reason) as reasons,
            GROUP_CONCAT(DISTINCT creator) as creators,
            GROUP_CONCAT(DISTINCT state) as states,
            GROUP_CONCAT(DISTINCT zipcode) as zipcodes,
            GROUP_CONCAT(DISTINCT payment_method) as payment_methods,
            MAX(CASE WHEN flags LIKE '%SELF-BUY%' THEN 1 ELSE 0 END) as is_self_buy,
            MAX(CASE WHEN flags LIKE '%SOCK PUPPET%' THEN 1 ELSE 0 END) as is_sock_puppet,
            MAX(CASE WHEN flags LIKE '%BNPL%' THEN 1 ELSE 0 END) as uses_bnpl,
            user_action
        FROM flagged_returns
        WHERE run_id = ?
        GROUP BY buyer_username
        ORDER BY max_score DESC
    """, (run_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_address_aggregation(run_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT
            state,
            city,
            zipcode,
            COUNT(*) as return_count,
            COUNT(DISTINCT buyer_username) as unique_buyers,
            COUNT(DISTINCT order_id) as unique_orders,
            SUM(refund_amount) as total_refund,
            SUM(commission_at_risk) as total_commission,
            MAX(fraud_score) as max_score,
            ROUND(AVG(fraud_score), 0) as avg_score,
            GROUP_CONCAT(DISTINCT buyer_username) as buyers,
            GROUP_CONCAT(DISTINCT creator) as creators,
            GROUP_CONCAT(DISTINCT return_reason) as reasons
        FROM flagged_returns
        WHERE run_id = ? AND zipcode != ''
        GROUP BY state, city, zipcode
        ORDER BY return_count DESC, max_score DESC
    """, (run_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_creator_aggregation(run_id):
    conn = get_db()
    rows = conn.execute("""
        SELECT
            creator,
            COUNT(*) as return_count,
            COUNT(DISTINCT buyer_username) as unique_buyers,
            COUNT(DISTINCT order_id) as unique_orders,
            SUM(refund_amount) as total_refund,
            SUM(commission_at_risk) as total_commission,
            MAX(fraud_score) as max_score,
            ROUND(AVG(fraud_score), 0) as avg_score,
            GROUP_CONCAT(DISTINCT buyer_username) as buyers,
            GROUP_CONCAT(DISTINCT return_reason) as reasons,
            GROUP_CONCAT(DISTINCT state) as states,
            MAX(CASE WHEN flags LIKE '%SELF-BUY%' THEN 1 ELSE 0 END) as has_self_buy,
            MAX(commission_paid) as commission_paid
        FROM flagged_returns
        WHERE run_id = ? AND creator != ''
        GROUP BY creator
        ORDER BY total_commission DESC, max_score DESC
    """, (run_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_overview(run_id, overview_data):
    conn = get_db()
    conn.execute("INSERT INTO overview_data (run_id, data_json) VALUES (?, ?)",
                 (run_id, json.dumps(overview_data)))
    conn.commit()
    conn.close()


def get_overview(run_id):
    conn = get_db()
    row = conn.execute("SELECT data_json FROM overview_data WHERE run_id = ?", (run_id,)).fetchone()
    conn.close()
    if row:
        return json.loads(row["data_json"])
    return None


def get_dashboard_stats():
    conn = get_db()
    runs = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 10").fetchall()
    total_actions = conn.execute("SELECT COUNT(*) as cnt FROM action_log").fetchone()
    pending = conn.execute(
        "SELECT COUNT(*) as cnt FROM flagged_returns WHERE user_action IS NULL AND risk_level IN ('CRITICAL', 'HIGH')"
    ).fetchone()
    conn.close()
    return {
        "recent_runs": [dict(r) for r in runs],
        "total_actions": total_actions["cnt"] if total_actions else 0,
        "pending_critical_high": pending["cnt"] if pending else 0,
    }
