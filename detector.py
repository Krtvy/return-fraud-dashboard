"""
Detection Engine v5.0 — MagAsha Liposomal Magnesium
Importable module. No side effects, no file I/O, no prints.
Returns structured results for any consumer (CLI, web, API).
"""

import csv
from datetime import datetime
from collections import defaultdict

# ─── CONFIG ───────────────────────────────────────────────────────────────────
COMMISSION_HOLD_DAYS = 14
PRODUCT_KEYWORD = "liposomal magnesium"

WEIGHTS = {
    "high_buyer_return_rate": 25,
    "repeat_returner": 20,
    "high_creator_return_rate": 20,
    "return_after_commission": 30,
    "return_near_commission_window": 15,
    "suspicious_reason": 10,
    "missing_items_cluster_zip": 15,
    "bnpl_payment": 5,
    "duplicate_return_attempt": 25,
    "repeat_address_returns": 15,
    "state_return_anomaly": 10,
    "zip_return_hotspot": 15,
    "creator_state_concentration": 15,
    "creator_self_buy": 40,
    "commission_already_paid": 25,
    "same_day_return": 10,
    "sock_puppet_phone": 30,
    "sock_puppet_address": 20,
}


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def clean(val):
    if val is None:
        return ""
    return val.strip().strip("\t").strip("\ufeff")


def parse_dollar(val):
    val = clean(val)
    if not val:
        return 0.0
    try:
        return float(val.replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def parse_date_ddmm(val):
    val = clean(val)
    if not val:
        return None
    for fmt in ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y"]:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def parse_date_mmdd(val):
    val = clean(val)
    if not val:
        return None
    for fmt in ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def is_target_product(product_name):
    return PRODUCT_KEYWORD in (product_name or "").lower()


def risk_label(score):
    if score >= 60:
        return "CRITICAL"
    elif score >= 40:
        return "HIGH"
    elif score >= 20:
        return "MEDIUM"
    return "LOW"


def recommended_action(score, reason, return_status, days_since):
    if return_status == "Completed":
        if score >= 60:
            return "INVESTIGATE"
        return "CLOSED"
    if "rejected" in (return_status or "").lower():
        if score >= 40:
            return "KEEP REJECTED"
        return "REVIEW REJECTION"
    if score >= 60:
        return "REJECT"
    elif score >= 40:
        return "ESCALATE"
    elif score >= 20:
        return "REVIEW"
    return "APPROVE"


def normalize_phone(phone):
    if not phone:
        return ""
    return "".join(c for c in phone if c.isdigit())[-10:]


def normalize_name(name):
    if not name:
        return ""
    return "".join(c for c in name.lower() if c.isalnum() or c == " ").strip()


def read_csv_robust(filepath):
    rows = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned = {}
            for k, v in row.items():
                cleaned[clean(k)] = clean(v) if v else ""
            rows.append(cleaned)
    return rows


# ─── MAIN DETECTION PIPELINE ─────────────────────────────────────────────────
def run_detection(return_raw_path, affiliate_raw_path, all_orders_raw_path):
    """
    Run full fraud detection pipeline.
    Returns: (results_list, stats_dict)
    """
    # ── Load ──
    returns = read_csv_robust(return_raw_path)
    affiliates = read_csv_robust(affiliate_raw_path)
    orders = read_csv_robust(all_orders_raw_path)

    mag_returns_all = [r for r in returns if is_target_product(r.get("Product Name", ""))]
    mag_affiliates = [r for r in affiliates if is_target_product(r.get("Product Name", ""))]
    mag_orders = [r for r in orders if is_target_product(r.get("Product Name", ""))]

    # ── Build lookups (needs all affiliates for order->creator mapping) ──
    lookups = _build_lookups(returns, affiliates, orders, mag_returns_all, mag_affiliates, mag_orders)

    # ── AFFILIATE-ONLY FILTER: only score returns from creator link orders ──
    mag_returns = [r for r in mag_returns_all
                   if clean(r.get("Order ID", "")) in lookups["creator_by_order"]]

    # ── Score ──
    results = _score_returns(mag_returns, lookups)

    # ── Daily stats ──
    daily_stats = _compute_daily_stats(mag_affiliates, mag_returns, lookups)

    # ── Stats ──
    levels = {"CRITICAL": [], "HIGH": [], "MEDIUM": [], "LOW": []}
    for r in results:
        levels[r["risk_level"]].append(r)

    crit_high = levels["CRITICAL"] + levels["HIGH"]

    stats = {
        "total_returns_all": len(returns),
        "total_affiliates_all": len(affiliates),
        "total_orders_all": len(orders),
        "mag_returns_all": len(mag_returns_all),
        "mag_returns": len(mag_returns),  # affiliate-only
        "mag_affiliates": len(mag_affiliates),
        "mag_orders": len(mag_orders),
        "critical_count": len(levels["CRITICAL"]),
        "high_count": len(levels["HIGH"]),
        "medium_count": len(levels["MEDIUM"]),
        "low_count": len(levels["LOW"]),
        "total_refund": sum(r["refund_amount"] for r in results),
        "total_commission_risk": sum(r["commission_at_risk"] for r in results),
        "at_risk_refund": sum(r["refund_amount"] for r in crit_high),
        "at_risk_commission": sum(r["commission_at_risk"] for r in crit_high),
        "overall_return_rate": lookups["overall_return_rate"],
        "self_buy_count": len([r for r in results if "SELF-BUY" in r["flags"]]),
        "sock_puppet_count": len(set(
            r["buyer_username"] for r in results if "SOCK PUPPET" in r["flags"]
        )),
    }

    return results, stats, daily_stats


def _build_lookups(returns, affiliates, orders, mag_returns, mag_affiliates, mag_orders):
    creator_by_order = {}
    creator_orders = defaultdict(set)
    all_creators = set()

    for row in affiliates:
        oid = clean(row.get("Order ID", ""))
        creator = clean(row.get("Creator Username", ""))
        if oid and creator:
            est_comm = parse_dollar(row.get("Est. standard commission payment", ""))
            actual_comm = parse_dollar(row.get("Actual Commission Payment", ""))
            creator_by_order[oid] = {
                "creator": creator,
                "commission_rate": clean(row.get("Standard commission rate", "")),
                "est_commission": est_comm,
                "actual_commission": actual_comm,
                "delivery_time": clean(row.get("Order Delivery Time", "")),
                "commission_paid_time": clean(row.get("Time Commission Paid", "")),
            }
            if is_target_product(row.get("Product Name", "")):
                creator_orders[creator].add(oid)
            all_creators.add(creator.lower())

    address_by_order = {}
    for row in orders:
        oid = clean(row.get("Order ID", ""))
        if oid:
            address_by_order[oid] = {
                "buyer_username": clean(row.get("Buyer Username", "")),
                "recipient": clean(row.get("Recipient", "")),
                "phone": clean(row.get("Phone #", "")),
                "state": clean(row.get("State", "")),
                "city": clean(row.get("City", "")),
                "zipcode": clean(row.get("Zipcode", "")),
                "address": clean(row.get("Address Line 1", "")),
                "payment_method": clean(row.get("Payment Method", "")),
                "delivered_time": clean(row.get("Delivered Time", "")),
            }

    # Buyer behavior — ALL products for behavioral signal
    buyer_orders = defaultdict(set)
    buyer_returns = defaultdict(set)
    for row in orders:
        buyer = clean(row.get("Buyer Username", ""))
        oid = clean(row.get("Order ID", ""))
        if buyer and oid:
            buyer_orders[buyer].add(oid)
    for row in returns:
        buyer = clean(row.get("Buyer Username", ""))
        oid = clean(row.get("Order ID", ""))
        if buyer and oid:
            buyer_returns[buyer].add(oid)

    # MagAsha-specific lookups
    buyer_return_records = defaultdict(list)
    for row in mag_returns:
        buyer = clean(row.get("Buyer Username", ""))
        oid = clean(row.get("Order ID", ""))
        if buyer:
            buyer_return_records[buyer].append(oid)

    creator_return_orders = defaultdict(set)
    creator_order_count = {}
    for creator, oids in creator_orders.items():
        creator_order_count[creator] = len(oids)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        if oid in creator_by_order:
            creator = creator_by_order[oid]["creator"]
            creator_return_orders[creator].add(oid)

    zip_returns = defaultdict(list)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        reason = clean(row.get("Return Reason", ""))
        if oid in address_by_order:
            zc = address_by_order[oid]["zipcode"]
            if zc:
                zip_returns[zc].append({"order_id": oid, "reason": reason,
                                         "buyer": clean(row.get("Buyer Username", ""))})

    addr_returns = defaultdict(set)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        if oid in address_by_order:
            addr = address_by_order[oid]["address"]
            if addr and len(addr) > 5:
                addr_returns[addr.upper()].add(oid)

    # Sock puppet — MagAsha returns only
    phone_to_buyers = defaultdict(set)
    recipient_to_buyers = defaultdict(set)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        buyer = clean(row.get("Buyer Username", ""))
        if oid in address_by_order and buyer:
            info = address_by_order[oid]
            phone = normalize_phone(info.get("phone", ""))
            recipient = normalize_name(info.get("recipient", ""))
            if phone and len(phone) >= 7:
                phone_to_buyers[phone].add(buyer)
            if recipient and len(recipient) > 3:
                recipient_to_buyers[recipient].add(buyer)

    sock_puppet_phones = {p: b for p, b in phone_to_buyers.items() if len(b) >= 2}
    sock_puppet_recipients = {n: b for n, b in recipient_to_buyers.items() if len(b) >= 2}

    sock_puppet_buyer_phone = {}
    for phone, buyers in sock_puppet_phones.items():
        for b in buyers:
            sock_puppet_buyer_phone[b] = (phone, buyers)

    sock_puppet_buyer_name = {}
    for name, buyers in sock_puppet_recipients.items():
        for b in buyers:
            sock_puppet_buyer_name[b] = (name, buyers)

    # Self-buy — MagAsha only
    self_buy_orders = set()
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        buyer = clean(row.get("Buyer Username", ""))
        if oid in creator_by_order:
            creator = creator_by_order[oid]["creator"]
            if buyer.lower() == creator.lower():
                self_buy_orders.add(oid)
            elif buyer.lower() in all_creators:
                self_buy_orders.add(oid)

    # Geographic — MagAsha scoped
    state_orders = defaultdict(set)
    state_returns = defaultdict(set)
    for row in mag_orders:
        oid = clean(row.get("Order ID", ""))
        state = clean(row.get("State", ""))
        if state and oid:
            state_orders[state].add(oid)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        if oid in address_by_order:
            state = address_by_order[oid].get("state", "")
            if state:
                state_returns[state].add(oid)

    total_orders_ct = sum(len(v) for v in state_orders.values()) or 1
    total_returns_ct = sum(len(v) for v in state_returns.values()) or 1
    overall_return_rate = total_returns_ct / total_orders_ct

    state_return_rates = {}
    for state in set(list(state_orders.keys()) + list(state_returns.keys())):
        s_orders = len(state_orders.get(state, set()))
        s_returns = len(state_returns.get(state, set()))
        s_rate = s_returns / s_orders if s_orders > 0 else 0
        state_return_rates[state] = {
            "orders": s_orders, "returns": s_returns, "rate": s_rate,
            "is_anomaly": s_rate > (overall_return_rate * 2) and s_returns >= 5,
        }

    zip_orders = defaultdict(set)
    zip_return_set = defaultdict(set)
    for row in mag_orders:
        zc = clean(row.get("Zipcode", ""))
        oid = clean(row.get("Order ID", ""))
        if zc and oid:
            zip_orders[zc].add(oid)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        if oid in address_by_order:
            zc = address_by_order[oid].get("zipcode", "")
            if zc:
                zip_return_set[zc].add(oid)

    zip_hotspots = set()
    for zc in zip_return_set:
        z_orders = len(zip_orders.get(zc, set()))
        z_returns = len(zip_return_set[zc])
        if z_orders > 0:
            z_rate = z_returns / z_orders
            if z_rate > (overall_return_rate * 3) and z_returns >= 3:
                zip_hotspots.add(zc)

    creator_return_states = defaultdict(lambda: defaultdict(int))
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        if oid in creator_by_order and oid in address_by_order:
            creator = creator_by_order[oid]["creator"]
            state = address_by_order[oid].get("state", "")
            if creator and state:
                creator_return_states[creator][state] += 1

    creator_concentrated = {}
    for creator, states in creator_return_states.items():
        total_c_returns = sum(states.values())
        if total_c_returns >= 3:
            top_state = max(states, key=states.get)
            top_pct = states[top_state] / total_c_returns
            if top_pct >= 0.70:
                creator_concentrated[creator] = (top_state, top_pct, states[top_state])

    return {
        "creator_by_order": creator_by_order,
        "address_by_order": address_by_order,
        "buyer_orders": buyer_orders,
        "buyer_returns": buyer_returns,
        "buyer_return_records": buyer_return_records,
        "creator_order_count": creator_order_count,
        "creator_return_orders": creator_return_orders,
        "all_creators": all_creators,
        "zip_returns": zip_returns,
        "addr_returns": addr_returns,
        "self_buy_orders": self_buy_orders,
        "sock_puppet_buyer_phone": sock_puppet_buyer_phone,
        "sock_puppet_buyer_name": sock_puppet_buyer_name,
        "state_return_rates": state_return_rates,
        "overall_return_rate": overall_return_rate,
        "zip_hotspots": zip_hotspots,
        "zip_orders": {zc: len(v) for zc, v in zip_orders.items()},
        "zip_return_counts": {zc: len(v) for zc, v in zip_return_set.items()},
        "creator_concentrated": creator_concentrated,
    }


def _score_returns(mag_returns, lookups):
    results = []
    suspicious_reasons = {"Missing items", "Missing package", "Missing parts"}

    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        return_oid = clean(row.get("Return Order ID", ""))
        buyer = clean(row.get("Buyer Username", ""))
        reason = clean(row.get("Return Reason", ""))
        return_type = clean(row.get("Return Type", ""))
        refund_amount = parse_dollar(row.get("Return unit price", ""))
        order_amount = parse_dollar(row.get("Order Amount", ""))
        return_status = clean(row.get("Return Status", ""))
        return_sub = clean(row.get("Return Sub Status", ""))
        product = clean(row.get("Product Name", ""))
        buyer_note = clean(row.get("Buyer Note", ""))
        time_requested = parse_date_ddmm(row.get("Time Requested", ""))
        payment_method = clean(row.get("Payment Method", ""))

        score = 0
        flags = []

        # RULE 1: Buyer Return Rate (ALL products)
        buyer_order_count = len(lookups["buyer_orders"].get(buyer, set()))
        buyer_return_count = len(lookups["buyer_returns"].get(buyer, set()))
        buyer_return_rate = buyer_return_count / buyer_order_count if buyer_order_count > 0 else 0

        if buyer_return_rate > 0.5 and buyer_return_count > 1:
            score += WEIGHTS["high_buyer_return_rate"]
            flags.append(f"Buyer return rate {buyer_return_rate:.0%} ({buyer_return_count}/{buyer_order_count} orders)")

        # RULE 2: Repeat Returner (ALL products)
        if buyer_return_count >= 2:
            score += WEIGHTS["repeat_returner"]
            flags.append(f"Repeat returner: {buyer_return_count} orders returned")

        # RULE 3: Creator Return Rate (MagAsha only)
        creator_info = lookups["creator_by_order"].get(oid, {})
        creator = creator_info.get("creator", "")
        commission_at_risk = 0.0
        if creator:
            c_orders = lookups["creator_order_count"].get(creator, 0)
            c_returns = len(lookups["creator_return_orders"].get(creator, set()))
            if c_orders > 0:
                c_rate = c_returns / c_orders
                if c_rate > 0.10 and c_returns >= 2:
                    score += WEIGHTS["high_creator_return_rate"]
                    flags.append(f"Creator '{creator}' return rate {c_rate:.1%} ({c_returns}/{c_orders})")
            commission_at_risk = creator_info.get("est_commission", 0) or creator_info.get("actual_commission", 0)

        # RULE 4: Return Timing vs Commission
        delivery_str = creator_info.get("delivery_time", "") or ""
        delivery_date = parse_date_ddmm(delivery_str) if delivery_str else None
        addr_info = lookups["address_by_order"].get(oid, {})
        if not delivery_date and addr_info:
            delivery_str_orders = addr_info.get("delivered_time", "")
            delivery_date = parse_date_mmdd(delivery_str_orders) if delivery_str_orders else None

        days_since_delivery = None
        if delivery_date and time_requested:
            days_since_delivery = (time_requested - delivery_date).days
            if days_since_delivery < 0:
                days_since_delivery = None
            elif days_since_delivery > COMMISSION_HOLD_DAYS:
                score += WEIGHTS["return_after_commission"]
                flags.append(f"Return {days_since_delivery}d after delivery (AFTER {COMMISSION_HOLD_DAYS}d window)")
            elif days_since_delivery >= 12:
                score += WEIGHTS["return_near_commission_window"]
                flags.append(f"Return {days_since_delivery}d after delivery (near commission window)")

        # RULE 4b: Commission Already Paid
        commission_paid_str = creator_info.get("commission_paid_time", "")
        commission_paid_date = parse_date_ddmm(commission_paid_str) if commission_paid_str else None
        commission_already_paid = False
        if commission_paid_date:
            commission_already_paid = True
            if time_requested and time_requested > commission_paid_date:
                score += WEIGHTS["commission_already_paid"]
                days_after = (time_requested - commission_paid_date).days
                flags.append(f"COMMISSION ALREADY PAID {days_after}d before return")
            elif time_requested is None:
                score += WEIGHTS["commission_already_paid"]
                flags.append("COMMISSION WAS PAID (timing unclear)")

        # RULE 5: Suspicious Reason
        if reason in suspicious_reasons:
            score += WEIGHTS["suspicious_reason"]
            flags.append(f"Suspicious reason: '{reason}'")

        # RULE 6: Missing Items Cluster by Zip
        zc = addr_info.get("zipcode", "") if addr_info else ""
        if zc and zc in lookups["zip_returns"]:
            zip_missing = [r for r in lookups["zip_returns"][zc]
                           if r["reason"] in suspicious_reasons and r["order_id"] != oid]
            if len(zip_missing) >= 2:
                score += WEIGHTS["missing_items_cluster_zip"]
                flags.append(f"Zip {zc}: {len(zip_missing)+1} 'missing items' returns")

        # RULE 7: BNPL Payment
        pay = payment_method or (addr_info.get("payment_method", "") if addr_info else "")
        bnpl_keywords = ["klarna", "affirm", "paylater", "pay over time", "pay in"]
        if any(kw in pay.lower() for kw in bnpl_keywords):
            score += WEIGHTS["bnpl_payment"]
            flags.append(f"BNPL payment: {pay}")

        # RULE 8: Duplicate Return Attempts
        buyer_ret_records = lookups["buyer_return_records"].get(buyer, [])
        same_order_records = [r for r in buyer_ret_records if r == oid]
        if len(same_order_records) > 1:
            score += WEIGHTS["duplicate_return_attempt"]
            flags.append(f"Duplicate return: {len(same_order_records)} requests on same order")

        # RULE 9: Repeat Address Returns
        addr = addr_info.get("address", "").upper() if addr_info else ""
        if addr and len(addr) > 5 and addr in lookups["addr_returns"]:
            addr_ret_count = len(lookups["addr_returns"][addr])
            if addr_ret_count >= 2:
                score += WEIGHTS["repeat_address_returns"]
                flags.append(f"Address has {addr_ret_count} return orders")

        # RULE 10: State Anomaly
        state = addr_info.get("state", "") if addr_info else ""
        if state and state in lookups["state_return_rates"]:
            sr = lookups["state_return_rates"][state]
            if sr["is_anomaly"]:
                score += WEIGHTS["state_return_anomaly"]
                mult = sr["rate"] / lookups["overall_return_rate"] if lookups["overall_return_rate"] > 0 else 0
                flags.append(f"State '{state}' return rate {sr['rate']:.1%} ({mult:.1f}x avg)")

        # RULE 11: Zip Hotspot
        if zc and zc in lookups["zip_hotspots"]:
            z_ord = lookups["zip_orders"].get(zc, 0)
            z_ret = lookups["zip_return_counts"].get(zc, 0)
            z_rate = z_ret / z_ord if z_ord > 0 else 0
            score += WEIGHTS["zip_return_hotspot"]
            flags.append(f"Zip {zc} hotspot: {z_rate:.0%} ({z_ret}/{z_ord})")

        # RULE 12: Creator State Concentration
        if creator and creator in lookups["creator_concentrated"]:
            top_st, top_pct, top_count = lookups["creator_concentrated"][creator]
            score += WEIGHTS["creator_state_concentration"]
            flags.append(f"Creator '{creator}' returns: {top_pct:.0%} from {top_st}")

        # RULE 13: Self-Buy
        if oid in lookups["self_buy_orders"]:
            score += WEIGHTS["creator_self_buy"]
            if buyer.lower() == creator.lower():
                flags.append(f"SELF-BUY: buyer @{buyer} IS the creator")
            else:
                flags.append(f"SELF-BUY: buyer @{buyer} is a creator buying via @{creator}")

        # RULE 14: Same-Day Return
        if days_since_delivery is not None and days_since_delivery == 0:
            score += WEIGHTS["same_day_return"]
            flags.append("Same-day return: filed on delivery day")

        # RULE 15: Sock Puppet — Phone
        if buyer in lookups["sock_puppet_buyer_phone"]:
            phone, other_buyers = lookups["sock_puppet_buyer_phone"][buyer]
            other = [b for b in other_buyers if b != buyer]
            if other:
                score += WEIGHTS["sock_puppet_phone"]
                flags.append(f"SOCK PUPPET: same phone as @{', @'.join(list(other)[:3])}")

        # RULE 16: Sock Puppet — Recipient Name
        if buyer in lookups["sock_puppet_buyer_name"]:
            name, other_buyers = lookups["sock_puppet_buyer_name"][buyer]
            other = [b for b in other_buyers if b != buyer]
            if other:
                score += WEIGHTS["sock_puppet_address"]
                flags.append(f"SOCK PUPPET: same recipient '{name}' as @{', @'.join(list(other)[:3])}")

        # Build result
        risk = risk_label(score)
        city = addr_info.get("city", "") if addr_info else ""
        action = recommended_action(score, reason, return_status, days_since_delivery)

        results.append({
            "risk_level": risk,
            "fraud_score": score,
            "recommended_action": action,
            "flags": " | ".join(flags) if flags else "",
            "flags_list": flags,
            "return_order_id": return_oid,
            "order_id": oid,
            "buyer_username": buyer,
            "product": product[:80] if product else "",
            "return_reason": reason,
            "return_type": return_type,
            "refund_amount": refund_amount,
            "order_amount": order_amount,
            "commission_at_risk": commission_at_risk,
            "commission_paid": "YES" if commission_already_paid else "NO",
            "return_status": return_status,
            "return_sub_status": return_sub,
            "time_requested": time_requested.strftime("%Y-%m-%d %H:%M") if time_requested else "",
            "days_since_delivery": days_since_delivery if days_since_delivery is not None else "",
            "creator": creator,
            "state": state,
            "city": city,
            "zipcode": zc,
            "payment_method": pay,
            "buyer_note": buyer_note[:200] if buyer_note else "",
            "buyer_unique_orders": buyer_order_count,
            "buyer_unique_returns": buyer_return_count,
            "buyer_return_rate": f"{buyer_return_rate:.0%}" if buyer_order_count > 0 else "",
        })

    results.sort(key=lambda x: x["fraud_score"], reverse=True)
    return results


def _compute_daily_stats(mag_affiliates, mag_returns, lookups):
    """Compute per-day order and return counts for MagAsha affiliate orders."""
    daily = defaultdict(lambda: {
        "orders": 0, "returns": 0, "refund": 0.0, "commission": 0.0
    })

    # Count affiliate orders by creation date (DD/MM format)
    for row in mag_affiliates:
        dt = parse_date_ddmm(row.get("Time Created", ""))
        if dt:
            day = dt.strftime("%Y-%m-%d")
            daily[day]["orders"] += 1

    # Count returns by request date (DD/MM format)
    for row in mag_returns:
        oid = clean(row.get("Order ID", ""))
        # Only count affiliate returns
        if oid not in lookups["creator_by_order"]:
            continue
        dt = parse_date_ddmm(row.get("Time Requested", ""))
        if dt:
            day = dt.strftime("%Y-%m-%d")
            daily[day]["returns"] += 1
            daily[day]["refund"] += parse_dollar(row.get("Return unit price", ""))
            creator_info = lookups["creator_by_order"].get(oid, {})
            daily[day]["commission"] += (
                creator_info.get("est_commission", 0) or
                creator_info.get("actual_commission", 0)
            )

    # Sort by date
    return [
        {"date": d, **daily[d]}
        for d in sorted(daily.keys())
    ]
