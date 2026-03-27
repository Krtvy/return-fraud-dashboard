"""
Return Fraud Dashboard — Flask Web Application
Detection runs in the browser (JS). Server only saves results and serves pages.
"""

import os
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from datetime import datetime

import database as db

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    dash = db.get_dashboard_stats()
    return render_template("upload.html", dash=dash)


@app.route("/save-results", methods=["POST"])
def save_results():
    """Receive pre-computed results from the browser JS detector and save to DB."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400

    try:
        results = data["results"]
        stats = data["stats"]
        daily_stats = data["dailyStats"]
        overview = data["overview"]

        run_id = db.save_run(stats)
        db.save_results(run_id, results)
        db.save_daily_stats(run_id, daily_stats)
        db.save_overview(run_id, overview)
        db.update_creator_profiles(results)

        return jsonify({"run_id": run_id, "status": "ok"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/results/<int:run_id>")
def results(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    risk_filter = request.args.get("risk", "ALL")
    action_filter = request.args.get("action", "ALL")
    rows = db.get_results(run_id, risk_filter=risk_filter, action_filter=action_filter)
    summary = db.get_results_summary(run_id)
    return render_template("results.html", run=run, rows=rows,
                           risk_filter=risk_filter, action_filter=action_filter, summary=summary)


@app.route("/return/<int:flagged_id>")
def return_detail(flagged_id):
    detail = db.get_return_detail(flagged_id)
    if not detail:
        flash("Return not found", "error")
        return redirect(url_for("index"))
    return render_template("return_detail.html", **detail)


@app.route("/users/<int:run_id>")
def users(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    rows = db.get_user_aggregation(run_id)
    return render_template("users.html", run=run, rows=rows)


@app.route("/addresses/<int:run_id>")
def addresses(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    rows = db.get_address_aggregation(run_id)
    return render_template("addresses.html", run=run, rows=rows)


@app.route("/creator-data/<int:run_id>")
def creator_data(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    rows = db.get_creator_aggregation(run_id)
    return render_template("creator_data.html", run=run, rows=rows)


@app.route("/stats/<int:run_id>")
def stats(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    daily = db.get_daily_stats(run_id)
    return render_template("stats.html", run=run, daily=daily)


@app.route("/overview/<int:run_id>")
def overview(run_id):
    run = db.get_run(run_id)
    if not run:
        flash("Run not found", "error")
        return redirect(url_for("index"))
    data = db.get_overview(run_id)
    if not data:
        flash("No overview data for this run. Re-upload CSVs to generate it.", "error")
        return redirect(url_for("results", run_id=run_id))
    return render_template("overview.html", run=run, data=data)


@app.route("/action", methods=["POST"])
def take_action():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400
    flagged_id = data.get("id")
    action = data.get("action")
    notes = data.get("notes", "")
    if not flagged_id or not action:
        return jsonify({"error": "Missing id or action"}), 400
    db.update_action(flagged_id, action, notes)
    return jsonify({"status": "ok", "action": action})


@app.route("/history")
def history():
    runs = db.get_runs()
    return render_template("history.html", runs=runs)


@app.route("/creators")
def creators():
    profiles = db.get_creator_profiles()
    return render_template("creators.html", profiles=profiles)


# ─── TEMPLATE FILTERS ────────────────────────────────────────────────────────
@app.template_filter("currency")
def currency_filter(value):
    try: return f"${float(value):,.2f}"
    except: return "$0.00"

@app.template_filter("pct")
def pct_filter(value):
    try: return f"{float(value):.1%}"
    except: return "0.0%"


# ─── INIT ────────────────────────────────────────────────────────────────────
db.init_db()

if __name__ == "__main__":
    app.run(debug=True, port=8080, host="0.0.0.0")
