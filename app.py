"""
Return Fraud Dashboard — Flask Web Application
Upload CSVs → Detect fraud → Review & take action → Track history
Three data levels: User, Address, Creator + Daily Stats
"""

import os
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from werkzeug.utils import secure_filename
from datetime import datetime

import database as db
import detector

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")

UPLOAD_DIR = os.environ.get(
    "UPLOAD_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
)
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    runs = db.get_runs()
    dash = db.get_dashboard_stats()
    return render_template("upload.html", runs=runs, dash=dash)


@app.route("/upload", methods=["POST"])
def upload():
    files = {
        "return_raw": request.files.get("return_raw"),
        "affiliate_raw": request.files.get("affiliate_raw"),
        "all_orders_raw": request.files.get("all_orders_raw"),
    }

    for key, f in files.items():
        if not f or f.filename == "":
            flash(f"Missing file: {key.replace('_', ' ').title()}", "error")
            return redirect(url_for("index"))
        if not allowed_file(f.filename):
            flash(f"Invalid file type for {key}: must be .csv", "error")
            return redirect(url_for("index"))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths = {}
    for key, f in files.items():
        filename = f"{timestamp}_{secure_filename(f.filename)}"
        path = os.path.join(UPLOAD_DIR, filename)
        f.save(path)
        paths[key] = path

    try:
        results, stats, daily_stats = detector.run_detection(
            paths["return_raw"],
            paths["affiliate_raw"],
            paths["all_orders_raw"],
        )

        # Compute overview from the uploaded raw CSVs
        overview_data = detector.parse_overview_data(
            paths["return_raw"],
            paths["affiliate_raw"],
            paths["all_orders_raw"],
        )

        run_id = db.save_run(stats)
        db.save_results(run_id, results)
        db.save_daily_stats(run_id, daily_stats)
        db.save_overview(run_id, overview_data)
        db.update_creator_profiles(results)

        flash(
            f"Analysis complete: {stats['mag_returns']} affiliate MagAsha returns analyzed "
            f"(filtered from {stats['mag_returns_all']} total), "
            f"{stats['critical_count']} CRITICAL, {stats['high_count']} HIGH",
            "success"
        )
        return redirect(url_for("results", run_id=run_id))

    except Exception as e:
        flash(f"Error processing files: {str(e)}", "error")
        return redirect(url_for("index"))
    finally:
        for path in paths.values():
            try:
                os.remove(path)
            except OSError:
                pass


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

    return render_template("results.html",
                           run=run, rows=rows,
                           risk_filter=risk_filter,
                           action_filter=action_filter,
                           summary=summary)


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

@app.route("/return/<int:flagged_id>")
   def return_detail(flagged_id):
       detail = db.get_return_detail(flagged_id)
       if not detail:
           flash("Return not found", "error")
           return redirect(url_for("index"))
       return render_template("return_detail.html", **detail)


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
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return "$0.00"


@app.template_filter("pct")
def pct_filter(value):
    try:
        return f"{float(value):.1%}"
    except (ValueError, TypeError):
        return "0.0%"


# ─── INIT ────────────────────────────────────────────────────────────────────
db.init_db()

if __name__ == "__main__":
    print("\n  Return Fraud Dashboard running at http://127.0.0.1:8080\n")
    app.run(debug=True, port=8080, host="0.0.0.0")
