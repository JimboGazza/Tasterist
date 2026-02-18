# ==========================================================
# TASTERIST — MAIN APPLICATION
# Dashboard-first, stable routing
# ==========================================================

import os
import sys
import sqlite3
import calendar
import re
import subprocess
import json
import tempfile
import secrets
import time
from functools import wraps
from datetime import date, datetime, timedelta
from pathlib import Path

from flask import (
    Flask, g, render_template, request,
    redirect, url_for, flash, session, send_file, abort
)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from markupsafe import Markup

import pandas as pd
from openpyxl import load_workbook

# ==========================================================
# APP CONFIG
# ==========================================================

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("TASTERIST_SECRET_KEY", "tasterist-dev-key")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_FILE = (
    "/var/data/tasterist.db"
    if (os.environ.get("RENDER") or os.environ.get("TASTERIST_CANONICAL_HOST"))
    else os.path.join(BASE_DIR, "tasterist.db")
)
DB_FILE = os.environ.get("TASTERIST_DB_FILE", DEFAULT_DB_FILE)
IMPORT_LOG_FILE = os.path.join(BASE_DIR, "import_previews", "last_import.log")
IMPORT_META_FILE = os.path.join(BASE_DIR, "import_previews", "last_import_meta.json")
DAY_ORDER = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6,
}
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]
OWNER_EMAIL = os.environ.get("TASTERIST_OWNER_EMAIL", "james@penninegymnastics.com").strip().lower()
OWNER_NAME = os.environ.get("TASTERIST_OWNER_NAME", "James Gardner").strip() or "James Gardner"
OWNER_RESET_PASSWORD = os.environ.get("TASTERIST_OWNER_RESET_PASSWORD", "").strip()
WEAK_PASSWORDS = {
    "admin123",
    "jammy",
    "password",
    "12345",
    "123456",
    "qwerty",
    "letmein",
}
LOGIN_RATE_LIMIT_WINDOW_SEC = int(os.environ.get("TASTERIST_LOGIN_WINDOW_SEC", "900"))
LOGIN_RATE_LIMIT_ATTEMPTS = int(os.environ.get("TASTERIST_LOGIN_MAX_ATTEMPTS", "8"))
LOGIN_LOCKOUT_SEC = int(os.environ.get("TASTERIST_LOGIN_LOCKOUT_SEC", "900"))
LOGIN_ATTEMPTS = {}
SQLITE_BUSY_TIMEOUT_MS = int(os.environ.get("TASTERIST_SQLITE_BUSY_TIMEOUT_MS", "60000"))
DB_INIT_MAX_RETRIES = int(os.environ.get("TASTERIST_DB_INIT_MAX_RETRIES", "8"))


def is_env_true(name, default="0"):
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _running_in_prod():
    if is_env_true("TASTERIST_FORCE_SECURE_COOKIES", "0"):
        return True
    if os.environ.get("RENDER"):
        return True
    if os.environ.get("TASTERIST_CANONICAL_HOST", "").strip():
        return True
    return False


app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=_running_in_prod(),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    MAX_CONTENT_LENGTH=100 * 1024 * 1024,
)


def log_runtime_environment():
    python_executable = sys.executable
    active_venv = os.environ.get("VIRTUAL_ENV")

    print(f"🐍 Python executable: {python_executable}")
    if active_venv:
        print(f"📦 Virtual environment: {active_venv}")
    else:
        print("⚠️ Virtual environment: not active")


log_runtime_environment()


def get_import_source_folder():
    configured = os.environ.get("TASTER_SHEETS_FOLDER")
    if configured:
        return os.path.expanduser(configured)

    render_default = "/var/data/taster-sheets"
    if _running_in_prod():
        return render_default

    onedrive_default = (
        "/Users/jamesgardner/Library/CloudStorage/OneDrive-Personal/"
        "New Shared Folder/AA Admin/Class Management/Taster Sheets"
    )
    if os.path.isdir(onedrive_default):
        return onedrive_default

    return os.path.join(BASE_DIR, "Taster Sheets")

# ==========================================================
# DATABASE
# ==========================================================

def get_db():
    if "_db" not in g:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        g._db = sqlite3.connect(DB_FILE, timeout=max(5, SQLITE_BUSY_TIMEOUT_MS // 1000))
        g._db.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        g._db.row_factory = sqlite3.Row
    return g._db

def query(sql, args=()):
    db = get_db()
    cur = db.execute(sql, args)
    rows = cur.fetchall()
    return rows

@app.teardown_appcontext
def close_db(exception):
    db = g.pop("_db", None)
    if db:
        db.close()


@app.after_request
def apply_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    if _running_in_prod():
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


def _init_db_once():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    db = sqlite3.connect(DB_FILE, timeout=max(5, SQLITE_BUSY_TIMEOUT_MS // 1000))
    cur = db.cursor()
    cur.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    cur.execute("PRAGMA journal_mode=WAL")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            child TEXT,
            programme TEXT,
            location TEXT,
            session TEXT,
            class_name TEXT DEFAULT '',
            taster_date DATE,
            notes TEXT,
            attended INTEGER DEFAULT 0,
            club_fees INTEGER DEFAULT 0,
            bg INTEGER DEFAULT 0,
            badge INTEGER DEFAULT 0,
            reschedule_contacted INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS leavers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            child TEXT NOT NULL,
            programme TEXT NOT NULL,
            leave_month TEXT NOT NULL,
            leave_date TEXT DEFAULT '',
            class_day TEXT DEFAULT '',
            session TEXT DEFAULT '',
            class_name TEXT DEFAULT '',
            removed_la INTEGER DEFAULT 0,
            removed_bg INTEGER DEFAULT 0,
            added_to_board INTEGER DEFAULT 0,
            reason TEXT DEFAULT '',
            email TEXT DEFAULT '',
            source TEXT DEFAULT 'import'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS class_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            programme TEXT NOT NULL,
            location TEXT NOT NULL,
            session_date TEXT NOT NULL DEFAULT '',
            day TEXT NOT NULL,
            class_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL DEFAULT '',
            source_file TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'staff',
            password_must_change INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_admin_days (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            programme TEXT NOT NULL,
            UNIQUE(user_id, day_name, programme),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            user_id INTEGER,
            username TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT '',
            entity_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'ok',
            details TEXT NOT NULL DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_taster
        ON tasters (child, programme, taster_date, session)
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_leaver
        ON leavers (child, programme, leave_month)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
        ON audit_logs (created_at DESC)
    """)
    # Backward-compat: old DBs may still use leave_date only.
    leaver_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(leavers)")
    }
    if "leave_month" not in leaver_cols and "leave_date" in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN leave_month TEXT")
        cur.execute("""
            UPDATE leavers
            SET leave_month = substr(leave_date, 1, 7)
            WHERE leave_month IS NULL
        """)
    if "leave_date" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN leave_date TEXT DEFAULT ''")
    if "class_day" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN class_day TEXT DEFAULT ''")
    if "session" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN session TEXT DEFAULT ''")
    if "class_name" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN class_name TEXT DEFAULT ''")
    if "removed_la" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN removed_la INTEGER DEFAULT 0")
    if "removed_bg" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN removed_bg INTEGER DEFAULT 0")
    if "added_to_board" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN added_to_board INTEGER DEFAULT 0")
    if "reason" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN reason TEXT DEFAULT ''")
    if "email" not in leaver_cols:
        cur.execute("ALTER TABLE leavers ADD COLUMN email TEXT DEFAULT ''")

    class_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(class_sessions)")
    }
    if "session_date" not in class_cols:
        cur.execute(
            "ALTER TABLE class_sessions ADD COLUMN session_date TEXT NOT NULL DEFAULT ''"
        )

    taster_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(tasters)")
    }
    if "class_name" not in taster_cols:
        cur.execute("ALTER TABLE tasters ADD COLUMN class_name TEXT DEFAULT ''")
    if "club_fees" not in taster_cols:
        cur.execute("ALTER TABLE tasters ADD COLUMN club_fees INTEGER DEFAULT 0")
    if "reschedule_contacted" not in taster_cols:
        cur.execute("ALTER TABLE tasters ADD COLUMN reschedule_contacted INTEGER DEFAULT 0")
    # Keep session format consistent: time-only (e.g. 16:00), no weekday prefix.
    for day_name in (
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday"
    ):
        cur.execute(
            "UPDATE tasters SET session=trim(substr(session, ?)) WHERE session LIKE ?",
            (len(day_name) + 2, f"{day_name} %")
        )

    user_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(users)")
    }
    if "full_name" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN full_name TEXT NOT NULL DEFAULT ''")
    if "password_must_change" not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN password_must_change INTEGER NOT NULL DEFAULT 0")

    cur.execute("DROP INDEX IF EXISTS uniq_class_session")
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_class_session
        ON class_sessions (
            programme, session_date, day,
            class_name, start_time, end_time
        )
    """)

    users_count = cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    owner_bootstrap_password = os.environ.get("TASTERIST_OWNER_BOOTSTRAP_PASSWORD", "").strip()
    if users_count == 0:
        if not owner_bootstrap_password:
            owner_bootstrap_password = secrets.token_urlsafe(16)
            print("⚠️ No users found: created owner with generated bootstrap password.")
            print("⚠️ Set TASTERIST_OWNER_BOOTSTRAP_PASSWORD for predictable first boot credentials.")
        owner_must_change = 1 if (password_strength_errors(owner_bootstrap_password) or is_password_weak_literal(owner_bootstrap_password)) else 0
        cur.execute("""
            INSERT INTO users (username, password_hash, full_name, role, password_must_change)
            VALUES (?, ?, ?, 'owner', ?)
        """, (OWNER_EMAIL, generate_password_hash(owner_bootstrap_password), OWNER_NAME, owner_must_change))

    existing_owner = cur.execute(
        "SELECT id FROM users WHERE lower(username)=?",
        (OWNER_EMAIL,)
    ).fetchone()
    if existing_owner:
        cur.execute("""
            UPDATE users
            SET role='owner',
                full_name=CASE
                    WHEN full_name IS NULL OR trim(full_name)='' THEN ?
                    ELSE full_name
                END
            WHERE id=?
        """, (OWNER_NAME, existing_owner["id"] if isinstance(existing_owner, sqlite3.Row) else existing_owner[0]))
    else:
        if not owner_bootstrap_password:
            owner_bootstrap_password = secrets.token_urlsafe(16)
            print("⚠️ Owner account missing: created owner with generated bootstrap password.")
        owner_must_change = 1 if (password_strength_errors(owner_bootstrap_password) or is_password_weak_literal(owner_bootstrap_password)) else 0
        cur.execute("""
            INSERT INTO users (username, password_hash, full_name, role, password_must_change)
            VALUES (?, ?, ?, 'owner', ?)
        """, (OWNER_EMAIL, generate_password_hash(owner_bootstrap_password), OWNER_NAME, owner_must_change))

    # Break-glass owner reset for cloud recovery.
    # If TASTERIST_OWNER_RESET_PASSWORD is set, owner password is rotated at startup.
    if OWNER_RESET_PASSWORD:
        reset_must_change = 1
        cur.execute("""
            UPDATE users
            SET password_hash=?,
                password_must_change=?,
                role='owner'
            WHERE lower(username)=?
        """, (generate_password_hash(OWNER_RESET_PASSWORD), reset_must_change, OWNER_EMAIL))
        print("⚠️ Owner password reset applied from TASTERIST_OWNER_RESET_PASSWORD.")

    # Remove insecure historical default admin bootstrap accounts.
    insecure_usernames = {
        "admin",
        os.environ.get("TASTERIST_ADMIN_USER", "admin").strip().lower(),
    }
    for username in insecure_usernames:
        if not username or username == OWNER_EMAIL:
            continue
        cur.execute("DELETE FROM user_admin_days WHERE user_id IN (SELECT id FROM users WHERE lower(username)=?)", (username,))
        cur.execute("DELETE FROM users WHERE lower(username)=?", (username,))

    # Remove non-owner users still using known weak/default passwords.
    user_rows = cur.execute("SELECT id, username, password_hash FROM users").fetchall()
    for row in user_rows:
        row_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
        row_username = (row["username"] if isinstance(row, sqlite3.Row) else row[1]).strip().lower()
        row_hash = row["password_hash"] if isinstance(row, sqlite3.Row) else row[2]
        if any(check_password_hash(row_hash, weak) for weak in WEAK_PASSWORDS):
            if row_username == OWNER_EMAIL:
                cur.execute("UPDATE users SET password_must_change=1 WHERE id=?", (row_id,))
                continue
            cur.execute("DELETE FROM user_admin_days WHERE user_id=?", (row_id,))
            cur.execute("DELETE FROM users WHERE id=?", (row_id,))

    db.commit()
    db.close()


def init_db():
    for attempt in range(1, DB_INIT_MAX_RETRIES + 1):
        try:
            _init_db_once()
            return
        except sqlite3.OperationalError as exc:
            locked = "locked" in str(exc).lower()
            if not locked or attempt == DB_INIT_MAX_RETRIES:
                raise
            print(f"⚠️ DB init lock detected, retrying ({attempt}/{DB_INIT_MAX_RETRIES})...")
            time.sleep(1.5)


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    row = query(
        "SELECT id, username, full_name, role, password_must_change FROM users WHERE id=?",
        (user_id,)
    )
    if not row:
        return None
    user = dict(row[0])
    assignments = query("""
        SELECT day_name, programme
        FROM user_admin_days
        WHERE user_id=?
        ORDER BY day_name, programme
    """, (user["id"],))
    user["admin_days"] = [dict(a) for a in assignments]
    return user


def user_initials(text):
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", text or "") if t]
    if not tokens:
        return "U"
    if len(tokens) == 1:
        return tokens[0][:2].upper()
    return (tokens[0][0] + tokens[1][0]).upper()


def is_admin_user(user):
    if not user:
        return False
    if user.get("role") in {"admin", "owner"}:
        return True
    return user.get("username", "").lower() == OWNER_EMAIL


def is_owner_user(user):
    if not user:
        return False
    return user.get("role") == "owner" or user.get("username", "").lower() == OWNER_EMAIL


def owner_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        user = current_user()
        if not is_owner_user(user):
            flash("Owner access only.", "warning")
            return redirect(url_for("dashboard"))
        return view_func(*args, **kwargs)
    return wrapped


def password_strength_errors(password):
    value = password or ""
    errors = []
    if len(value) < 7:
        errors.append("must be at least 7 characters")
    if not re.search(r"[A-Z]", value):
        errors.append("must include an uppercase letter")
    if not re.search(r"\d", value):
        errors.append("must include a number")
    return errors


def is_password_weak_literal(password):
    return (password or "").strip().lower() in WEAK_PASSWORDS


def get_csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def csrf_field():
    token = get_csrf_token()
    return Markup(f'<input type="hidden" name="_csrf_token" value="{token}">')


def validate_csrf_token():
    sent = request.form.get("_csrf_token", "")
    expected = session.get("_csrf_token", "")
    if not sent or not expected:
        return False
    return secrets.compare_digest(sent, expected)


def client_ip_key():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or "unknown"
    return request.remote_addr or "unknown"


def is_login_rate_limited(ip_key):
    now = time.time()
    state = LOGIN_ATTEMPTS.get(ip_key)
    if not state:
        return False, 0
    if state.get("locked_until", 0) > now:
        wait_sec = int(state["locked_until"] - now)
        return True, max(wait_sec, 1)
    if now - state.get("window_start", 0) > LOGIN_RATE_LIMIT_WINDOW_SEC:
        LOGIN_ATTEMPTS.pop(ip_key, None)
    return False, 0


def record_failed_login(ip_key):
    now = time.time()
    state = LOGIN_ATTEMPTS.get(ip_key)
    if not state or now - state.get("window_start", 0) > LOGIN_RATE_LIMIT_WINDOW_SEC:
        state = {"count": 0, "window_start": now, "locked_until": 0}
    state["count"] += 1
    if state["count"] >= LOGIN_RATE_LIMIT_ATTEMPTS:
        state["locked_until"] = now + LOGIN_LOCKOUT_SEC
        state["count"] = 0
        state["window_start"] = now
    LOGIN_ATTEMPTS[ip_key] = state


def clear_login_failures(ip_key):
    LOGIN_ATTEMPTS.pop(ip_key, None)


def admin_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        user = current_user()
        if not is_admin_user(user):
            flash("Admin access only.", "warning")
            return redirect(url_for("account_settings"))
        return view_func(*args, **kwargs)
    return wrapped


def log_audit(action, entity_type="", entity_id="", details="", status="ok"):
    db = get_db()
    user = current_user()
    user_id = user["id"] if user else None
    username = (user["username"] if user else "system")
    db.execute("""
        INSERT INTO audit_logs (
            created_at, user_id, username, action,
            entity_type, entity_id, status, details
        )
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        datetime.now().isoformat(timespec="seconds"),
        user_id,
        username,
        action,
        entity_type,
        str(entity_id or ""),
        status,
        details[:1000],
    ))
    db.commit()


@app.context_processor
def inject_current_user():
    user = current_user()
    return {
        "current_user": user,
        "is_admin_user": is_admin_user(user),
        "is_owner_user": is_owner_user(user),
        "user_initials": user_initials,
        "csrf_token": get_csrf_token,
        "csrf_field": csrf_field,
    }


@app.before_request
def enforce_canonical_host():
    canonical_host = os.environ.get("TASTERIST_CANONICAL_HOST", "").strip().lower()
    if not canonical_host:
        return None
    if request.path == "/health":
        return None
    host = request.host.split(":", 1)[0].strip().lower()
    if host in {canonical_host, "localhost", "127.0.0.1"}:
        return None
    full_path = request.full_path if request.query_string else request.path
    # Keep one public host and force HTTPS for staff-facing links/bookmarks.
    return redirect(f"https://{canonical_host}{full_path}", code=301)


@app.before_request
def enforce_csrf():
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    if request.endpoint in {"static", "health"}:
        return None
    if not validate_csrf_token():
        abort(400, description="Invalid CSRF token")
    return None


@app.before_request
def require_login():
    allowed = {"login", "signup", "static", "health"}
    if request.endpoint in allowed:
        return None
    user = current_user()
    if user is None:
        return redirect(url_for("login", next=request.path))
    must_change = bool(session.get("must_change_password")) or bool(user.get("password_must_change"))
    if must_change and request.endpoint not in {
        "account_settings", "logout", "static", "health"
    }:
        flash("Security update: set a stronger password to continue.", "warning")
        return redirect(url_for("account_settings"))
    return None


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user() is not None:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        ip_key = client_ip_key()
        limited, wait_sec = is_login_rate_limited(ip_key)
        if limited:
            flash(f"Too many sign-in attempts. Try again in about {wait_sec} seconds.", "danger")
            return render_template("login.html"), 429

        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        if not username or not password:
            record_failed_login(ip_key)
            flash("Enter username and password.", "warning")
            return render_template("login.html"), 400

        user_rows = query(
            "SELECT id, username, password_hash, password_must_change FROM users WHERE username=?",
            (username,)
        )
        if not user_rows or not check_password_hash(user_rows[0]["password_hash"], password):
            record_failed_login(ip_key)
            flash("Invalid username or password.", "danger")
            return render_template("login.html"), 401

        clear_login_failures(ip_key)
        session["user_id"] = user_rows[0]["id"]
        session.permanent = True
        session["must_change_password"] = bool(
            user_rows[0]["password_must_change"]
            or password_strength_errors(password)
        )
        log_audit("login", entity_type="user", entity_id=user_rows[0]["id"], details="Successful login")
        if should_run_login_import():
            rc, _ = run_import_process(trigger="login")
            log_audit(
                "run_import",
                entity_type="system",
                entity_id="login",
                details=f"Import trigger=login rc={rc}",
                status="ok" if rc == 0 else "warn",
            )
            last_import = load_last_import_data() or {}
            warning_count = len(last_import.get("warnings", []))
            if rc != 0:
                flash("Signed in. Login import failed; check monitor status.", "warning")
            elif warning_count > 0:
                flash("Signed in. Login import completed with warnings.", "warning")
            else:
                flash("Signed in.", "success")
        else:
            flash("Signed in.", "success")

        if session.get("must_change_password"):
            flash("Security update: please change your password now.", "warning")
            return redirect(url_for("account_settings"))

        target = request.args.get("next")
        if not target or not target.startswith("/"):
            target = url_for("dashboard")
        return redirect(target)

    return render_template("login.html")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    abort(404)


@app.route("/logout", methods=["POST"])
def logout():
    user = current_user()
    if user:
        log_audit("logout", entity_type="user", entity_id=user["id"], details="Signed out")
    session.pop("user_id", None)
    flash("Signed out.", "success")
    return redirect(url_for("login"))


def load_last_import_data():
    if not os.path.exists(IMPORT_LOG_FILE):
        return None

    with open(IMPORT_LOG_FILE, "r", encoding="utf-8") as f:
        log_text = f.read().strip()

    if not log_text:
        return None

    run_at = None
    exit_code = None
    if os.path.exists(IMPORT_META_FILE):
        try:
            with open(IMPORT_META_FILE, "r", encoding="utf-8") as f:
                meta = json.load(f)
            run_at = meta.get("run_at")
            exit_code = meta.get("exit_code")
        except (OSError, json.JSONDecodeError):
            run_at = None
            exit_code = None

    if not run_at:
        run_at = datetime.fromtimestamp(
            os.path.getmtime(IMPORT_LOG_FILE)
        ).isoformat(timespec="seconds")

    file_lines = [
        line for line in log_text.splitlines()
        if line.startswith("📘 FILE:")
    ]
    warning_lines = [
        line for line in log_text.splitlines()
        if "⚠️" in line
    ]

    taster_matches = re.findall(r"✔ Tasters:\s*(\d+)", log_text)
    leaver_matches = re.findall(r"✔ Leavers:\s*(\d+)", log_text)

    try:
        run_at_display = datetime.fromisoformat(run_at).strftime("%d %b %Y %H:%M")
    except ValueError:
        run_at_display = run_at

    return {
        "log_text": log_text,
        "run_at": run_at_display,
        "exit_code": exit_code,
        "files_processed": len(file_lines),
        "warnings": warning_lines,
        "total_tasters": int(taster_matches[-1]) if taster_matches else None,
        "total_leavers": int(leaver_matches[-1]) if leaver_matches else None,
    }


def load_import_meta():
    if not os.path.exists(IMPORT_META_FILE):
        return None
    try:
        with open(IMPORT_META_FILE, "r", encoding="utf-8") as f:
            meta = json.load(f)
        return meta if isinstance(meta, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def should_run_login_import():
    enabled = os.environ.get("TASTERIST_LOGIN_IMPORT_ENABLED", "1").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return False

    min_minutes_raw = os.environ.get("TASTERIST_LOGIN_IMPORT_MINUTES", "15").strip()
    try:
        min_minutes = max(1, int(min_minutes_raw))
    except ValueError:
        min_minutes = 15

    meta = load_import_meta()
    if not meta:
        return True
    run_at_raw = str(meta.get("run_at") or "").strip()
    if not run_at_raw:
        return True
    try:
        last_run = datetime.fromisoformat(run_at_raw)
    except ValueError:
        return True
    return datetime.now() - last_run >= timedelta(minutes=min_minutes)


def run_import_process(trigger="manual"):
    import_source = get_import_source_folder()
    os.makedirs(import_source, exist_ok=True)
    local_fallback = os.path.join(BASE_DIR, "Taster Sheets")
    timeout_raw = os.environ.get("TASTERIST_IMPORT_TIMEOUT_SEC", "120").strip()
    try:
        timeout_seconds = max(15, int(timeout_raw))
    except ValueError:
        timeout_seconds = 120
    cmd = [
        sys.executable,
        "import_taster_sheets.py",
        "--folder", import_source,
        "--apply"
    ]
    if os.path.isdir(local_fallback):
        cmd.extend(["--fallback-folder", local_fallback])
    try:
        result = subprocess.run(
            cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=timeout_seconds
        )
    except subprocess.TimeoutExpired as exc:
        log_parts = []
        stdout_txt = (exc.stdout or "").strip()
        stderr_txt = (exc.stderr or "").strip()
        if stdout_txt:
            log_parts.append(stdout_txt)
        if stderr_txt:
            log_parts.append(stderr_txt)
        log_parts.append(f"Import timed out after {timeout_seconds}s.")
        log_text = "\n\n".join(part for part in log_parts if part).strip() or "(No output captured)"
        os.makedirs(os.path.dirname(IMPORT_LOG_FILE), exist_ok=True)
        with open(IMPORT_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(log_text + "\n")
        with open(IMPORT_META_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "run_at": datetime.now().isoformat(timespec="seconds"),
                "exit_code": 124,
                "trigger": trigger
            }, f)
        return 124, log_text
    except Exception as exc:
        log_text = f"Import execution error: {exc}"
        os.makedirs(os.path.dirname(IMPORT_LOG_FILE), exist_ok=True)
        with open(IMPORT_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(log_text + "\n")
        with open(IMPORT_META_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "run_at": datetime.now().isoformat(timespec="seconds"),
                "exit_code": 125,
                "trigger": trigger
            }, f)
        return 125, log_text

    os.makedirs(os.path.dirname(IMPORT_LOG_FILE), exist_ok=True)
    log_parts = []
    if result.stdout:
        log_parts.append(result.stdout.strip())
    if result.stderr:
        log_parts.append(result.stderr.strip())
    log_text = "\n\n".join(part for part in log_parts if part).strip()
    if not log_text:
        log_text = "(No output captured)"

    with open(IMPORT_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(log_text + "\n")

    with open(IMPORT_META_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": datetime.now().isoformat(timespec="seconds"),
            "exit_code": result.returncode,
            "trigger": trigger
        }, f)

    return result.returncode, log_text

# ==========================================================
# HELPERS
# ==========================================================

def load_tasters_df(programme=None):
    db = get_db()
    q = "SELECT * FROM tasters"
    args = []

    if programme:
        q += " WHERE programme=?"
        args.append(programme)

    q += " ORDER BY taster_date, session, child"

    df = pd.read_sql_query(q, db, params=args)
    if not df.empty:
        df["taster_date"] = pd.to_datetime(df["taster_date"]).dt.date
        if "session" in df.columns:
            df["session"] = df["session"].fillna("")
        if "class_name" in df.columns:
            df["class_name"] = df["class_name"].fillna("")
    return df


def normalise_session_label(value):
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if not m:
        return s
    hour = int(m.group(1))
    minute = m.group(2)
    return f"{hour:02d}:{minute}"


def extract_day_name(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    for day_name in WEEKDAY_NAMES:
        if re.search(rf"\b{day_name.lower()}\b", text):
            return day_name
    return ""


def _programme_tokens(programme):
    p = (programme or "").lower()
    if p == "preschool":
        return ["preschool", "pre-school"]
    if p == "honley":
        return ["honley"]
    return ["lockwood"]


def _candidate_workbooks(root_path, programme, year):
    if not root_path.exists():
        return []
    tokens = _programme_tokens(programme)
    out = []
    for file_path in sorted(root_path.rglob("*.xlsx")):
        name = file_path.name.lower()
        if name.startswith("~$"):
            continue
        if "taster" not in name or "leaver" not in name:
            continue
        if not any(tok in name for tok in tokens):
            continue
        folder_has_year = str(year) in file_path.as_posix()
        name_has_year = str(year) in name
        score = 0
        if folder_has_year:
            score += 2
        if name_has_year:
            score += 2
        if "tasters and leavers" in name:
            score += 1
        out.append((score, file_path))
    out.sort(key=lambda t: (-t[0], str(t[1]).lower()))
    return [p for _, p in out]


def find_programme_workbook(programme, year, prefer_local=True, local_only=False):
    source_root = Path(get_import_source_folder()).expanduser().resolve()
    fallback_root = Path(BASE_DIR, "Taster Sheets").resolve()
    if local_only:
        roots = (fallback_root,)
    else:
        roots = (fallback_root, source_root) if prefer_local else (source_root, fallback_root)
    for root in roots:
        matches = _candidate_workbooks(root, programme, year)
        if matches:
            return matches[0]
    return None


def excel_sync_local_only_mode():
    # Default behaviour:
    # - local/dev: write only to fallback "Taster Sheets" copies
    # - cloud/render: write to configured TASTER_SHEETS_FOLDER
    raw = os.environ.get("TASTERIST_EXCEL_SYNC_LOCAL_ONLY")
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return not _running_in_prod()


def _extract_time(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if hasattr(value, "hour") and hasattr(value, "minute"):
        try:
            return f"{int(value.hour):02d}:{int(value.minute):02d}"
        except Exception:
            pass
    s = str(value).strip()
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if not m:
        return ""
    return f"{int(m.group(1)):02d}:{m.group(2)}"


def _time_matches(target, observed):
    t = _extract_time(target)
    o = _extract_time(observed)
    if not t or not o:
        return False
    if t == o:
        return True
    try:
        t_h, t_m = t.split(":", 1)
        o_h, o_m = o.split(":", 1)
        if t_m != o_m:
            return False
        t_i = int(t_h)
        o_i = int(o_h)
        return (t_i + 12) % 24 == o_i or (o_i + 12) % 24 == t_i
    except ValueError:
        return False


def _parse_sheet_date(value, month_name, year):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        try:
            return date(value.year, value.month, value.day)
        except ValueError:
            return None
    s = str(value).strip().lower()
    if not s:
        return None
    s = re.sub(r"(st|nd|rd|th)", "", s)
    s = re.sub(r"\bof\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m", "%d-%b", "%d %b", "%d %B"):
        try:
            d = datetime.strptime(s, fmt)
            if d.year == 1900:
                d = d.replace(year=year)
            return d.date()
        except ValueError:
            continue
    try:
        return datetime.strptime(f"{s} {month_name} {year}", "%d %B %Y").date()
    except ValueError:
        pass
    try:
        return datetime.strptime(f"{s} {month_name[:3]} {year}", "%d %b %Y").date()
    except ValueError:
        return None


def _find_name_columns_ws(ws, max_scan_rows=25):
    for r in range(1, min(max_scan_rows, ws.max_row) + 1):
        cols = []
        for c in range(1, ws.max_column + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str) and v.strip().lower() == "name":
                cols.append(c)
        if cols:
            return r, cols
    return None, []


def _find_section_rows_ws(ws, marker):
    hits = []
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str) and v.strip().lower() == marker.lower():
                hits.append(r)
                break
    return hits


def _find_leaver_header_row_ws(ws, start_row):
    scan_to = min(start_row + 18, ws.max_row)
    for r in range(start_row, scan_to + 1):
        name_cols = []
        has_leave = False
        for c in range(1, ws.max_column + 1):
            v = ws.cell(r, c).value
            if not isinstance(v, str):
                continue
            s = v.strip().lower()
            if s == "name":
                name_cols.append(c)
            if "leave" in s:
                has_leave = True
        if name_cols and has_leave:
            return r, name_cols
    return None, []


def _build_column_map(ws, name_header_row, name_cols):
    def header_text(col_idx):
        if col_idx < 1 or col_idx > ws.max_column:
            return ""
        v = ws.cell(name_header_row, col_idx).value
        return str(v).strip().lower() if v is not None else ""

    def find_col(name_col, fallback_offset, matcher):
        fallback = name_col + fallback_offset
        for c in range(name_col + 1, min(name_col + 11, ws.max_column + 1)):
            if matcher(header_text(c)):
                return c
        return fallback

    out = {}
    for col in name_cols:
        out[col] = {
            "day_col": col - 1,
            "date_col": find_col(
                col, 1,
                lambda t: "date" in t and ("taster" in t or "date of" in t)
            ),
            "attended_col": find_col(
                col, 2,
                lambda t: ("attended" in t) or ("attend" in t)
            ),
            "club_fees_col": find_col(
                col, 3,
                lambda t: ("paid club fees" in t) or ("club fees" in t) or ("dd" in t and "paid" in t)
            ),
            "bg_col": find_col(
                col, 4,
                lambda t: ("paid bg" in t) or (t == "bg") or ("paid" in t and "bg" in t)
            ),
            "badge_col": find_col(
                col, 5,
                lambda t: ("added bg" in t) or ("badge" in t) or ("account" in t and "bg" in t)
            ),
            "notes_col": find_col(
                col, 6,
                lambda t: ("note" in t) or ("medical" in t)
            ),
            "added_by_col": find_col(
                col, 7,
                lambda t: ("added by" in t) or (t.strip() == "added")
            ),
        }
    return out


def _build_leaver_column_map(ws, header_row, name_cols):
    def header_text(col_idx):
        if col_idx < 1 or col_idx > ws.max_column:
            return ""
        v = ws.cell(header_row, col_idx).value
        return str(v).strip().lower() if v is not None else ""

    def find_col(name_col, fallback_offset, matcher):
        fallback = name_col + fallback_offset
        for c in range(name_col + 1, min(name_col + 11, ws.max_column + 1)):
            if matcher(header_text(c)):
                return c
        return fallback

    out = {}
    for col in name_cols:
        out[col] = {
            "day_col": col - 1,
            "date_col": find_col(
                col, 1,
                lambda t: "date" in t and ("leave" in t or "email" in t)
            ),
            "removed_la_col": find_col(
                col, 2,
                lambda t: ("removed from la" in t) or ("inactive" in t) or ("removed" in t and "la" in t)
            ),
            "removed_bg_col": find_col(
                col, 3,
                lambda t: ("removed from bg" in t) or ("removed" in t and "bg" in t)
            ),
            "board_col": find_col(
                col, 4,
                lambda t: ("leavers board" in t) or ("added" in t and "board" in t)
            ),
            "reason_col": find_col(
                col, 5,
                lambda t: ("reason" in t)
            ),
            "added_by_col": find_col(
                col, 6,
                lambda t: ("added by" in t) or (t.strip() == "added")
            ),
        }
    return out


def _sync_yes_cell(value):
    return "yes" if int(value or 0) == 1 else ""


def sync_taster_to_excel(taster_row, mode="add", changed_field="", actor_initials=""):
    try:
        row = dict(taster_row)
        row_date = datetime.fromisoformat(str(row["taster_date"])).date()
    except Exception:
        return False, "Invalid taster date"

    local_only_sync = excel_sync_local_only_mode()
    workbook = find_programme_workbook(
        row.get("programme"), row_date.year, prefer_local=True, local_only=local_only_sync
    )
    if workbook is None:
        if local_only_sync:
            return False, "Local workbook not found in Taster Sheets for programme/year"
        return False, "Workbook not found in configured sheets folder for programme/year"

    try:
        wb = load_workbook(workbook)
    except Exception as exc:
        return False, f"Could not open workbook: {exc}"

    sheet_name = MONTH_NAMES[row_date.month - 1]
    if sheet_name not in wb.sheetnames:
        return False, f"Month sheet not found: {sheet_name}"
    ws = wb[sheet_name]

    name_header_row, name_cols = _find_name_columns_ws(ws)
    if not name_cols:
        return False, "No Name columns found"
    leaver_markers = _find_section_rows_ws(ws, "LEAVERS")
    taster_end_row = min(leaver_markers) - 1 if leaver_markers else ws.max_row
    column_map = _build_column_map(ws, name_header_row, name_cols)

    target_day = row_date.strftime("%A")
    target_time = _extract_time(row.get("session"))
    month_name = MONTH_NAMES[row_date.month - 1]
    block_state = {
        col: {"day": "", "date": "", "time": ""}
        for col in name_cols
    }
    target_slot = None
    exact_empty_slot = None
    same_day_slot = None
    any_empty_slot = None

    for r in range(1, taster_end_row + 1):
        for col in name_cols:
            cols = column_map[col]
            day_val = ws.cell(r, cols["day_col"]).value if cols["day_col"] >= 1 else ""
            day_txt = str(day_val).strip() if day_val is not None else ""
            parsed_date = _parse_sheet_date(
                ws.cell(r, cols["date_col"]).value if cols["date_col"] <= ws.max_column else "",
                month_name,
                row_date.year
            )

            if day_txt in WEEKDAY_NAMES:
                block_state[col]["day"] = day_txt
            parsed_time = _extract_time(day_txt)
            if parsed_time:
                block_state[col]["time"] = parsed_time
            if parsed_date:
                block_state[col]["date"] = parsed_date.isoformat()

            if r <= name_header_row:
                continue

            name_val = ws.cell(r, col).value
            name_txt = str(name_val).strip() if name_val is not None else ""
            same_day = block_state[col]["day"] == target_day
            same_time = _time_matches(target_time, block_state[col]["time"]) if target_time else same_day

            if name_txt and name_txt.lower() == str(row["child"]).strip().lower():
                row_date_cell = _parse_sheet_date(
                    ws.cell(r, cols["date_col"]).value if cols["date_col"] <= ws.max_column else "",
                    month_name,
                    row_date.year
                )
                if (row_date_cell == row_date) or (same_day and same_time):
                    target_slot = (r, col, cols)
                    break

            if not name_txt:
                if same_day and same_time and exact_empty_slot is None:
                    exact_empty_slot = (r, col, cols)
                if same_day and same_day_slot is None:
                    same_day_slot = (r, col, cols)
                if any_empty_slot is None:
                    any_empty_slot = (r, col, cols)
        if target_slot:
            break

    if target_slot is None:
        if target_time:
            target_slot = exact_empty_slot
        else:
            target_slot = same_day_slot or exact_empty_slot
    if target_slot is None and not target_time:
        target_slot = any_empty_slot
    if target_slot is None:
        return False, "No writable time-matched slot found on sheet"

    row_idx, name_col, cols = target_slot
    if mode == "add":
        ws.cell(row_idx, name_col).value = row["child"]
        if cols["date_col"] <= ws.max_column:
            ws.cell(row_idx, cols["date_col"]).value = f"{row_date.day} {row_date.strftime('%b')}"
        if cols["notes_col"] <= ws.max_column:
            new_notes = str(row.get("notes") or "").strip()
            if new_notes:
                ws.cell(row_idx, cols["notes_col"]).value = new_notes
        if cols["added_by_col"] <= ws.max_column and actor_initials:
            ws.cell(row_idx, cols["added_by_col"]).value = actor_initials
        if cols["attended_col"] <= ws.max_column:
            ws.cell(row_idx, cols["attended_col"]).value = _sync_yes_cell(row.get("attended", 0))
        if cols["club_fees_col"] <= ws.max_column:
            ws.cell(row_idx, cols["club_fees_col"]).value = _sync_yes_cell(row.get("club_fees", 0))
        if cols["bg_col"] <= ws.max_column:
            ws.cell(row_idx, cols["bg_col"]).value = _sync_yes_cell(row.get("bg", 0))
        if cols["badge_col"] <= ws.max_column:
            ws.cell(row_idx, cols["badge_col"]).value = _sync_yes_cell(row.get("badge", 0))
    elif mode == "status":
        field_col_lookup = {
            "attended": cols["attended_col"],
            "club_fees": cols["club_fees_col"],
            "bg": cols["bg_col"],
            "badge": cols["badge_col"],
        }
        target_col = field_col_lookup.get(changed_field)
        if not target_col or target_col > ws.max_column:
            return False, f"Status column not found for {changed_field}"
        ws.cell(row_idx, target_col).value = _sync_yes_cell(row.get(changed_field, 0))
    elif mode == "contacted":
        if cols["notes_col"] <= ws.max_column and int(row.get("reschedule_contacted", 0) or 0) == 1:
            old_note = str(ws.cell(row_idx, cols["notes_col"]).value or "").strip()
            if "contacted" not in old_note.lower():
                ws.cell(row_idx, cols["notes_col"]).value = (
                    f"{old_note} | Contacted for reschedule" if old_note else "Contacted for reschedule"
                )
    else:
        return False, "Unknown sync mode"

    try:
        wb.save(workbook)
    except Exception as exc:
        return False, f"Could not save workbook: {exc}"
    return True, f"Synced to {workbook.name} ({sheet_name})"


def sync_leaver_to_excel(leaver_row, actor_initials=""):
    try:
        row = dict(leaver_row)
        leave_date_raw = (row.get("leave_date") or "").strip()
        if not leave_date_raw:
            return False, "Leave date missing"
        leave_dt = datetime.fromisoformat(leave_date_raw).date()
    except Exception:
        return False, "Invalid leave date"

    local_only_sync = excel_sync_local_only_mode()
    workbook = find_programme_workbook(
        row.get("programme"), leave_dt.year, prefer_local=True, local_only=local_only_sync
    )
    if workbook is None:
        if local_only_sync:
            return False, "Local workbook not found in Taster Sheets for programme/year"
        return False, "Workbook not found in configured sheets folder for programme/year"
    try:
        wb = load_workbook(workbook)
    except Exception as exc:
        return False, f"Could not open workbook: {exc}"

    sheet_name = MONTH_NAMES[leave_dt.month - 1]
    if sheet_name not in wb.sheetnames:
        return False, f"Month sheet not found: {sheet_name}"
    ws = wb[sheet_name]

    leaver_markers = _find_section_rows_ws(ws, "LEAVERS")
    if not leaver_markers:
        return False, "LEAVERS section not found"
    header_row, name_cols = _find_leaver_header_row_ws(ws, min(leaver_markers))
    if not header_row or not name_cols:
        return False, "Leaver columns not found"
    column_map = _build_leaver_column_map(ws, header_row, name_cols)
    leaver_start_row = min(leaver_markers)

    target_day = extract_day_name(row.get("class_day")) or leave_dt.strftime("%A")
    target_time = _extract_time(row.get("session"))

    block_state = {
        col: {"day": "", "time": ""}
        for col in name_cols
    }
    target_slot = None
    exact_empty_slot = None
    same_day_slot = None
    same_time_slot = None

    for r in range(leaver_start_row, ws.max_row + 1):
        for col in name_cols:
            cols = column_map[col]
            day_col = cols["day_col"]
            day_val = ws.cell(r, day_col).value if day_col >= 1 else ""
            day_txt = str(day_val).strip() if day_val is not None else ""
            if day_txt in WEEKDAY_NAMES:
                block_state[col]["day"] = day_txt
            parsed_time = _extract_time(day_txt)
            if parsed_time:
                block_state[col]["time"] = parsed_time

            if r <= header_row:
                continue

            name_val = ws.cell(r, col).value
            name_txt = str(name_val).strip() if name_val is not None else ""
            same_day = block_state[col]["day"] == target_day if target_day else True
            same_time = _time_matches(target_time, block_state[col]["time"]) if target_time else True

            if name_txt and name_txt.lower() == str(row.get("child", "")).strip().lower():
                if same_day and (same_time or not target_time):
                    target_slot = (r, col, cols)
                    break

            if not name_txt:
                if same_day and same_time and exact_empty_slot is None:
                    exact_empty_slot = (r, col, cols)
                if same_day and same_day_slot is None:
                    same_day_slot = (r, col, cols)
                if same_time and same_time_slot is None:
                    same_time_slot = (r, col, cols)
        if target_slot:
            break

    if target_slot is None:
        target_slot = exact_empty_slot or same_day_slot or (same_time_slot if not target_day else None)
    if target_slot is None:
        return False, "No writable leaver slot found for selected day/time"

    row_idx, name_col, cols = target_slot
    ws.cell(row_idx, name_col).value = row.get("child", "")
    if cols["date_col"] <= ws.max_column:
        ws.cell(row_idx, cols["date_col"]).value = leave_dt.strftime("%d %b")
    if cols["removed_la_col"] <= ws.max_column:
        ws.cell(row_idx, cols["removed_la_col"]).value = _sync_yes_cell(row.get("removed_la", 0))
    if cols["removed_bg_col"] <= ws.max_column:
        ws.cell(row_idx, cols["removed_bg_col"]).value = _sync_yes_cell(row.get("removed_bg", 0))
    if cols["board_col"] <= ws.max_column:
        ws.cell(row_idx, cols["board_col"]).value = _sync_yes_cell(row.get("added_to_board", 0))
    if cols["reason_col"] <= ws.max_column:
        reason_txt = str(row.get("reason") or "").strip()
        if reason_txt:
            ws.cell(row_idx, cols["reason_col"]).value = reason_txt
    if cols["added_by_col"] <= ws.max_column and actor_initials:
        ws.cell(row_idx, cols["added_by_col"]).value = actor_initials

    try:
        wb.save(workbook)
    except Exception as exc:
        return False, f"Could not save workbook: {exc}"
    return True, f"Synced to {workbook.name} ({sheet_name})"


def get_day_programme_options():
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    programmes = ["preschool", "honley", "lockwood"]
    options = []
    for d in days:
        for p in programmes:
            options.append({
                "value": f"{d}|{p}",
                "day_name": d,
                "programme": p,
                "label": f"{d} • {p.title()}",
            })
    return options


def build_week_schedule(programme, week_start):
    week_days = []
    db = get_db()
    today = date.today()
    window_start = today.isoformat()
    window_end = (today + timedelta(days=30)).isoformat()

    for offset in range(7):
        day_date = week_start + timedelta(days=offset)
        day_str = day_date.isoformat()
        day_name = day_date.strftime("%A")

        rows = db.execute("""
            SELECT class_name, start_time, end_time, location
            FROM class_sessions
            WHERE programme=? AND session_date=?
            ORDER BY start_time, class_name
        """, (programme, day_str)).fetchall()

        source_mode = "dated"
        if not rows:
            rows = db.execute("""
                SELECT class_name, start_time, end_time, location
                FROM class_sessions
                WHERE programme=? AND day=?
                ORDER BY start_time, class_name
            """, (programme, day_name)).fetchall()
            source_mode = "weekly"

        sessions = []
        for row in rows:
            start_time = (row["start_time"] or "")[:5]
            end_time = (row["end_time"] or "")[:5]
            time_range = f"{start_time} - {end_time}" if end_time else start_time
            weekday_sql = day_date.strftime("%w")
            session_time = normalise_session_label(start_time)
            session_with_day = f"{day_name} {start_time}".strip()
            sessions.append({
                "session_value": f"{day_name} {start_time}",
                "class_name": row["class_name"],
                "time_range": time_range,
                "start_time": start_time,
                "end_time": end_time,
                "location": row["location"],
                "upcoming_count": db.execute("""
                    SELECT COUNT(*) AS c
                    FROM tasters
                    WHERE programme=?
                      AND class_name=?
                      AND (
                        lower(trim(session)) = lower(?)
                        OR lower(trim(session)) = lower(?)
                      )
                      AND strftime('%w', taster_date)=?
                      AND taster_date>=?
                      AND taster_date<=?
                """, (
                    programme,
                    row["class_name"],
                    session_time,
                    session_with_day,
                    weekday_sql,
                    window_start,
                    window_end
                )).fetchone()["c"],
            })

        week_days.append({
            "date_obj": day_date,
            "date_str": day_str,
            "day_name": day_name,
            "sessions": sessions,
            "source_mode": source_mode,
        })

    return week_days


def toggle_flag(taster_id, column):
    if column not in ("attended", "club_fees", "bg", "badge"):
        return None
    db = get_db()
    cur = db.cursor()
    cur.execute(f"SELECT {column} FROM tasters WHERE id=?", (taster_id,))
    row = cur.fetchone()
    if not row:
        return None
    new_value = 0 if row[column] else 1
    cur.execute(
        f"UPDATE tasters SET {column}=? WHERE id=?",
        (new_value, taster_id)
    )
    db.commit()
    updated = cur.execute("SELECT * FROM tasters WHERE id=?", (taster_id,)).fetchone()
    return dict(updated) if updated else None

# ==========================================================
# DASHBOARD (HOME)
# ==========================================================

@app.route("/")
def dashboard():
    db = get_db()
    today = date.today()
    month_key = today.strftime("%Y-%m")
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    cutoff_iso = (today - timedelta(days=62)).isoformat()
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_left = max(days_in_month - today.day, 0)

    tasters_month = db.execute("""
        SELECT COUNT(*) c FROM tasters
        WHERE strftime('%Y-%m', taster_date)=?
    """, (month_key,)).fetchone()["c"]

    leavers_month = db.execute("""
        SELECT COUNT(*) c FROM leavers
        WHERE leave_month=?
    """, (month_key,)).fetchone()["c"]
    total_tasters_all = db.execute(
        "SELECT COUNT(*) c FROM tasters"
    ).fetchone()["c"]

    todays_counts_rows = db.execute("""
        SELECT programme, COUNT(*) c
        FROM tasters
        WHERE taster_date=?
        GROUP BY programme
    """, (today.isoformat(),)).fetchall()
    todays_counts = {"lockwood": 0, "honley": 0, "preschool": 0}
    for row in todays_counts_rows:
        todays_counts[row["programme"]] = int(row["c"] or 0)
    todays_total = sum(todays_counts.values())

    todays_tasters_rows = db.execute("""
        SELECT child, programme, session, class_name
        FROM tasters
        WHERE taster_date=?
        ORDER BY session, child
    """, (today.isoformat(),)).fetchall()
    todays_by_programme = {
        "lockwood": [],
        "honley": [],
        "preschool": [],
    }
    for row in todays_tasters_rows:
        programme = row["programme"] if row["programme"] in todays_by_programme else "lockwood"
        todays_by_programme[programme].append(dict(row))

    month_by_programme_rows = db.execute("""
        SELECT programme, COUNT(*) c
        FROM tasters
        WHERE strftime('%Y-%m', taster_date)=?
        GROUP BY programme
    """, (month_key,)).fetchall()
    counts = {"lockwood": 0, "honley": 0, "preschool": 0}
    for row in month_by_programme_rows:
        counts[row["programme"]] = row["c"]
    max_count = max(counts.values()) if counts else 0
    month_by_programme = []
    for key, label in (
        ("lockwood", "Lockwood"),
        ("honley", "Honley"),
        ("preschool", "Preschool"),
    ):
        count = counts.get(key, 0)
        pct = int((count / max_count) * 100) if max_count > 0 else 0
        month_by_programme.append({
            "key": key,
            "label": label,
            "count": count,
            "pct": pct,
        })

    followups_open = db.execute("""
        SELECT COUNT(*) c
        FROM tasters
        WHERE taster_date>=?
          AND taster_date<=?
          AND (attended=0 OR club_fees=0 OR bg=0 OR badge=0)
    """, (cutoff_iso, today.isoformat())).fetchone()["c"]

    converted_month = db.execute("""
        SELECT COUNT(*) c
        FROM tasters
        WHERE strftime('%Y-%m', taster_date)=?
          AND attended=1
          AND club_fees=1
          AND bg=1
          AND badge=1
    """, (month_key,)).fetchone()["c"]

    week_bookings = db.execute("""
        SELECT COUNT(*) c
        FROM tasters
        WHERE taster_date>=?
          AND taster_date<=?
    """, (week_start.isoformat(), week_end.isoformat())).fetchone()["c"]

    last_import = load_last_import_data()
    monitor = {
        "status": "idle",
        "label": "Not Run",
        "class_name": "monitor-idle",
        "run_at": "—",
        "warnings": 0,
        "import_total_tasters": None,
        "db_total_tasters": total_tasters_all,
    }
    if last_import:
        monitor["run_at"] = last_import.get("run_at", "—")
        monitor["warnings"] = len(last_import.get("warnings", []))
        monitor["import_total_tasters"] = last_import.get("total_tasters")
        if last_import.get("exit_code") != 0:
            monitor["status"] = "error"
            monitor["label"] = "Import Failed"
            monitor["class_name"] = "monitor-error"
        elif monitor["warnings"] > 0:
            monitor["status"] = "warn"
            monitor["label"] = "Check Import"
            monitor["class_name"] = "monitor-warn"
        else:
            monitor["status"] = "ok"
            monitor["label"] = "Import Healthy"
            monitor["class_name"] = "monitor-ok"

    return render_template(
        "dashboard.html",
        month=today.strftime("%B %Y"),
        current_time=datetime.now().strftime("%H:%M"),
        today=today,
        tasters=tasters_month,
        leavers=leavers_month,
        net=tasters_month - leavers_month,
        followups_open=followups_open,
        converted_month=converted_month,
        week_bookings=week_bookings,
        todays_total=todays_total,
        todays_counts=todays_counts,
        todays_by_programme=todays_by_programme,
        month_by_programme=month_by_programme,
        monitor=monitor,
        days_left=days_left,
        week_start=week_start,
        week_end=week_end,
    )


# ==========================================================
# CORE VIEWS
# ==========================================================

@app.route("/today")
def today():
    programme = request.args.get("programme", "lockwood")
    return redirect(url_for(
        "day_detail",
        date_str=date.today().isoformat(),
        programme=programme
    ))

@app.post("/toggle/<int:taster_id>/<field>", endpoint="toggle")
def toggle_view(taster_id, field):
    if field not in ("attended", "club_fees", "bg", "badge"):
        flash("Invalid toggle field", "danger")
        return redirect(request.referrer or url_for("today"))

    updated_row = toggle_flag(taster_id, field)
    if updated_row:
        initials = user_initials((current_user() or {}).get("full_name", ""))
        ok, msg = sync_taster_to_excel(updated_row, mode="status", changed_field=field, actor_initials=initials)
        log_audit(
            "toggle_taster_field",
            entity_type="taster",
            entity_id=taster_id,
            details=f"{field}={updated_row.get(field)} | excel_sync={msg}",
            status="ok" if ok else "warn",
        )
        if not ok:
            flash(f"Updated in app, but Excel sync needs review: {msg}", "warning")
    else:
        log_audit(
            "toggle_taster_field",
            entity_type="taster",
            entity_id=taster_id,
            details=f"Toggle failed: taster not found ({field})",
            status="warn",
        )
    return redirect(request.referrer or url_for("today"))

@app.route("/month")
def month():
    programme = request.args.get("programme", "lockwood")
    year = int(request.args.get("y", date.today().year))
    month_n = int(request.args.get("m", date.today().month))

    df = load_tasters_df(programme)
    if not df.empty:
        df = df[
            (pd.to_datetime(df["taster_date"]).dt.year == year) &
            (pd.to_datetime(df["taster_date"]).dt.month == month_n)
        ]

    db = get_db()
    year_rows = db.execute("""
        SELECT DISTINCT CAST(strftime('%Y', taster_date) AS INTEGER) AS y
        FROM tasters
        WHERE taster_date IS NOT NULL
        ORDER BY y
    """).fetchall()
    year_options = [row["y"] for row in year_rows if row["y"]]
    if not year_options:
        current = date.today().year
        year_options = list(range(current - 1, current + 2))
    if year not in year_options:
        year_options.append(year)
        year_options = sorted(set(year_options))

    return render_template(
        "month.html",
        year=year,
        month=month_n,
        month_name=calendar.month_name[month_n],
        month_matrix=calendar.monthcalendar(year, month_n),
        month_df=df,
        programme=programme,
        location=programme.title(),
        today_date=date.today(),
        year_options=year_options,
        calendar=calendar,
        datetime=datetime
    )


@app.route("/day/<date_str>")
def day_detail(date_str):
    programme = request.args.get("programme", "lockwood")
    try:
        selected = datetime.fromisoformat(date_str).date()
    except ValueError:
        flash("Invalid date", "danger")
        return redirect(url_for("dashboard"))

    df = load_tasters_df(programme)
    day_df = df[df["taster_date"] == selected] if not df.empty else df

    return render_template(
        "day.html",
        data=day_df,
        selected_date=selected,
        prev_date=(selected - timedelta(days=1)),
        next_date=(selected + timedelta(days=1)),
        today_date=date.today(),
        programme=programme,
        location=programme.title()
    )

@app.route("/stats")
def stats():
    raw_monthly = query("""
        WITH t AS (
            SELECT strftime('%Y-%m', taster_date) AS m, COUNT(*) c
            FROM tasters
            GROUP BY m
        ),
        l AS (
            SELECT leave_month AS m, COUNT(*) c
            FROM leavers
            GROUP BY m
        )
        SELECT
            COALESCE(t.m, l.m) AS month,
            COALESCE(t.c, 0) AS tasters,
            COALESCE(l.c, 0) AS leavers
        FROM t
        LEFT JOIN l ON t.m = l.m
        UNION
        SELECT
            COALESCE(t.m, l.m),
            COALESCE(t.c, 0),
            COALESCE(l.c, 0)
        FROM l
        LEFT JOIN t ON t.m = l.m
        ORDER BY month;
    """)
    monthly = []
    for row in raw_monthly:
        m = (row["month"] or "").strip()
        if not re.match(r"^\d{4}-\d{2}$", m):
            continue
        if m < "2000-01":
            continue
        monthly.append({
            "month": m,
            "tasters": int(row["tasters"] or 0),
            "leavers": int(row["leavers"] or 0),
        })
    monthly_desc = sorted(monthly, key=lambda r: r["month"], reverse=True)
    monthly_chart = sorted(monthly, key=lambda r: r["month"])
    current_month = date.today().strftime("%Y-%m")
    monthly_desc = [r for r in monthly_desc if r["month"] <= current_month]
    monthly_chart = [r for r in monthly_chart if r["month"] <= current_month]

    this_month = date.today().strftime("%Y-%m")
    month_programme_rows = query("""
        SELECT programme, COUNT(*) AS c
        FROM tasters
        WHERE strftime('%Y-%m', taster_date)=?
        GROUP BY programme
        ORDER BY c DESC
    """, (this_month,))
    this_month_programme = [
        {"programme": r["programme"], "count": int(r["c"] or 0)}
        for r in month_programme_rows
    ]

    totals = {
        "tasters_all": sum(r["tasters"] for r in monthly_desc),
        "leavers_all": sum(r["leavers"] for r in monthly_desc),
        "months_tracked": len(monthly_desc),
        "net_all": sum(r["tasters"] - r["leavers"] for r in monthly_desc),
        "latest_month": monthly_desc[0]["month"] if monthly_desc else "—",
    }

    return render_template(
        "stats.html",
        monthly=monthly_desc,
        monthly_chart=monthly_chart,
        current_month=current_month,
        totals=totals,
        this_month_programme=this_month_programme,
    )


# ==========================================================
# LEAVERS
# ==========================================================

@app.route("/leavers/add", methods=["GET", "POST"])
def add_leaver():
    programme = request.args.get("programme", "preschool")
    if request.method == "POST":
        child = request.form["child"].strip()
        programme = request.form.get("programme", programme)
        leave_date = request.form.get("leave_date", "").strip()
        session_label = request.form.get("session", "").strip()
        class_day = request.form.get("class_day", "").strip()
        class_name = request.form.get("class_name", "").strip()
        manual_session = request.form.get("manual_session", "").strip()
        removed_la = 1 if request.form.get("removed_la") == "1" else 0
        removed_bg = 1 if request.form.get("removed_bg") == "1" else 0
        added_to_board = 1 if request.form.get("added_to_board") == "1" else 0
        reason = request.form.get("reason", "").strip()
        email = request.form.get("email", "").strip()
        sync_excel = request.form.get("sync_excel") == "1"

        if not child or not leave_date:
            flash("Name and leave date are required.", "danger")
            return redirect(request.url)

        if session_label == "__manual__":
            session_label = normalise_session_label(manual_session)
            if not class_day:
                class_day = extract_day_name(manual_session)
            class_name = class_name or "Manual Session"
        else:
            if not class_day:
                class_day = extract_day_name(session_label)
            session_label = normalise_session_label(session_label)

        if not session_label:
            flash("Please choose a class/session.", "warning")
            return redirect(request.url)

        try:
            leave_dt = datetime.fromisoformat(leave_date).date()
            leave_month = leave_dt.strftime("%Y-%m")
            if not class_day:
                class_day = leave_dt.strftime("%A")
        except ValueError:
            flash("Invalid leave date", "danger")
            return redirect(request.url)

        db = get_db()
        db.execute("""
            INSERT INTO leavers (
                child, programme, leave_month, leave_date,
                class_day, session, class_name,
                removed_la, removed_bg, added_to_board, reason,
                email, source
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            child,
            programme,
            leave_month,
            leave_date,
            class_day,
            session_label,
            class_name,
            removed_la,
            removed_bg,
            added_to_board,
            reason,
            email,
            "manual"
        ))
        leaver_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.commit()

        inserted = db.execute("SELECT * FROM leavers WHERE id=?", (leaver_id,)).fetchone()
        actor_initials = user_initials((current_user() or {}).get("full_name", ""))
        sync_msg = "Excel sync skipped"
        sync_status = "ok"
        if sync_excel and inserted:
            ok, sync_msg = sync_leaver_to_excel(inserted, actor_initials=actor_initials)
            if not ok:
                flash(f"Leaver saved in app, but Excel sync needs review: {sync_msg}", "warning")
                sync_status = "warn"

        log_audit(
            "add_leaver",
            entity_type="leaver",
            entity_id=leaver_id,
            details=f"{child} | {programme} | {class_day} {session_label} | {leave_date} | excel_sync={sync_msg}",
            status=sync_status,
        )

        flash(f"Leaver recorded for {child}", "success")
        return redirect(url_for("admin_tasks"))

    week_start_raw = request.args.get("week_start") or request.args.get("leave_date")
    if week_start_raw:
        try:
            anchor_date = datetime.fromisoformat(week_start_raw).date()
        except ValueError:
            anchor_date = date.today()
    else:
        anchor_date = date.today()

    week_start = anchor_date - timedelta(days=anchor_date.weekday())
    week_end = week_start + timedelta(days=6)
    week_days = build_week_schedule(programme, week_start)

    return render_template(
        "add_leaver.html",
        programme=programme,
        week_days=week_days,
        week_start=week_start,
        week_end=week_end,
        prev_week=(week_start - timedelta(days=7)).isoformat(),
        next_week=(week_start + timedelta(days=7)).isoformat(),
        today_str=date.today().isoformat()
    )


@app.route("/admin/tasks")
def admin_tasks():
    today_dt = date.today()
    today_iso = today_dt.isoformat()
    cutoff_iso = (today_dt - timedelta(days=62)).isoformat()
    month_key = today_dt.strftime("%Y-%m")
    month_label = today_dt.strftime("%B %Y")
    user = current_user()
    assignments = user.get("admin_days", []) if user else []
    assignments_sorted = sorted(
        assignments,
        key=lambda a: (DAY_ORDER.get(a["day_name"], 99), a["programme"])
    )
    assignment_set = {(a["day_name"], a["programme"]) for a in assignments_sorted}

    def key_for(day_name, programme):
        return f"{day_name}|{programme}"

    db = get_db()
    leaver_count_map = {}
    unknown_leaver_counts = {}
    leaver_rows = db.execute("""
        SELECT child, programme, leave_date, class_day, session, class_name
        FROM leavers
        WHERE leave_month=?
    """, (month_key,)).fetchall()
    for row in leaver_rows:
        row_dict = dict(row)
        day_name = (
            extract_day_name(row_dict.get("class_day"))
            or extract_day_name(row_dict.get("session"))
        )
        leave_date = str(row_dict.get("leave_date") or "").strip()
        if not day_name and leave_date:
            try:
                day_name = datetime.fromisoformat(leave_date).strftime("%A")
            except ValueError:
                day_name = ""
        if not day_name:
            unknown_key = key_for("?", row_dict["programme"])
            unknown_leaver_counts[unknown_key] = unknown_leaver_counts.get(unknown_key, 0) + 1
            continue
        if assignment_set and (day_name, row_dict["programme"]) not in assignment_set:
            continue
        key = key_for(day_name, row_dict["programme"])
        leaver_count_map[key] = leaver_count_map.get(key, 0) + 1

    member_count_map = {}
    member_rows = db.execute("""
        SELECT taster_date, programme
        FROM tasters
        WHERE strftime('%Y-%m', taster_date)=?
          AND attended=1
          AND club_fees=1
          AND bg=1
          AND badge=1
    """, (month_key,)).fetchall()
    for row in member_rows:
        day_name = datetime.fromisoformat(row["taster_date"]).strftime("%A")
        if assignment_set and (day_name, row["programme"]) not in assignment_set:
            continue
        key = key_for(day_name, row["programme"])
        member_count_map[key] = member_count_map.get(key, 0) + 1

    followup_rows = []
    raw_followups = db.execute("""
        SELECT
            id, child, programme, taster_date, session, class_name,
            attended, club_fees, bg, badge, reschedule_contacted, notes
        FROM tasters
        WHERE taster_date>=?
          AND taster_date<=?
          AND (attended=0 OR club_fees=0 OR bg=0 OR badge=0)
        ORDER BY taster_date DESC, programme, session, child
    """, (cutoff_iso, today_iso)).fetchall()
    for row in raw_followups:
        row_dict = dict(row)
        day_name = datetime.fromisoformat(row["taster_date"]).strftime("%A")
        if assignment_set and (day_name, row["programme"]) not in assignment_set:
            continue
        row_dict["day_name"] = day_name
        followup_rows.append(row_dict)

    if assignments_sorted:
        summary_rows = []
        for a in assignments_sorted:
            key = key_for(a["day_name"], a["programme"])
            summary_rows.append({
                "day_name": a["day_name"],
                "programme": a["programme"],
                "key": key,
                "leavers": leaver_count_map.get(key, 0),
                "members": member_count_map.get(key, 0),
            })
    else:
        keys = set(leaver_count_map.keys()) | set(member_count_map.keys())
        for row in followup_rows:
            keys.add(key_for(row["day_name"], row["programme"]))
        summary_rows = []
        for key in sorted(
            keys,
            key=lambda k: (
                DAY_ORDER.get(k.split("|", 1)[0], 99),
                k.split("|", 1)[1]
            )
        ):
            day_name, programme = key.split("|", 1)
            summary_rows.append({
                "day_name": day_name,
                "programme": programme,
                "key": key,
                "leavers": leaver_count_map.get(key, 0),
                "members": member_count_map.get(key, 0),
            })

    unknown_summary = []
    for key, count in sorted(unknown_leaver_counts.items()):
        _, programme = key.split("|", 1)
        unknown_summary.append({
            "programme": programme,
            "count": count,
        })

    return render_template(
        "admin_tasks.html",
        month_label=month_label,
        summary_rows=summary_rows,
        unknown_summary=unknown_summary,
        followup_rows=followup_rows,
        followup_total=len(followup_rows),
        assignments=assignments_sorted,
        has_custom_assignments=bool(assignment_set),
        today=today_iso,
        cutoff=cutoff_iso,
    )


@app.post("/admin/tasks/contact/<int:taster_id>")
def admin_mark_contacted(taster_id):
    db = get_db()
    row = db.execute("SELECT * FROM tasters WHERE id=?", (taster_id,)).fetchone()
    if not row:
        flash("Taster not found.", "warning")
        return redirect(request.referrer or url_for("admin_tasks"))
    db.execute("UPDATE tasters SET reschedule_contacted=1 WHERE id=?", (taster_id,))
    db.commit()
    updated_row = db.execute("SELECT * FROM tasters WHERE id=?", (taster_id,)).fetchone()
    sync_ok = True
    sync_msg = "Excel sync skipped"
    if updated_row:
        initials = user_initials((current_user() or {}).get("full_name", ""))
        sync_ok, sync_msg = sync_taster_to_excel(
            updated_row,
            mode="contacted",
            actor_initials=initials
        )
        if not sync_ok:
            flash(f"Marked contacted in app, but Excel sync needs review: {sync_msg}", "warning")
    log_audit(
        "mark_no_show_contacted",
        entity_type="taster",
        entity_id=taster_id,
        details=f"Marked as contacted for reschedule | excel_sync={sync_msg}",
        status="ok" if sync_ok else "warn",
    )
    flash("Marked as contacted.", "success")
    return redirect(request.referrer or url_for("admin_tasks"))


@app.route("/account", methods=["GET", "POST"])
def account_settings():
    user = current_user()
    if user is None:
        return redirect(url_for("login"))

    db = get_db()
    if request.method == "POST":
        action = request.form.get("action", "")
        if action == "profile":
            first_name = request.form.get("first_name", "").strip()
            last_name = request.form.get("last_name", "").strip()
            email = request.form.get("email", "").strip().lower()
            full_name = f"{first_name} {last_name}".strip()
            if not first_name or not last_name:
                flash("First and last name are required.", "warning")
                return redirect(url_for("account_settings"))
            if not email or "@" not in email:
                flash("Valid email is required.", "warning")
                return redirect(url_for("account_settings"))
            existing = db.execute(
                "SELECT id FROM users WHERE username=? AND id<>?",
                (email, user["id"])
            ).fetchone()
            if existing:
                flash("That email is already in use.", "warning")
                return redirect(url_for("account_settings"))
            db.execute(
                "UPDATE users SET full_name=?, username=? WHERE id=?",
                (full_name, email, user["id"])
            )
            db.commit()
            log_audit(
                "update_profile",
                entity_type="user",
                entity_id=user["id"],
                details=f"Profile updated to {full_name} ({email})",
            )
            flash("Profile updated.", "success")
            return redirect(url_for("account_settings"))
        if action == "password":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")
            row = db.execute(
                "SELECT password_hash FROM users WHERE id=?",
                (user["id"],)
            ).fetchone()
            if not row or not check_password_hash(row["password_hash"], current_password):
                flash("Current password is incorrect.", "danger")
                return redirect(url_for("account_settings"))
            password_errors = password_strength_errors(new_password)
            if password_errors:
                flash("New password " + "; ".join(password_errors) + ".", "warning")
                return redirect(url_for("account_settings"))
            if new_password != confirm_password:
                flash("New password and confirmation do not match.", "warning")
                return redirect(url_for("account_settings"))
            db.execute(
                "UPDATE users SET password_hash=?, password_must_change=0 WHERE id=?",
                (generate_password_hash(new_password), user["id"])
            )
            db.commit()
            session.pop("must_change_password", None)
            log_audit("change_password", entity_type="user", entity_id=user["id"], details="Password updated")
            flash("Password updated.", "success")
            return redirect(url_for("account_settings"))

        if action == "admin_days":
            selected_values = request.form.getlist("admin_days")
            db.execute("DELETE FROM user_admin_days WHERE user_id=?", (user["id"],))
            for value in selected_values:
                if "|" not in value:
                    continue
                day_name, programme = value.split("|", 1)
                db.execute("""
                    INSERT OR IGNORE INTO user_admin_days (user_id, day_name, programme)
                    VALUES (?, ?, ?)
                """, (user["id"], day_name, programme))
            db.commit()
            log_audit(
                "update_admin_days",
                entity_type="user",
                entity_id=user["id"],
                details=f"Selected {len(selected_values)} admin day cells",
            )
            flash("Admin day ownership updated.", "success")
            return redirect(url_for("account_settings"))

        if action == "import_now":
            rc, _ = run_import_process(trigger="account")
            log_audit(
                "run_import",
                entity_type="system",
                entity_id="manual",
                details=f"Import trigger=account rc={rc}",
                status="ok" if rc == 0 else "warn",
            )
            if rc == 0:
                flash("Import completed.", "success")
            else:
                flash("Import failed. Check import log.", "danger")
            return redirect(url_for("account_settings"))

    selected_set = {
        f"{r['day_name']}|{r['programme']}"
        for r in db.execute(
            "SELECT day_name, programme FROM user_admin_days WHERE user_id=?",
            (user["id"],)
        ).fetchall()
    }

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    programmes = ["preschool", "honley", "lockwood"]
    grouped_options = []
    for day_name in days:
        grouped_options.append({
            "day_name": day_name,
            "cells": [
                {
                    "programme": programme,
                    "value": f"{day_name}|{programme}"
                }
                for programme in programmes
            ]
        })

    full_name_parts = (user.get("full_name") or "").strip().split()
    first_name = full_name_parts[0] if full_name_parts else ""
    last_name = " ".join(full_name_parts[1:]) if len(full_name_parts) > 1 else ""

    return render_template(
        "account.html",
        user=user,
        first_name=first_name,
        last_name=last_name,
        selected_set=selected_set,
        grouped_options=grouped_options,
        last_import=load_last_import_data()
    )


@app.route("/account/admin", methods=["GET", "POST"])
@owner_required
def account_admin():
    db = get_db()
    admin_user = current_user()

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        if action == "create_user":
            full_name = request.form.get("full_name", "").strip()
            username = request.form.get("username", "").strip().lower()
            role = request.form.get("role", "staff").strip().lower()
            password = request.form.get("password", "")
            selected_values = request.form.getlist("admin_days")

            if not full_name or not username:
                flash("Name and email are required.", "warning")
                return redirect(url_for("account_admin"))
            if role not in {"admin", "staff"}:
                role = "staff"
            password_errors = password_strength_errors(password)
            if password_errors:
                flash("Password " + "; ".join(password_errors) + ".", "warning")
                return redirect(url_for("account_admin"))

            existing = db.execute(
                "SELECT id FROM users WHERE username=?",
                (username,)
            ).fetchone()
            if existing:
                flash("That email is already used by another account.", "warning")
                return redirect(url_for("account_admin"))

            db.execute("""
                INSERT INTO users (username, password_hash, full_name, role, password_must_change)
                VALUES (?, ?, ?, ?, 1)
            """, (username, generate_password_hash(password), full_name, role))
            target_user_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            for value in selected_values:
                if "|" not in value:
                    continue
                day_name, programme = value.split("|", 1)
                if day_name not in DAY_ORDER or programme not in {"preschool", "honley", "lockwood"}:
                    continue
                db.execute("""
                    INSERT OR IGNORE INTO user_admin_days (user_id, day_name, programme)
                    VALUES (?, ?, ?)
                """, (target_user_id, day_name, programme))
            db.commit()
            log_audit(
                "admin_create_user",
                entity_type="user",
                entity_id=target_user_id,
                details=f"Created account {username} role={role} admin_days={len(selected_values)}",
            )
            flash(f"Created account: {username}", "success")
            return redirect(url_for("account_admin"))

        user_id_raw = request.form.get("user_id", "").strip()
        try:
            target_user_id = int(user_id_raw)
        except ValueError:
            flash("Invalid account selection.", "warning")
            return redirect(url_for("account_admin"))

        target_user = db.execute(
            "SELECT id, username, full_name, role FROM users WHERE id=?",
            (target_user_id,)
        ).fetchone()
        if not target_user:
            flash("Account not found.", "warning")
            return redirect(url_for("account_admin"))

        if action == "delete_user":
            if admin_user and target_user_id == admin_user["id"]:
                flash("You cannot remove your own account.", "warning")
                return redirect(url_for("account_admin"))

            target_role = (target_user["role"] or "").lower()
            if target_role == "owner":
                flash("Owner account cannot be removed.", "warning")
                return redirect(url_for("account_admin"))

            if target_role == "admin":
                admin_count = db.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE lower(role) IN ('admin','owner')"
                ).fetchone()["c"]
                if admin_count <= 1:
                    flash("Cannot remove the last admin-level account.", "warning")
                    return redirect(url_for("account_admin"))

            db.execute("DELETE FROM user_admin_days WHERE user_id=?", (target_user_id,))
            db.execute("DELETE FROM users WHERE id=?", (target_user_id,))
            db.commit()
            log_audit(
                "admin_delete_user",
                entity_type="user",
                entity_id=target_user_id,
                details=f"Deleted account {target_user['username']}",
                status="warn",
            )
            flash(f"Removed account: {target_user['username']}", "success")
            return redirect(url_for("account_admin"))

        if action == "save_user":
            full_name = request.form.get("full_name", "").strip()
            username = request.form.get("username", "").strip().lower()
            role = request.form.get("role", "staff").strip().lower()
            new_password = request.form.get("new_password", "")
            selected_values = request.form.getlist("admin_days")

            if not full_name or not username:
                flash("Name and email are required.", "warning")
                return redirect(url_for("account_admin"))

            if role not in {"admin", "staff"}:
                role = "staff"

            target_role = (target_user["role"] or "").lower()
            if target_role == "owner":
                role = "owner"

            username_conflict = db.execute(
                "SELECT id FROM users WHERE username=? AND id<>?",
                (username, target_user_id)
            ).fetchone()
            if username_conflict:
                flash("That email is already used by another account.", "warning")
                return redirect(url_for("account_admin"))

            if admin_user and target_user_id == admin_user["id"] and role not in {"admin", "owner"}:
                flash("You cannot remove your own admin role.", "warning")
                return redirect(url_for("account_admin"))

            old_role = (target_user["role"] or "").lower()
            if old_role == "admin" and role != "admin":
                admin_count = db.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE lower(role) IN ('admin','owner')"
                ).fetchone()["c"]
                if admin_count <= 1:
                    flash("Cannot demote the last admin-level account.", "warning")
                    return redirect(url_for("account_admin"))

            if new_password:
                password_errors = password_strength_errors(new_password)
                if password_errors:
                    flash("New password " + "; ".join(password_errors) + ".", "warning")
                    return redirect(url_for("account_admin"))

            db.execute(
                "UPDATE users SET full_name=?, username=?, role=? WHERE id=?",
                (full_name, username, role, target_user_id)
            )
            if new_password:
                db.execute(
                    "UPDATE users SET password_hash=?, password_must_change=1 WHERE id=?",
                    (generate_password_hash(new_password), target_user_id)
                )

            db.execute("DELETE FROM user_admin_days WHERE user_id=?", (target_user_id,))
            for value in selected_values:
                if "|" not in value:
                    continue
                day_name, programme = value.split("|", 1)
                if day_name not in DAY_ORDER or programme not in {"preschool", "honley", "lockwood"}:
                    continue
                db.execute("""
                    INSERT OR IGNORE INTO user_admin_days (user_id, day_name, programme)
                    VALUES (?, ?, ?)
                """, (target_user_id, day_name, programme))

            db.commit()
            details = f"Updated {username} role={role} admin_days={len(selected_values)}"
            if new_password:
                details += " password=changed"
            log_audit(
                "admin_update_user",
                entity_type="user",
                entity_id=target_user_id,
                details=details,
            )
            flash(f"Updated account: {username}", "success")
            return redirect(url_for("account_admin"))

        flash("Unknown admin action.", "warning")
        return redirect(url_for("account_admin"))

    logs = db.execute("""
        SELECT created_at, username, action, entity_type, entity_id, status, details
        FROM audit_logs
        ORDER BY created_at DESC, id DESC
        LIMIT 500
    """).fetchall()
    user_rows = db.execute("""
        SELECT id, username, full_name, role, created_at
        FROM users
        ORDER BY username
    """).fetchall()
    day_rows = db.execute("""
        SELECT user_id, day_name, programme
        FROM user_admin_days
        ORDER BY user_id, day_name, programme
    """).fetchall()
    assignment_map = {}
    for row in day_rows:
        key = row["user_id"]
        assignment_map.setdefault(key, set()).add(f"{row['day_name']}|{row['programme']}")

    grouped_options = []
    for day_name in WEEKDAY_NAMES:
        grouped_options.append({
            "day_name": day_name,
            "cells": [
                {
                    "programme": programme,
                    "value": f"{day_name}|{programme}"
                }
                for programme in ("preschool", "honley", "lockwood")
            ]
        })

    return render_template(
        "account_admin.html",
        logs=logs,
        users=user_rows,
        assignment_map=assignment_map,
        grouped_options=grouped_options,
    )


@app.route("/tasters")
def all_tasters():
    tasters = query("""
        SELECT
            id, child, programme, location, session, class_name,
            taster_date, attended, bg, badge, notes
        FROM tasters
        ORDER BY taster_date DESC, child
    """)
    leavers = query("""
        SELECT child, programme, leave_month, leave_date, session, class_name, email, source
        FROM leavers
        ORDER BY leave_month DESC, leave_date DESC, child
    """)
    return render_template("all_tasters.html", tasters=tasters, leavers=leavers)

# ==========================================================
# ADD TASTER
# ==========================================================

@app.route("/add", methods=["GET", "POST"])
def add():
    programme = request.args.get("programme", "lockwood")
    db = get_db()

    if request.method == "POST":
        child = request.form["child"].strip()
        taster_date = request.form["taster_date"]
        session_label = request.form["session"]
        class_name = request.form.get("class_name", "").strip()
        manual_session = request.form.get("manual_session", "").strip()
        notes = request.form.get("notes", "").strip()
        sync_excel = request.form.get("sync_excel") == "1"

        if not child or not taster_date:
            flash("Missing fields", "danger")
            return redirect(request.url)

        if session_label == "__manual__":
            session_label = manual_session
            class_name = class_name or "Manual Session"

        session_label = normalise_session_label(session_label)

        if not session_label:
            flash("Please choose a session", "danger")
            return redirect(request.url)

        db.execute("""
            INSERT INTO tasters
            (child, programme, location, session, class_name, taster_date, notes)
            VALUES (?,?,?,?,?,?,?)
        """, (
            child,
            programme,
            programme.title(),
            session_label,
            class_name,
            taster_date,
            notes,
        ))
        taster_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.commit()

        inserted = db.execute("SELECT * FROM tasters WHERE id=?", (taster_id,)).fetchone()
        actor_initials = user_initials((current_user() or {}).get("full_name", ""))
        sync_msg = "Excel sync skipped"
        sync_status = "ok"
        if sync_excel and inserted:
            ok, sync_msg = sync_taster_to_excel(inserted, mode="add", actor_initials=actor_initials)
            if not ok:
                flash(f"Taster saved in app, but Excel sync needs review: {sync_msg}", "warning")
                sync_status = "warn"

        log_audit(
            "add_taster",
            entity_type="taster",
            entity_id=taster_id,
            details=f"{child} | {programme} | {taster_date} {session_label} | excel_sync={sync_msg}",
            status=sync_status,
        )

        flash(f"Taster added for {child}", "success")
        return redirect(url_for("day_detail", date_str=taster_date, programme=programme))

    week_start_raw = request.args.get("week_start") or request.args.get("taster_date")
    if week_start_raw:
        try:
            anchor_date = datetime.fromisoformat(week_start_raw).date()
        except ValueError:
            anchor_date = date.today()
    else:
        anchor_date = date.today()

    week_start = anchor_date - timedelta(days=anchor_date.weekday())
    week_end = week_start + timedelta(days=6)
    week_days = build_week_schedule(programme, week_start)

    return render_template(
        "add.html",
        programme=programme,
        week_days=week_days,
        week_start=week_start,
        week_end=week_end,
        prev_week=(week_start - timedelta(days=7)).isoformat(),
        next_week=(week_start + timedelta(days=7)).isoformat(),
        today_str=date.today().isoformat()
    )

# ==========================================================
# IMPORT / DEV
# ==========================================================

@app.route("/_routes")
def show_routes():
    if not is_env_true("TASTERIST_DEV_TOOLS_ENABLED", "0"):
        abort(404)
    if not is_owner_user(current_user()):
        abort(403)
    return "<br>".join(
        f"{rule.endpoint} → {rule.rule}"
        for rule in app.url_map.iter_rules()
    )


@app.route("/health")
def health():
    db = get_db()
    db.execute("SELECT 1").fetchone()
    return {
        "status": "ok",
        "time": datetime.now().isoformat(timespec="seconds"),
    }


@app.route("/cloud/preflight")
@admin_required
def cloud_preflight():
    db_path = Path(DB_FILE)
    db_parent = db_path.parent
    sheets_path = Path(get_import_source_folder()).expanduser()

    def status(ok, ok_label="OK", bad_label="Needs Attention"):
        return ok_label if ok else bad_label

    checks = [
        {
            "name": "Database Directory",
            "path": str(db_parent),
            "ok": db_parent.exists() and os.access(db_parent, os.W_OK),
            "detail": "Must exist and be writable by the app process.",
        },
        {
            "name": "Database File",
            "path": str(db_path),
            "ok": (db_path.exists() and os.access(db_path, os.W_OK)) or (not db_path.exists() and os.access(db_parent, os.W_OK)),
            "detail": "Either writable existing file or writable parent for first create.",
        },
        {
            "name": "Taster Sheets Folder",
            "path": str(sheets_path),
            "ok": sheets_path.exists() and os.access(sheets_path, os.R_OK),
            "detail": "Folder must be readable in cloud for imports.",
        },
    ]
    last_import = load_last_import_data() or {}
    import_ok = (last_import.get("exit_code", 0) == 0) if last_import else False
    import_warn = len(last_import.get("warnings", [])) if last_import else 0
    checks.append({
        "name": "Latest Import",
        "path": str(last_import.get("run_at") or "Not run"),
        "ok": import_ok and import_warn == 0,
        "detail": "Should be green before onboarding staff.",
    })

    overall_ok = all(c["ok"] for c in checks)
    return render_template(
        "cloud_preflight.html",
        checks=checks,
        status=status,
        overall_ok=overall_ok,
        last_import=last_import,
    )


@app.route("/cloud/backup")
@admin_required
def cloud_backup():
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_name = f"tasterist-backup-{ts}.db"
    tmp_file = tempfile.NamedTemporaryFile(prefix="tasterist-backup-", suffix=".db", delete=False)
    tmp_file_path = tmp_file.name
    tmp_file.close()

    src = sqlite3.connect(DB_FILE, timeout=max(5, SQLITE_BUSY_TIMEOUT_MS // 1000))
    src.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    try:
        dst = sqlite3.connect(tmp_file_path, timeout=max(5, SQLITE_BUSY_TIMEOUT_MS // 1000))
        dst.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    return send_file(
        tmp_file_path,
        as_attachment=True,
        download_name=backup_name,
        mimetype="application/octet-stream"
    )

@app.route("/import")
def import_page():
    db = get_db()
    db_path = Path(DB_FILE)
    import_source = get_import_source_folder()
    import_root = Path(import_source).expanduser()
    xlsx_count = 0
    if import_root.exists():
        xlsx_count = len([
            f for f in import_root.rglob("*.xlsx")
            if not f.name.startswith("~$")
        ])
    stats = {
        "user_count": db.execute("SELECT COUNT(*) c FROM users").fetchone()["c"],
        "taster_count": db.execute("SELECT COUNT(*) c FROM tasters").fetchone()["c"],
        "leaver_count": db.execute("SELECT COUNT(*) c FROM leavers").fetchone()["c"],
        "db_file": str(db_path),
        "db_exists": db_path.exists(),
        "import_source": str(import_root),
        "import_source_exists": import_root.exists(),
        "xlsx_count": xlsx_count,
    }
    return render_template(
        "import.html",
        last_import=load_last_import_data(),
        import_source=import_source,
        storage_stats=stats,
    )


@app.post("/import/upload")
@admin_required
def import_upload():
    files = request.files.getlist("workbooks")
    if not files:
        flash("No files selected.", "warning")
        return redirect(url_for("import_page"))

    import_root = Path(get_import_source_folder()).expanduser()
    import_root.mkdir(parents=True, exist_ok=True)

    saved = 0
    skipped = 0
    for f in files:
        original_name = (f.filename or "").strip()
        if not original_name:
            skipped += 1
            continue
        safe_name = secure_filename(original_name)
        if not safe_name.lower().endswith(".xlsx"):
            skipped += 1
            continue
        m = re.search(r"(20\d{2})", safe_name)
        target_dir = import_root / m.group(1) if m else import_root
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_name
        f.save(target_path)
        saved += 1

    if saved == 0:
        flash("No valid .xlsx files uploaded.", "warning")
        return redirect(url_for("import_page"))

    flash(f"Uploaded {saved} workbook(s) to import storage.", "success")
    if skipped:
        flash(f"Skipped {skipped} file(s) (invalid or empty filename).", "warning")

    if request.form.get("run_after_upload") == "1":
        rc, _ = run_import_process(trigger="upload")
        log_audit(
            "run_import",
            entity_type="system",
            entity_id="upload",
            details=f"Import trigger=upload rc={rc}",
            status="ok" if rc == 0 else "warn",
        )
        if rc == 0:
            flash("Import complete after upload.", "success")
        else:
            flash("Upload complete, but import finished with warnings/errors.", "warning")

    return redirect(url_for("import_page"))


@app.post("/import/run")
@admin_required
def import_run():
    rc, _ = run_import_process(trigger="manual")
    log_audit(
        "run_import",
        entity_type="system",
        entity_id="manual",
        details=f"Import trigger=manual rc={rc}",
        status="ok" if rc == 0 else "warn",
    )
    if rc == 0:
        flash("Import complete", "success")
    else:
        flash("Import finished with warnings/errors. Check the log.", "warning")
    return redirect(url_for("import_page"))


@app.route("/dev", methods=["GET", "POST"])
def dev_panel():
    if not is_env_true("TASTERIST_DEV_TOOLS_ENABLED", "0"):
        abort(404)
    if not is_owner_user(current_user()):
        abort(403)
    if request.method == "POST":
        db = get_db()
        db.execute("DELETE FROM tasters")
        db.execute("DELETE FROM leavers")
        db.commit()
        flash("Database cleared", "warning")
    return render_template("dev.html")

# ==========================================================
# BOOT
# ==========================================================

init_db()

if __name__ == "__main__":
    app.run(debug=is_env_true("TASTERIST_DEBUG", "0"), port=8501)
