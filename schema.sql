-- ==========================================
-- TASTERIST — CLEAN SCHEMA v1
-- ==========================================

PRAGMA foreign_keys = ON;

-- --------------------------
-- TASERS
-- --------------------------
CREATE TABLE tasters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    child TEXT NOT NULL,
    programme TEXT NOT NULL,         -- lockwood | honley | preschool
    location TEXT NOT NULL,          -- Lockwood | Honley

    session TEXT,                    -- "Tuesday 16:00"
    taster_date DATE NOT NULL,

    attended INTEGER DEFAULT 0,
    bg INTEGER DEFAULT 0,
    badge INTEGER DEFAULT 0,

    notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- One child can only have ONE taster per programme per date
CREATE UNIQUE INDEX uniq_taster
ON tasters (child, programme, taster_date);


-- --------------------------
-- LEAVERS (MONTH-BASED)
-- --------------------------
CREATE TABLE leavers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    child TEXT NOT NULL,
    programme TEXT NOT NULL,

    leave_month TEXT NOT NULL,       -- YYYY-MM

    source TEXT DEFAULT 'import',    -- import | manual
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Prevent duplicate leaver entries per month
CREATE UNIQUE INDEX uniq_leaver
ON leavers (child, programme, leave_month);
