"""Local SQLite cache fallback for SpaceShield backend.

Provides lightweight persistence when Supabase is unavailable.
Implements minimal CRUD for satellites, debris, alerts, maneuvers, collision_events.
"""
import sqlite3
import threading
import time
from pathlib import Path
from typing import List, Dict, Optional, Any

DB_PATH = Path(__file__).parent.parent / "space_cache.db"
_lock = threading.Lock()

TABLE_DEFINITIONS = {
    "satellites": """
        CREATE TABLE IF NOT EXISTS satellites (
            id TEXT PRIMARY KEY,
            name TEXT,
            x REAL, y REAL, z REAL,
            vx REAL, vy REAL, vz REAL,
            updated_at REAL
        )
    """,
    "debris": """
        CREATE TABLE IF NOT EXISTS debris (
            id TEXT PRIMARY KEY,
            name TEXT,
            x REAL, y REAL, z REAL,
            vx REAL, vy REAL, vz REAL,
            updated_at REAL
        )
    """,
    "alerts": """
        CREATE TABLE IF NOT EXISTS alerts (
            id TEXT PRIMARY KEY,
            satellite_id TEXT,
            debris_id TEXT,
            risk_level INTEGER,
            probability REAL,
            message TEXT,
            created_at REAL
        )
    """,
    "maneuvers": """
        CREATE TABLE IF NOT EXISTS maneuvers (
            id TEXT PRIMARY KEY,
            satellite_id TEXT,
            delta_v REAL,
            delta_vx REAL, delta_vy REAL, delta_vz REAL,
            confidence REAL,
            expected_miss_improvement REAL,
            created_at REAL
        )
    """,
    "collision_events": """
        CREATE TABLE IF NOT EXISTS collision_events (
            id TEXT PRIMARY KEY,
            satellite_id TEXT,
            debris_id TEXT,
            distance_km REAL,
            tca_seconds REAL,
            probability REAL,
            risk_level INTEGER,
            created_at REAL
        )
    """
}

class LocalCache:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self._ensure_db()

    def _connect(self):
        return sqlite3.connect(self.path)

    def _ensure_db(self):
        with _lock:
            conn = self._connect()
            cur = conn.cursor()
            for ddl in TABLE_DEFINITIONS.values():
                cur.execute(ddl)
            conn.commit()
            conn.close()

    def upsert(self, table: str, record: Dict[str, Any]):
        # Get existing columns for this table
        with _lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table})")
            schema_info = {row[1]: row[2] for row in cur.fetchall()}  # {col_name: col_type}
            
            # Only include fields that exist in the table schema
            filtered_record = {}
            for k, v in record.items():
                if k not in schema_info:
                    continue
                    
                # Convert datetime strings to Unix timestamps for REAL columns
                if schema_info[k] == 'REAL' and isinstance(v, str):
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(v.replace('Z', '+00:00'))
                        filtered_record[k] = dt.timestamp()
                    except Exception:
                        # If conversion fails, skip this field
                        continue
                else:
                    filtered_record[k] = v
            
            if not filtered_record or 'id' not in filtered_record:
                conn.close()
                return
            
            keys = list(filtered_record.keys())
            placeholders = ",".join(["?"] * len(keys))
            updates = ",".join([f"{k}=excluded.{k}" for k in keys if k != "id"])
            sql = f"INSERT INTO {table} ({','.join(keys)}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}"
            values = [filtered_record[k] for k in keys]
            
            try:
                cur.execute(sql, values)
                conn.commit()
            except Exception as e:
                import logging
                logging.error(f"SQLite upsert error for table {table}: {e}")
                logging.error(f"Record: {filtered_record}")
            finally:
                conn.close()

    def get_all(self, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        with _lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table} LIMIT ?", (limit,))
            rows = cur.fetchall()
            columns = [c[0] for c in cur.description]
            conn.close()
        return [dict(zip(columns, r)) for r in rows]

    def get_by_id(self, table: str, record_id: str) -> Optional[Dict[str, Any]]:
        with _lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,))
            row = cur.fetchone()
            columns = [c[0] for c in cur.description] if cur.description else []
            conn.close()
        return dict(zip(columns, row)) if row else None

    def delete(self, table: str, record_id: str) -> bool:
        with _lock:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {table} WHERE id = ?", (record_id,))
            conn.commit()
            affected = cur.rowcount
            conn.close()
        return affected > 0

    # Convenience wrappers
    def upsert_satellite(self, sat: Dict[str, Any]):
        sat.setdefault("updated_at", time.time())
        self.upsert("satellites", sat)

    def upsert_debris(self, deb: Dict[str, Any]):
        deb.setdefault("updated_at", time.time())
        self.upsert("debris", deb)

local_cache = LocalCache()
