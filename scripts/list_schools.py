from pathlib import Path
import json
import sqlite3

db_path = Path(__file__).resolve().parents[1] / "client-health-backend" / "client_health.db"
conn = sqlite3.connect(db_path)
c = conn.cursor()
c.execute("SELECT DISTINCT school, display_name FROM school_snapshots")
schools = c.fetchall()
print(json.dumps(schools, indent=2))
