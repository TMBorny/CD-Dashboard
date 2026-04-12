import sqlite3
import json
conn = sqlite3.connect('client-health-backend/client_health.db')
c = conn.cursor()
c.execute("SELECT DISTINCT school, display_name FROM school_snapshots")
schools = c.fetchall()
print(json.dumps(schools, indent=2))
