#!/bin/sh
set -euo pipefail
echo "[debug-entry] Starting interactive debug shell at $(date -u)" | tee -a /mnt/share/debug.log
echo "[debug-entry] Listing environment variables of interest:" | tee -a /mnt/share/debug.log
env | grep -E 'EVENTHUB|SQLSERVER|BHSIM' | tee -a /mnt/share/debug.log || true
echo "[debug-entry] ODBC drivers present:" | tee -a /mnt/share/debug.log
ls -1 /opt/microsoft/msodbcsql*/lib || true
echo "[debug-entry] Testing DNS resolution for SQL server..." | tee -a /mnt/share/debug.log
getent hosts flightopsdb.database.windows.net | tee -a /mnt/share/debug.log || echo "[debug-entry] DNS lookup failed" | tee -a /mnt/share/debug.log
echo "[debug-entry] Attempting python one-off connectivity check..." | tee -a /mnt/share/debug.log
python - <<'PYCODE' 2>&1 | tee -a /mnt/share/debug.log || true
import os, sys, time
import pyodbc
cs = os.environ.get('SQLSERVER_CONNECTION_STRING')
print('[pycheck] Using connection string:', cs)
try:
    conn = pyodbc.connect(cs, timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT SUSER_SNAME(), DB_NAME()")
    row = cur.fetchone()
    print('[pycheck] Connected as', row[0], 'db', row[1])
    conn.close()
except Exception as e:
    print('[pycheck] Exception:', e)
    import traceback; traceback.print_exc()
PYCODE
echo "[debug-entry] Sleeping indefinitely for manual inspection..." | tee -a /mnt/share/debug.log
tail -F /dev/null