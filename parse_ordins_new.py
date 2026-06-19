#!/usr/bin/env python3

import os
import re
import time
import pycurl
import sys
import subprocess
import glob
import logging
import sqlite3
import fitz  # pip install PyMuPDF
from pdfminer.high_level import extract_text
from io import BytesIO  # Ensure io module is correctly imported
from datetime import datetime
import unicodedata
from bs4 import BeautifulSoup

import sys
sys.dont_write_bytecode = True

# Record start time
start_time = time.time()

# Constants
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

from incremental import db_path
Database = db_path()
Ordins = './ordins/'

# Logging setup (shared factory honoring the global ANC_DEBUG switch).
from incremental import setup_logger

if __name__ == '__main__':
    logger = setup_logger('main_logger', 'parse-ordins-new-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
    SQLlogger = setup_logger('SQLlogger', 'sql-ordins-new-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')
else:
    logger = logging.getLogger('main_logger')
    SQLlogger = logging.getLogger('SQLlogger')

# Reuse parse_ordins_all's connection/cursor so that parsing (parse_pdf) and the
# refuzuri recompute below write through the SAME connection to the SAME DB.
# (Previously this script opened its own connection -> split-brain risk.)
from parse_ordins_all import parse_pdf, connection, db
from incremental import FileState

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

# PDF validity check
def is_valid_pdf(filepath):
    try:
        with fitz.open(filepath) as doc:
            return True
    except Exception:
        return False

# Use the robust downloader script (handles WAF and skips SSL verification)
# Detect files present before running the downloader, run it, then compute newly added files.
os.makedirs(Ordins, exist_ok=True)
downloader = os.path.join(os.path.dirname(__file__), 'get_ordins_no_ssl.py')
if os.path.isfile(downloader):
    print(f"Running downloader: {downloader}")
    try:
        subprocess.run([sys.executable, downloader], check=False)
    except Exception as e:
        print(f"Downloader failed: {e}")
else:
    print(f"Downloader script not found: {downloader}")

# Determine which ordin PDFs still need parsing via the shared sidecar state.
# Robust to files fetched by a separate downloader run, and recoverable after a
# crash (only files marked below are skipped next time).
state = FileState('ordins')
candidates = sorted(glob.glob(os.path.join(Ordins, '*.pdf')))
new_files = state.new_files(candidates)
print(f"New files detected: {len(new_files)} (of {len(candidates)} total)")

# --- Recompute rejections (incrementally by ordinance list) ---
def recompute_refuzuri():

    db.execute('''
        UPDATE Dosar11
        SET refuz=1
        WHERE ordin IN (
            SELECT ordin
            FROM Dosar11
            WHERE ordin IS NOT NULL
            GROUP BY ordin
            HAVING COUNT(*) = 1
        )
    ''')
    SQLlogger.info('Refuz set: ' + str(db.rowcount))
    print(f"Total refuz set to 1 with uniq ordin = 1: {COK}{str(db.rowcount)}{CEND}")

    db.execute('''
        INSERT OR REPLACE INTO Refuz11 (id, ordin, depun, solutie)
        SELECT id, ordin, depun, solutie
        FROM Dosar11
        WHERE refuz=1 AND ordin IS NOT NULL
    ''')
    SQLlogger.info('Refuz11 rebuilt: ' + str(db.rowcount))


# Parsing is delegated to parse_ordins_all.parse_pdf


logger.info('Start parsing ordins at ' + datetime.now().strftime("%Y-%m-%d %M:%S"))

# Parse only new files; mark each in the sidecar state right after (crash-safe).
for filename in new_files:
    parse_pdf(filename)
    state.mark(filename)

recompute_refuzuri()

connection.close()
logger.info("Processing complete.")

# Calculate execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
