#!/usr/bin/python3
#!venv/bin/python3

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
from io import BytesIO  # Убедимся, что модуль io корректно импортирован
from datetime import datetime
import unicodedata
from bs4 import BeautifulSoup


# Фиксируем время начала выполнения
start_time = time.time()

# Константы
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

Database = './data.db'
Ordins = './ordins/'

# Logging setup
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    LogFormat = logging.Formatter('%(message)s')
    # Create or get existing logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding duplicate file handlers for the same file
    existing_files = set()
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler) and hasattr(h, 'baseFilename'):
            existing_files.add(os.path.abspath(h.baseFilename))

    log_file_abspath = os.path.abspath(log_file)
    if log_file_abspath not in existing_files:
        handler = logging.FileHandler(log_file, mode=mode)
        handler.setFormatter(LogFormat)
        logger.addHandler(handler)

    return logger

if __name__ == '__main__':
    logger = setup_logger('main_logger', 'parse-ordins-new-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
    SQLlogger = setup_logger('SQLlogger', 'sql-ordins-new-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')
else:
    logger = logging.getLogger('main_logger')
    SQLlogger = logging.getLogger('SQLlogger')

# Database setup
connection = sqlite3.connect(Database)
db = connection.cursor()

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

# Проверка валидности PDF
def is_valid_pdf(filepath):
    try:
        with fitz.open(filepath) as doc:
            return True
    except Exception:
        return False

# Use the robust downloader script (handles WAF and skips SSL verification)
# Detect files present before running the downloader, run it, then compute newly added files.
os.makedirs(Ordins, exist_ok=True)
before = set(glob.glob(os.path.join(Ordins, '*.pdf')))
downloader = os.path.join(os.path.dirname(__file__), 'get_ordins_no_ssl.py')
if os.path.isfile(downloader):
    print(f"Running downloader: {downloader}")
    try:
        subprocess.run([sys.executable, downloader], check=False)
    except Exception as e:
        print(f"Downloader failed: {e}")
else:
    print(f"Downloader script not found: {downloader}")

after = set(glob.glob(os.path.join(Ordins, '*.pdf')))
new_files = sorted(list(after - before))
missing_files = []
print(f"New files detected: {len(new_files)}")

from parse_ordins_all import parse_pdf

# --- Пересчет отказов (инкрементально по списку ординов) ---
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

# Парсинг только новых файлов
for filename in new_files:
    parse_pdf(filename)

recompute_refuzuri()

connection.close()
logger.info("Processing complete.")

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
