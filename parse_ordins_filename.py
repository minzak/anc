#!/usr/bin/python3

import os
import re
import time
import sqlite3
import logging
import fitz  # pip install PyMuPDF
from datetime import datetime
import time

# Фиксируем время начала выполнения
start_time = time.time()

# Константы
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

# Logging setup
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    LogFormat = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(LogFormat)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

logger = setup_logger('main_logger', 'parse-ordins-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')

# Constants
Ordins = './ordins/'

# Parsing function
def parse_pdf(file_path):
    try:
        # Reset variables for each file
        anexa = ""
        dosars = []
        ordinance_date = None
        ordinance_number = None

        # Extract ordinance number from file name as fallback
        filename = os.path.basename(file_path)
        # Удаляем год-месяц (YYYY-MM-) в начале строки, если он есть
        filename = re.sub(r'^\d{4}-\d{2}-', '', filename)
        # Оставляем только цифры после этого
        file_ordin_number = re.sub(r'^[^\d]*?(\d+).*', r'\1', filename) if re.match(r'^[^\d]*?(\d+)', filename) else None


        with fitz.open(file_path) as doc:
            logger.info(f"Parsing file: {file_path}  F:{file_ordin_number}")
            print(f"{'Parsing: ' + CWARN + file_path + CEND:.<128}", end="")
            print(f"{COK + file_ordin_number + CEND}")

    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {e}")

# Main processing loop
for filename in os.listdir(Ordins):
    if filename.endswith(('.pdf', '.PDF')):
        parse_pdf(os.path.join(Ordins, filename))

logger.info("Processing complete.")

# Фиксируем время окончания выполнения
end_time = time.time()
# Вычисляем время выполнения
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
