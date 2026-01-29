#!/usr/bin/env python3

import os
import re
import time
import sqlite3
import logging
from datetime import datetime
import fitz  # install using pip install PyMuPDF

import sys
sys.dont_write_bytecode = True

# Constants
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

# Record start time
start_time = time.time()

# PDF files processing
pdf_dir = './juramat/'

# Database connection
#database_path = './data.db'
database_path = '/dev/shm/data.db'

# Logging configuration
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    log_format = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(log_format)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

# Main logger
logger = setup_logger('main_logger', 'parse-juramat-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
# SQL logger
SQLlogger = setup_logger('SQLlogger', 'sql-juramat-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')

connection = sqlite3.connect(database_path)
connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()


def process_pdf(file_path, db, logger):
    duplicates = {}
    unique_records = 0
    total_records = 0
    juramat_date = None

    try:
        with fitz.open(file_path) as doc:
            print(f"{'Parsing: ' + CWARN + file_path + CEND:.<180}", end="")
            for page in doc:
                text = page.get_text()

                # Extract date from text
                if juramat_date is None:
                    match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                    if match:
                        try:
                            juramat_date = datetime.strptime(match.group(1), '%d/%m/%Y').date()
                        except ValueError:
                            juramat_date = None

                # Extract case identifiers
                dosar_ids = re.findall(r'\b\d+/\d{4}\b', text)
                filtered_ids = [
                    dosar_id for dosar_id in dosar_ids
                    if not re.match(r'\d{1,2}/\d{4}', dosar_id)  # Exclude "day/year" strings
                ]
                logger.info(f"Found dosar IDs on page: {filtered_ids}")
                for dosar_id in filtered_ids:
                    total_records += 1  # Count total number of records
                    formatted_id = f"{dosar_id.split('/')[0]}/RD/{dosar_id.split('/')[1]}"
                    if formatted_id not in duplicates:
                        duplicates[formatted_id] = {'count': 0, 'updated': False}
                        unique_records += 1  # Unique record
                    else:
                        duplicates[formatted_id]['count'] += 1

        # Process records from duplicates
        for dosar_id, data in duplicates.items():
            upsert_dosar_record(db, dosar_id, juramat_date, data['count'], logger)

        print(f"{'found ' + COK + str(unique_records).zfill(4) + CWARN + ' / ' + COK + str(total_records).zfill(4) + CEND + ' records'}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")

    return unique_records, total_records

def upsert_dosar_record(db, dosar_id, juramat_date, suplimentar_count, logger):
    # Updates or inserts a record into the database.
    try:
        db.execute('''
            INSERT INTO Dosar11 (id, year, number, juramat, suplimentar, result)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                juramat = excluded.juramat,
                suplimentar = excluded.suplimentar,
                result = excluded.result;
        ''', (
            dosar_id,
            int(dosar_id.split('/')[2]),
            int(dosar_id.split('/')[0]),
            juramat_date,
            suplimentar_count,
            "1"
        ))
        logger.info(f"Upserted record for Dosar ID: {dosar_id} with suplimentar = {suplimentar_count}")
    except Exception as e:
        logger.error(f"Error upserting record for Dosar ID: {dosar_id}: {e}")


total_files = 0
total_unique_records = 0
total_all_records = 0

for file in os.listdir(pdf_dir):
    if not file.endswith('.pdf'):
        continue

    file_path = os.path.join(pdf_dir, file)
    logger.info(f"Processing file: {file}")
    total_files += 1
    unique_records, all_records = process_pdf(file_path, db, logger)
    total_unique_records += unique_records
    total_all_records += all_records

# Save changes and close connection
connection.commit()
connection.close()

logger.info(f"Total files processed: {total_files}")
logger.info(f"Total unique records processed: {total_unique_records}")
logger.info(f"Total records (including duplicates) processed: {total_all_records}")

print(f"{'Parsing: ' + CWARN + str(total_files) + CEND + ' files':.<171}", end="")
print(f"{'unique / total ' + COK + str(total_unique_records).zfill(4) + CWARN + ' / ' + COK + str(total_all_records).zfill(4) + CEND }")

# Record end time
end_time = time.time()
# Calculate execution time
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
