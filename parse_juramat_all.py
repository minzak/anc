#!/usr/bin/env python3
"""Parse juramat (oath schedule) tables and set the juramat date on Dosar11.

Aligned with parse_ordins_all / parse_minori_all: shared date extraction,
the same printed/error_happened/try/finally structure, F/DF/DP logging, a
dedicated skip log (document:page + raw + pattern + reason) and a final summary.

Run get_juramat_no_ssl.py first to download; this only parses.
"""

import os
import re
import time
import sqlite3
import logging
from datetime import datetime
import fitz  # pip install PyMuPDF

import sys
sys.dont_write_bytecode = True

from incremental import db_path, id_pattern, is_reference, document_body, setup_logger, setup_issue_logger, debug_enabled, cprint
# Reuse the robust PDF date extraction (PyMuPDF -> PDFMiner) from ordinances.
from parse_ordins_all import extract_date

# Constants
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

start_time = time.time()

pdf_dir = './juramat/'
database_path = db_path()


logger = setup_logger('juramat_logger', 'parse-juramat-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
SQLlogger = setup_logger('juramat_sql_logger', 'sql-juramat-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
# Skip log: numbers that are NOT stored as dossiers (law/regulation references,
# day/year-like short ids), with raw text, pattern and document:page.
SkipLogger = setup_issue_logger('juramat_skip_logger', 'parse-juramat-issues-' + datetime.now().strftime("%Y-%m-%d") + '.log')

connection = sqlite3.connect(database_path)
if debug_enabled():
    connection.set_trace_callback(SQLlogger.info)  # logs every SQL statement
db = connection.cursor()

# Romanian month names (matched by first 3 letters; 'iui' covers the "iuilie" typo).
RO_MONTHS = {'ian': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'mai': 5, 'iun': 6,
             'iul': 7, 'iui': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'noi': 11, 'dec': 12}

# Run-wide counters, reported by print_summary() for both _all and _new.
stats = {'files': 0, 'unique': 0, 'records': 0, 'skipped': 0,
         'skip_ref': 0, 'skip_date': 0, 'boiler': 0, 'empty': 0, 'errors': 0, 'no_date': 0}


def _filename_numeric_date(name):
    """Explicit DD.MM.YYYY / DD-MM-YYYY in the filename (the ceremony date)."""
    for m in re.finditer(r'\b(\d{1,2})[.\-](\d{1,2})[.\-](20\d{2})\b', name):
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


def _filename_month_date(name):
    """DD <romanian-month> YYYY in the filename, e.g. '25-august-2025'."""
    for m in re.finditer(r'(\d{1,2})[-_.\s]+([A-Za-zăîâșțĂÎÂȘȚ]+)[-_.\s]+(20\d{2})', name):
        mon = RO_MONTHS.get(m.group(2).lower()[:3])
        if mon:
            try:
                return datetime(int(m.group(3)), mon, int(m.group(1)))
            except ValueError:
                pass
    return None


def _prefix_date(name):
    """YYYY-MM- archive prefix (day defaults to 01)."""
    m = re.match(r'(\d{4})-(\d{2})-', name)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            pass
    return None


def print_summary():
    """Final summary, printed to console and the parse/skip logs."""
    print(f"{'-'*60}")
    print(f"Files: {COK}{stats['files']}{CEND}  Records(uniq): {COK}{stats['unique']}{CEND}  Total: {stats['records']}")
    print(f"Skipped matches: {CWARN}{stats['skipped']}{CEND} "
          f"(law/reg ref: {stats['skip_ref']}, date-fragment: {stats['skip_date']})  "
          f"Boilerplate lines cut: {stats['boiler']}  "
          f"Empty files: {CWARN}{stats['empty']}{CEND}  No-date files: {CWARN}{stats['no_date']}{CEND}  "
          f"Errors: {CRED}{stats['errors']}{CEND}")
    print(f"{'Parsing PDF time: '}{COK}{time.time() - start_time:.2f}{CEND} seconds")
    summary = (f"files={stats['files']} unique={stats['unique']} records={stats['records']} "
               f"skipped={stats['skipped']} skip_ref={stats['skip_ref']} skip_date={stats['skip_date']} "
               f"boiler={stats['boiler']} empty={stats['empty']} no_date={stats['no_date']} errors={stats['errors']}")
    logger.info("SUMMARY " + summary)
    # Always-on issue log gets the summary only when there is something to inspect.
    if stats['skipped'] or stats['empty'] or stats['errors'] or stats['no_date']:
        SkipLogger.info("SUMMARY " + summary)


def process_pdf(file_path, db, logger):
    # Mirrors parse_ordins_all.parse_pdf / parse_minori_all: declare early, big
    # try, log DF/DP/DR, except logs, finally guarantees a printed status line.
    printed = False
    unique_records = 0
    total_records = 0
    file_skipped = 0
    stats['files'] += 1
    duplicates = {}   # id -> count of extra occurrences (suplimentar)
    try:
        raw_filename = os.path.basename(file_path)
        # Date priority: filename (ceremony date) -> PDF body -> archive prefix.
        date_file = _filename_numeric_date(raw_filename) or _filename_month_date(raw_filename)
        ordinance_date = extract_date(file_path)

        with fitz.open(file_path) as doc:
            pages = [page.get_text() for page in doc]
            juramat_date = date_file or ordinance_date or _prefix_date(raw_filename)
            if juramat_date is None:
                # Real anomaly (no date from filename, PDF body or YYYY-MM prefix):
                # records are still stored, but with a NULL juramat date.
                stats['no_date'] += 1
                SkipLogger.info(f"[NO-DATE] {file_path} | no date from filename / PDF / prefix")

            df_str = date_file.strftime('%Y-%m-%d') if date_file else 'None'
            dp_str = ordinance_date.strftime('%Y-%m-%d') if ordinance_date else 'None'
            dr_str = juramat_date.strftime('%Y-%m-%d') if juramat_date else 'None'
            logger.info(f"Parsing file: {file_path} | DF:{df_str} DP:{dp_str} DR:{dr_str}")

            cprint(f"{'Parsing: ' + CWARN + file_path + CEND:.<180}", end="")
            # Cut header/footer boilerplate (шапка/тапочки); parse the meat (the
            # oath table) line by line.
            body = document_body(pages)
            all_lines = sum(1 for p in pages for ln in p.split('\n') if ln.strip())
            stats['boiler'] += all_lines - sum(len(lines) for _, lines in body)

            for pnum, lines in body:
                for line in lines:
                    for m in re.finditer(r'\b(\d+)/(\d{4})\b', line):
                        raw = m.group(0)
                        num, year = m.group(1), m.group(2)
                        pre = line[max(0, m.start() - 45):m.start()]
                        if is_reference(pre):  # safety net; bands already cut
                            file_skipped += 1
                            stats['skip_ref'] += 1
                            SkipLogger.info(f"[SKIP] {file_path}:{pnum} | RAW: '{raw}' | PATTERN: {id_pattern(raw)} | REASON: law/regulation-reference")
                            continue
                        # Skip only the mm/yyyy TAIL of a dd/mm/yyyy date (e.g. header
                        # "LA DATA DE 16/12/2024"). Small dossier numbers (6/2022,
                        # 16/2022, ...) are valid and must NOT be filtered by length.
                        if re.search(r'\d{1,2}\s*/\s*$', pre):
                            file_skipped += 1
                            stats['skip_date'] += 1
                            SkipLogger.info(f"[SKIP] {file_path}:{pnum} | RAW: '{raw}' | PATTERN: {id_pattern(raw)} | REASON: date-fragment(dd/mm/yyyy)")
                            continue
                        total_records += 1
                        fid = f"{num}/RD/{year}"
                        if fid not in duplicates:
                            duplicates[fid] = 0
                            unique_records += 1
                        else:
                            duplicates[fid] += 1

            juramat_date_db = juramat_date.date() if juramat_date else None
            for fid, cnt in duplicates.items():
                upsert_dosar_record(db, fid, juramat_date_db, cnt, logger)

            color = COK if unique_records > 0 else CRED
            cprint(f"{'found ' + color + str(unique_records).zfill(4) + CWARN + ' / ' + COK + str(total_records).zfill(4) + CEND + ' records, ' + CWARN + str(file_skipped).zfill(3) + CEND + ' skipped'}")
            printed = True
            logger.info(f"Processed {unique_records}/{total_records} records, {file_skipped} skipped from {file_path} | DF:{df_str} DP:{dp_str} DR:{dr_str}\n")
            if unique_records == 0:
                stats['empty'] += 1
                SkipLogger.info(f"[EMPTY] {file_path} | no records parsed")
            stats['unique'] += unique_records
            stats['records'] += total_records
            stats['skipped'] += file_skipped
            connection.commit()
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        SkipLogger.info(f"[ERROR] {file_path} | {e}")
        stats['errors'] += 1
    finally:
        if not printed:
            cprint(f"{CRED}found {str(unique_records).zfill(4)} records (error){CEND}")
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


def main():
    for file in sorted(os.listdir(pdf_dir)):
        if not file.lower().endswith('.pdf'):
            continue
        logger.info(f"Processing file: {file}")
        process_pdf(os.path.join(pdf_dir, file), db, logger)

    connection.commit()
    connection.close()
    print_summary()


if __name__ == '__main__':
    main()
