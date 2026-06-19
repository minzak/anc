#!/usr/bin/env python3
"""Parse minori (minor citizenship) ordinances into the Minori11 table.

Each PDF is an ordinance approving citizenship for minors and lists the
dossiers involved, e.g. "(9791/M/2022)", "(87829/A/2019)" or older "(11436/2018)".
We extract the ordinance number + date and, per dossier, how many minors it
covers in that ordinance (siblings repeat the same id).

Run get_minori_no_ssl.py first to download; this only parses.
"""

import os
import re
import time
import sqlite3
import logging
import fitz  # pip install PyMuPDF
from datetime import datetime

import sys
sys.dont_write_bytecode = True

from incremental import db_path, id_pattern, is_reference, document_body, setup_logger, setup_issue_logger, cprint
# Reuse the robust date logic already implemented for ordinances (PyMuPDF ->
# PDFMiner fallback + filename date parser) instead of duplicating it here.
from parse_ordins_all import parse_date_from_filename, extract_date

# Constants
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

start_time = time.time()

pdf_dir = './minori/'
database_path = db_path()


logger = setup_logger('minori_logger', 'parse-minori-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
# Skip log: every match that is NOT stored as a dossier (e.g. NNNN/P/YYYY
# ordinance numbers, out-of-range years), with raw text, pattern and the
# document:page it came from — mirrors parse_stadiu's "[SKIP] ... | PATTERN:".
SkipLogger = setup_issue_logger('minori_skip_logger', 'parse-minori-issues-' + datetime.now().strftime("%Y-%m-%d") + '.log')

connection = sqlite3.connect(database_path)
db = connection.cursor()

# Make sure the table exists even when running against an already-built DB
# (create_tables.sql only runs on a fresh init_db).
db.executescript('''
CREATE TABLE IF NOT EXISTS Minori11(
    id TEXT NOT NULL,
    number INTEGER,
    year INTEGER,
    segment TEXT DEFAULT NULL,
    ordin TEXT NOT NULL,
    ordin_date DATE DEFAULT NULL,
    cminori INTEGER DEFAULT 1,
    PRIMARY KEY (id, ordin)
);
CREATE INDEX IF NOT EXISTS idx_minori11_numyear ON Minori11(number, year);
CREATE INDEX IF NOT EXISTS idx_minori11_ordin ON Minori11(ordin);
''')
connection.commit()

# number / optional alpha segment (M, A, RD...) / year. Years from the 1990s on
# are valid (dossiers are NOT limited to recent years), so no year cutoff is
# applied; non-dossier numbers are excluded by context instead (see _skip_reason).
ID_RE = re.compile(r'(\d{2,7})\s*/\s*(?:([A-Za-z]{1,6})\s*/\s*)?((?:19|20)\d{2})')

# Run-wide counters, reported by print_summary() for both _all and _new.
stats = {'files': 0, 'unique': 0, 'records': 0, 'skipped': 0,
         'skip_P': 0, 'skip_ref': 0, 'boiler': 0, 'empty': 0, 'errors': 0}


def _skip_reason(seg, pre_context):
    """Why a matched number is NOT a dossier, or None if it is one.

    `677/2001` after "Legii nr." is the data-protection law, not a dossier; it is
    effectively a `/P/`-style institutional number with the segment dropped, so we
    classify (restore the meaning of) it by its surrounding text (shared
    is_reference) rather than by a crude year cutoff.
    """
    if is_reference(pre_context):
        return 'law/regulation-reference'
    if seg == 'P':
        return 'ordinance-number(/P/)'
    return None


def _despace(s):
    return re.sub(r'\s+', ' ', s)


def print_summary():
    """Final summary, printed to console and the parse/skip logs."""
    print(f"{'-'*60}")
    print(f"Files: {COK}{stats['files']}{CEND}  Dossiers(uniq): {COK}{stats['unique']}{CEND}  Records: {stats['records']}")
    print(f"Skipped matches: {CWARN}{stats['skipped']}{CEND} "
          f"(/P/ ordin: {stats['skip_P']}, law/reg ref: {stats['skip_ref']})  "
          f"Boilerplate lines cut: {stats['boiler']}  "
          f"Empty files: {CWARN}{stats['empty']}{CEND}  Errors: {CRED}{stats['errors']}{CEND}")
    print(f"{'Parsing PDF time: '}{COK}{time.time() - start_time:.2f}{CEND} seconds")
    summary = (f"files={stats['files']} unique={stats['unique']} records={stats['records']} "
               f"skipped={stats['skipped']} skip_P={stats['skip_P']} skip_ref={stats['skip_ref']} "
               f"boiler={stats['boiler']} empty={stats['empty']} errors={stats['errors']}")
    logger.info("SUMMARY " + summary)
    # Write summary to the always-on issue log only when there is something to
    # inspect (so a perfectly clean run leaves no file).
    if stats['skipped'] or stats['empty'] or stats['errors']:
        SkipLogger.info("SUMMARY " + summary)


def extract_ordin_number(text, filename):
    head = _despace(text[:800])
    for pat in (r'\bnr\.?\s*(\d{1,5})\s*/\s*P\b',
                r'O\s*R\s*D\s*I\s*N[^0-9]{0,40}?(\d{1,5})\s*/?\s*P\b',
                r'\b(\d{1,5})\s*/\s*P\b'):
        m = re.search(pat, head, re.I)
        if m:
            return m.group(1)
    # fallback: from filename
    n = re.sub(r'^\d{4}-\d{2}-', '', filename)
    n = re.sub(r'\.pdf$', '', n, flags=re.I)
    n = re.sub(r'\b\d{1,2}[._-]\d{1,2}[._-]\d{2,4}\b', ' ', n)  # drop dates
    m = re.search(r'(?:ordin|ordon|op|ord)[^0-9]{0,12}(\d{1,5})', n, re.I)
    if m:
        return m.group(1)
    m = re.search(r'(\d{1,5})\s*[-_. ]?P\b', n, re.I)
    return m.group(1) if m else None


def process_pdf(file_path, db, logger):
    # Mirrors parse_ordins_all.parse_pdf: declare early, big try, log F/DF/DP/DR,
    # except logs the error, finally guarantees a printed status line.
    printed = False
    unique_records = 0
    total_records = 0
    file_skipped = 0
    stats['files'] += 1
    try:
        raw_filename = os.path.basename(file_path)
        date_file = parse_date_from_filename(raw_filename)          # from filename
        ordinance_date = extract_date(file_path)                    # PyMuPDF -> PDFMiner

        with fitz.open(file_path) as doc:
            pages = [page.get_text() for page in doc]
            first_page_text = pages[0] if pages else ""

            # Ordinance number: filename fallback, then header override (minori
            # headers are spaced, e.g. "O R D I N  nr. 1822/P").
            ordin_num = extract_ordin_number(first_page_text, raw_filename)

            # Date fallbacks: PDF -> filename -> YYYY-MM- archive prefix (day=01).
            if not ordinance_date and date_file:
                ordinance_date = date_file
            if not ordinance_date:
                m = re.match(r'(\d{4})-(\d{2})-', raw_filename)
                if m:
                    try:
                        ordinance_date = datetime(int(m.group(1)), int(m.group(2)), 1)
                    except ValueError:
                        pass

            ordinance_year = (ordinance_date.year if ordinance_date
                              else date_file.year if date_file else None)
            ordin = (f"{ordin_num}/P/{ordinance_year}" if ordin_num and ordinance_year
                     else f"{ordin_num}/P" if ordin_num else None)

            df_str = date_file.strftime('%Y-%m-%d') if date_file else 'None'
            dp_str = ordinance_date.strftime('%Y-%m-%d') if ordinance_date else 'None'
            logger.info(f"Parsing file: {file_path} | F:{ordin_num} DF:{df_str} DP:{dp_str} DR:{dp_str}")

            cprint(f"{'Parsing: ' + CWARN + file_path + CEND:.<170}", end="")
            if not ordin:
                logger.error(f"No ordin number in {file_path}")
                SkipLogger.info(f"[SKIP FILE] {file_path} | REASON: no-ordin-number")
                cprint(f"{CRED}no ordin number{CEND}")
                printed = True
                stats['errors'] += 1
                return 0, 0

            # Cut the header/preamble (everything up to ANEXA/LISTA) and the
            # repeating header/footer band; the dossier list is the meat between.
            body = document_body(pages, start_markers=('anex', 'lista'))
            all_lines = sum(1 for p in pages for ln in p.split('\n') if ln.strip())
            stats['boiler'] += all_lines - sum(len(lines) for _, lines in body)

            counts = {}   # id -> count of minors
            meta = {}     # id -> (number, year, segment)
            for pnum, lines in body:
                for line in lines:
                    for m in ID_RE.finditer(line):
                        num, seg, year = m.group(1), m.group(2), m.group(3)
                        raw = re.sub(r'\s+', '', m.group(0))
                        pat = id_pattern(raw)
                        seg_u = seg.upper() if seg else None
                        pre = line[max(0, m.start() - 45):m.start()]
                        # Safety net only; boilerplate (law refs) is already cut.
                        reason = _skip_reason(seg_u, pre)
                        if reason:
                            # The document's OWN ordinance number (matches the one
                            # in the title/filename) is expected — drop it silently,
                            # not as an anomaly in the skip log.
                            if reason.startswith('ordinance') and num == ordin_num:
                                continue
                            file_skipped += 1
                            stats['skip_P' if reason.startswith('ordinance') else 'skip_ref'] += 1
                            SkipLogger.info(f"[SKIP] {file_path}:{pnum} | RAW: '{raw}' | PATTERN: {pat} | REASON: {reason}")
                            continue
                        y = int(year)
                        dosar_id = f"{num}/{seg_u}/{year}" if seg_u else f"{num}/{year}"
                        total_records += 1
                        if dosar_id not in counts:
                            counts[dosar_id] = 0
                            meta[dosar_id] = (int(num), y, seg_u)
                            unique_records += 1
                        counts[dosar_id] += 1

            ordin_date_db = ordinance_date.date() if ordinance_date else None
            for dosar_id, cnt in counts.items():
                number, year, seg_u = meta[dosar_id]
                upsert_minori(db, dosar_id, number, year, seg_u, ordin, ordin_date_db, cnt, logger)

            color = COK if unique_records > 0 else CRED
            cprint(f"{'found ' + color + str(unique_records).zfill(4) + CWARN + ' / ' + COK + str(total_records).zfill(4) + CEND + ' minors, ' + CWARN + str(file_skipped).zfill(3) + CEND + ' skipped (ordin ' + str(ordin) + ')'}")
            printed = True
            logger.info(f"Processed {unique_records}/{total_records} minors, {file_skipped} skipped from {file_path} | F:{ordin_num} DF:{df_str} DP:{dp_str}\n")
            if unique_records == 0:
                stats['empty'] += 1
                SkipLogger.info(f"[EMPTY] {file_path} | ordin {ordin} | no dossiers parsed")
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
            cprint(f"{CRED}found {str(unique_records).zfill(4)} minors (error){CEND}")
    return unique_records, total_records


def upsert_minori(db, dosar_id, number, year, segment, ordin, ordin_date, cminori, logger):
    try:
        db.execute('''
            INSERT INTO Minori11 (id, number, year, segment, ordin, ordin_date, cminori)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, ordin) DO UPDATE SET
                ordin_date = excluded.ordin_date,
                cminori = excluded.cminori;
        ''', (dosar_id, number, year, segment, ordin, ordin_date, cminori))
        logger.info(f"Upserted minori {dosar_id} ordin {ordin} cminori={cminori}")
    except Exception as e:
        logger.error(f"Error upserting minori {dosar_id} / {ordin}: {e}")


def main():
    for file in sorted(os.listdir(pdf_dir)):
        if not file.lower().endswith('.pdf'):
            continue
        file_path = os.path.join(pdf_dir, file)
        logger.info(f"Processing file: {file}")
        process_pdf(file_path, db, logger)

    connection.commit()
    connection.close()
    print_summary()


if __name__ == '__main__':
    main()
