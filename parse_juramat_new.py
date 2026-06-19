#!/usr/bin/env python3
"""Incremental juramat parser.

Parses only juramat PDFs not yet recorded in the .state/juramat.json sidecar,
reusing the processing/DB logic from parse_juramat_all. Juramat lists are
immutable once published, so a file is parsed exactly once.

Run the downloader (get_juramat_no_ssl.py) before this; this script only parses.
"""

import os
import glob

import sys
sys.dont_write_bytecode = True

# Importing parse_juramat_all sets up the DB connection, loggers and the
# process_pdf/print_summary helpers at module level (without running a full
# parse, which lives behind main()).
from parse_juramat_all import process_pdf, print_summary, connection, db, logger, pdf_dir
from incremental import FileState

state = FileState('juramat')
candidates = sorted(glob.glob(os.path.join(pdf_dir, '*.pdf')))
new_files = state.new_files(candidates)
print(f"New juramat files: {len(new_files)} (of {len(candidates)} total)")

for file_path in new_files:
    logger.info(f"Processing file: {os.path.basename(file_path)}")
    process_pdf(file_path, db, logger)
    state.mark(file_path)  # crash-safe: persist after each file

connection.commit()
connection.close()
print_summary()
