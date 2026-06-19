#!/usr/bin/env python3
"""Incremental minori parser.

Parses only minori PDFs not yet recorded in .state/minori.json, reusing the
processing/DB logic from parse_minori_all. Minori ordinances are immutable once
published, so a file is parsed exactly once.

Run get_minori_no_ssl.py first to download; this only parses.
"""

import os
import glob

import sys
sys.dont_write_bytecode = True

from parse_minori_all import process_pdf, print_summary, connection, db, logger, pdf_dir
from incremental import FileState

state = FileState('minori')
candidates = sorted(glob.glob(os.path.join(pdf_dir, '*.pdf')))
new_files = state.new_files(candidates)
print(f"New minori files: {len(new_files)} (of {len(candidates)} total)")

for file_path in new_files:
    logger.info(f"Processing file: {os.path.basename(file_path)}")
    process_pdf(file_path, db, logger)
    state.mark(file_path)  # crash-safe: persist after each file

connection.commit()
connection.close()
print_summary()
