#!/usr/bin/env python3

import os
import pdfplumber
import logging
import re
import time
from multiprocessing import Pool, current_process
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Tuple
import sqlite3
import sys
from loguru import logger

import sys
sys.dont_write_bytecode = True

# Path to directory with PDF files
PDF_DIR = './stadiu'
from incremental import db_path, log_dir, is_boilerplate, debug_enabled, quiet_enabled, setup_logger, setup_issue_logger
Database = db_path()

# Log file names
SQL_LOG_FILE = os.path.join(log_dir(), 'sql-stadiu-' + datetime.now().strftime('%Y-%m-%d') + '.log')
PARSE_LOG_FILE = os.path.join(log_dir(), 'parse-stadiu-' + datetime.now().strftime('%Y-%m-%d') + '-{process_id}.log')

# Colors for terminal output
C_SUCCESS   = '\033[92m'
C_INFO      = '\033[93m'
C_DARK_GRAY = '\033[90m'
C_RESET     = '\033[0m'

# Vertical merge threshold for rows (by Y coordinate)
Y_TOLERANCE = 5  # pixels
# Horizontal merge threshold for words (by X coordinate)
MERGE_X_THRESHOLD = 60  # pixels (increased from 15)

# Header keywords (to skip)
HEADER_KEYWORDS = ['NR', 'NR. DOSAR', 'DATA ÎNREGISTRĂРЇ', 'TERMEN', 'SOLUȚIE', 'DATĂ', 'DATA']

# --- Token codes for UID ---
# UID will consist of a letter code corresponding to each token's pattern (see token_pattern)
# If token structure is not recognized — it gets code 'Z' (UNKNOWN)
TOKEN_CODES = {
    'D': 'A',           # number (any length)
    'L': 'B',           # 1 letter
    'LL': 'C',          # 2 letters
    'LLL': 'D',         # 3 letters (ANC)
    'DD.DD.DDDD': 'E',  # date
    'D/L': 'F',         # 123/P
    'D/L/DD.DD.DDDD': 'G', # 123/P/01.01.2020
    'D/L/DD': 'H',      # 789/P/07
    'D/L/D': 'I',       # 123/P/2020
    'D/LDDDD': 'J',     # 505/P2016
    'D/*L/D': 'K',      # 1629/*P/2024
    'D/D': 'L',         # 672/2018
    'D/DD.DD.DDDD': 'M',# 2189/17.12.2020
    'DL': 'N',          # 25P
    'D/L/DDDD': 'O',    # 5/P/2016
    'D/L/DDDDD': 'P',   # 91/P/20201
    'D/L/DDD': 'Q',     # 504/P/205
    'D/LLL/D': 'R',     # 2376/ANC/2014
    'D/LL/D': 'T',      # 25720/RD/2020
    'D/LL/DDDD': 'U',   # 5/RD/2016
    'D/LL/DDDDD': 'V',  # 91/RD/20201
    'D/LL/DDD': 'W',    # 504/RD/205
    'D/LL/DD': 'X',     # 789/RD/07
    'D/LL': 'Y',        # 156/RD
    'D/LLL/DDDD': '1',  # 5/ANC/2016
    'D/LLL/DDDDD': '2', # 91/ANC/20201
    'D/LLL/DDD': '3',   # 504/ANC/205
    'D/LLL/DD': '4',    # 789/ANC/07
    'D/LLL': '5',       # 156/ANC
    'DDL': 'S',         # 25P (two-digit number + letter)
    'UNKNOWN': 'Z'      # fallback
}

# Remove all handlers
logger.remove()

# Console handler – only for messages without process and SQL
logger.add(sys.stdout, format="{message}", level="INFO")

# SQL logger (shared factory honoring the global ANC_DEBUG switch).
sql_logger = setup_logger('stadiu_sql_logger', SQL_LOG_FILE, mode='w')
# Always-on issue log (empty files, unknown rows, errors). Workers collect issues
# and return them; only the parent (main) writes here, to avoid a file race.
IssueLogger = setup_issue_logger('stadiu_issue_logger', os.path.join(log_dir(), 'parse-stadiu-issues-' + datetime.now().strftime('%Y-%m-%d') + '.log'))
connection = sqlite3.connect(Database)
# SQL tracing logs every statement — very heavy, only when ANC_DEBUG is on.
if debug_enabled():
    connection.set_trace_callback(sql_logger.info)
db = connection.cursor()

def normalize_token(token: str) -> str:
    token = str(token or '').strip().upper()
    # Normalize slashes: // -> /, /// -> / etc.
    token = re.sub(r'/+', '/', token)
    # Normalize spaces around slashes
    token = re.sub(r'\s*/\s*', '/', token)
    # Normalize multiple spaces
    token = re.sub(r'\s+', ' ', token)
    # Normalization: 44 P 31.01.2011 -> 44/P/31.01.2011
    token = re.sub(r'(\d+)\s+([A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2/\3', token)
    # Normalization: 40/P 26.01.2011 -> 40/P/26.01.2011
    token = re.sub(r'(\d+/[A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2', token)
    # Normalization: 25P 18.01.2011 -> 25/P/18.01.2011
    token = re.sub(r'(\d+)([A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2/\3', token)
    # Чиним битые номера приказов (потерянный/лишний слеш вокруг буквы):
    # 1099P/2025 -> 1099/P/2025 (нет слеша перед буквой)
    token = re.sub(r'^(\d+)([A-Z]{1,3})/(\d{4,5})$', r'\1/\2/\3', token)
    # 1081/P2020 -> 1081/P/2020 (нет слеша между буквой и годом)
    token = re.sub(r'^(\d+)/([A-Z]{1,3})(\d{4,5})$', r'\1/\2/\3', token)
    # 1049/P/P2023 -> 1049/P/2023 (лишняя буква перед годом)
    token = re.sub(r'^(\d+)/([A-Z]{1,3})/[A-Z]{1,3}(\d{4,5})$', r'\1/\2/\3', token)
    return token

# --- Universal token classification function ---
# Returns letter pattern of token (e.g., D/L/DD.DD.DDDD) for structure analysis
def classify_token(token: str) -> str:
    pat = token_pattern(token)
    # Match only by main patterns
    if pat in TOKEN_CODES:
        return pat
    # fallback
    return 'UNKNOWN'

# Returns letter code for token by pattern, or 'Z' (UNKNOWN)
def normalize_and_code(token: str) -> str:
    key = classify_token(token)
    return TOKEN_CODES.get(key, 'Z')

# --- UID construction: letter code for each token ---
# Result — string like 'ABCD' or 'JKL', describing token types by position
def build_uid(tokens: List[str]) -> str:
    return ''.join(normalize_and_code(str(t) if t is not None else '') for t in tokens)


# --- Count and mark rejections (refuzuri) ---
# Quickly marks rejections (refuz=1) in `Dosar11` by criterion: ordinance number `ordin` appears exactly once.
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
    sql_logger.info('Refuz set: ' + str(db.rowcount))
    print(f"Total refuz set to 1 with uniq ordin = 1: {C_SUCCESS}{str(db.rowcount)}{C_RESET}")

    db.execute('''
        INSERT OR REPLACE INTO Refuz11 (id, ordin, depun, solutie)
        SELECT id, ordin, depun, solutie
        FROM Dosar11
        WHERE refuz=1 AND ordin IS NOT NULL
    ''')
    sql_logger.info('Refuz11 rebuilt: ' + str(db.rowcount))


# --- Word clustering by vertical (Y) and merging by horizontal (X) ---
def group_words_by_line(words: List[Dict]) -> Tuple[List[Tuple[float, List[Dict]]], List[Tuple[float, List[str]]]]:
    """
    Returns two lists:
    1) clusters_raw: clusters of original words by Y for logging (cy, [words])
    2) clusters_merged: clusters after merging texts by X (cy, [merged_texts])
    """
    # Group words by Y coordinate (rows)
    clusters_raw: List[Tuple[float, List[Dict]]] = []
    for w in words:
        placed = False
        for i, (cy, group) in enumerate(clusters_raw):
            if abs(w['top'] - cy) <= Y_TOLERANCE:
                group.append(w)
                # Recalculate average Y coordinate
                clusters_raw[i] = (sum(x['top'] for x in group)/len(group), group)
                placed = True
                break
        if not placed:
            clusters_raw.append((w['top'], [w]))

    # Sort clusters by Y (top to bottom)
    clusters_raw.sort(key=lambda x: x[0], reverse=False)

    clusters_merged: List[Tuple[float, List[str]]] = []
    for cy, group in clusters_raw:
        # Sort words in group by X coordinate (left to right)
        items = sorted(group, key=lambda w: w['x0'])

        # Merge words into one string if they are close by X
        merged_texts = []
        if items:
            cur_text = items[0]['text']
            cur_end_x = items[0]['x0'] + len(items[0]['text']) * 0.5

            for w in items[1:]:
                # Check if current word or next is a date
                is_current_date = re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", cur_text.strip())
                is_next_date = re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", w['text'].strip())

                # If both words are dates, don't merge them
                if is_current_date and is_next_date:
                    merged_texts.append(cur_text)
                    cur_text = w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5
                elif w['x0'] - cur_end_x <= MERGE_X_THRESHOLD:
                    # Words close - merge
                    cur_text += ' ' + w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5
                else:
                    # Words far - start new token
                    merged_texts.append(cur_text)
                    cur_text = w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5

            merged_texts.append(cur_text)

        clusters_merged.append((cy, merged_texts))

    return clusters_raw, clusters_merged

# --- Print table row to console ---
def print_table_row(fields, uid, original_pattern=None):
    col_widths = [24, 18, 22, 22, 18]  # column widths
    row = ''.join(
        f"{C_INFO}[{C_RESET}{str(f) if f is not None else '':>{col_widths[i]}}{C_INFO}]{C_RESET} "
        for i, f in enumerate(fields)
    )
    pattern = original_pattern if original_pattern else row_pattern(fields)
    print(f"Algo {C_SUCCESS}{uid}{C_RESET} ".ljust(20) + row + f" {C_DARK_GRAY}| PATTERN: {pattern}{C_RESET}")

# --- Parse table row ---
def process_table_row(fields):
    # Also remove "00:00:00" from fields if present
    filtered_line = [re.sub(r'/\s+', '/', (cell or '').replace('00:00:00', '').strip())
                                      for cell in fields if cell and str(cell).strip()]
    sfln = len(filtered_line)
    pattern = row_pattern(filtered_line)
    ID = DEPUN = TERMEN = ORDIN = SOLUTIE = None
    # New mask types
    if sfln == 5:
        ID, DEPUN, TERMEN, ORDIN, SOLUTIE = filtered_line
    elif sfln == 4:
        mask_parts = pattern.split(' ')
        # [ID, DEPUN, TERMEN, ORDIN]: номер · дата · дата · приказ (напр. снапшоты Art-11-2019).
        if len(mask_parts) == 4 and mask_parts[1] == 'DD.DD.DDDD' and mask_parts[2] == 'DD.DD.DDDD':
            ID, DEPUN, TERMEN, ORDIN = filtered_line
            SOLUTIE = None
        else:
            ID, DEPUN, ORDIN, SOLUTIE = filtered_line
            TERMEN = None
            # Normalize ORDIN if needed
            if ORDIN.endswith('/P') or '/' not in ORDIN:
                year_solutie = SOLUTIE.split('.')[-1]
                if ORDIN.endswith('/P'):
                    ORDIN = f"{ORDIN}/{year_solutie}"
                else:
                    ORDIN = f"{ORDIN}/P/{year_solutie}"
    elif sfln == 3:
        sub = filtered_line[0].split()
        # Первое поле — склейка "номер_дела дата_подачи", далее termen + ordin:
        # напр. ['103470/RD/2019 31.12.2019', '07.05.2020', '504/P/2023'].
        if (len(sub) == 2
                and token_pattern(sub[0]) in ('D/LL/D', 'D/L/D', 'D/LLL/D')
                and token_pattern(sub[1]) == 'DD.DD.DDDD'
                and token_pattern(filtered_line[1]) == 'DD.DD.DDDD'):
            if token_pattern(filtered_line[2]) in TOKEN_CODES:
                return [sub[0], sub[1], filtered_line[1], filtered_line[2], None], pattern
            return None, pattern          # битый приказ (нет года и т.п.) — остаётся UNKNOWN
        # Обратный порядок колонок (старые снапшоты 2016): дата · номер_дела · (приказ|termen),
        # напр. ['04.05.2016', '39003/RD/2016', '360/P/2017'].
        if (token_pattern(filtered_line[0]) == 'DD.DD.DDDD'
                and token_pattern(filtered_line[1]) in ('D/LL/D', 'D/L/D', 'D/LLL/D')):
            t2 = token_pattern(filtered_line[2])
            if t2 == 'DD.DD.DDDD':
                return [filtered_line[1], filtered_line[0], filtered_line[2], None, None], pattern  # ID,DEPUN,TERMEN
            if t2 in TOKEN_CODES:
                return [filtered_line[1], filtered_line[0], None, filtered_line[2], None], pattern  # ID,DEPUN,ORDIN
            return None, pattern          # битый третий токен — остаётся UNKNOWN
        mask_parts = pattern.split(' ')
        # If first token is ORDIN, second and third are dates (logically related)
        if (
            mask_parts[0] in ['D/LL/D', 'D/L/D', 'D/LLL/D'] and
            mask_parts[1] == 'DD.DD.DDDD' and
            mask_parts[2] == 'DD.DD.DDDD'
        ):
            # This is case like: 146/RD/2020 06.01.2020 05.05.2020
            ID = filtered_line[0]  # 146/RD/2020
            DEPUN = filtered_line[1]  # 06.01.2020 (first date)
            TERMEN = filtered_line[2]  # 05.05.2020 (second date)
            ORDIN = None  # no separate ordinance
            SOLUTIE = None  # no separate decision date
        # If third token is date (general case)
        elif mask_parts[2] == 'DD.DD.DDDD':
            ID = filtered_line[0]
            DEPUN = filtered_line[1]
            TERMEN = filtered_line[2]  # term date
            ORDIN = None   # no ordinance
            SOLUTIE = None  # no separate decision date
        # If first and third tokens are complex identifiers, second is date
        elif (
            mask_parts[0] in TOKEN_CODES and
            mask_parts[1] == 'DD.DD.DDDD' and
            mask_parts[2] in TOKEN_CODES
        ):
            # Correct distribution: ID, DEPUN, TERMEN, ORDIN, SOLUTIE
            ID = filtered_line[0]  # first token - case ID
            DEPUN = filtered_line[1]  # second token - submission date
            TERMEN = None  # no separate term
            ORDIN = filtered_line[2]  # third token
            SOLUTIE = None

            # If third token contains date — extract it
            if mask_parts[2] == 'D/L/DD.DD.DDDD':
                date_match = re.search(r'\d{2}\.\d{2}\.\d{4}', filtered_line[2])
                if date_match:
                    SOLUTIE = date_match.group(0)
            else:
                ORDIN = filtered_line[2]
        else:
            # Fallback: ID, DEPUN, TERMEN, ORDIN, SOLUTIE
            ID = filtered_line[0]
            DEPUN = filtered_line[1]

            # If third token is complex identifier
            TERMEN = None
            ORDIN = filtered_line[2]  # third token
            SOLUTIE = None
            # If third token contains date — extract it
            if len(mask_parts) >= 3 and mask_parts[2] == 'D/L/DD.DD.DDDD':
                date_match = re.search(r'\d{2}\.\d{2}\.\d{4}', filtered_line[2])
                if date_match:
                    SOLUTIE = date_match.group(0)
    elif sfln == 2:
        sub = filtered_line[0].split()
        # Первое поле PDF — склейка "номер_дела дата_подачи" (напр. '100/RD/2017 03.01.2017'):
        # номер дела и дата подачи стоят близко по X и слились в одно поле. Разбираем как sfln==3.
        if (len(sub) == 2
                and token_pattern(sub[0]) in ('D/LL/D', 'D/L/D', 'D/LLL/D')
                and token_pattern(sub[1]) == 'DD.DD.DDDD'):
            ID = sub[0]                       # 100/RD/2017
            DEPUN = sub[1]                    # 03.01.2017
            t1 = token_pattern(filtered_line[1])
            if t1 == 'DD.DD.DDDD':
                TERMEN = filtered_line[1]
                ORDIN = SOLUTIE = None        # ... || termen-дата
            elif t1 in TOKEN_CODES:
                ORDIN = filtered_line[1]
                TERMEN = SOLUTIE = None       # ... || ordin
            else:
                return None, pattern          # битый 3-й токен — остаётся UNKNOWN (пофиксим позже)
        else:
            mask_parts = pattern.split(' ')
            if mask_parts[1] == 'DD.DD.DDDD':
                ID = filtered_line[0]
                DEPUN = filtered_line[1]
                TERMEN = ORDIN = SOLUTIE = None
            else:
                return None, pattern
    elif sfln == 1:
        parts = filtered_line[0].split()
        if len(parts) >= 3:
            pat0 = token_pattern(parts[0])
            pat1 = token_pattern(parts[1])
            pat2 = token_pattern(parts[2])
            # Case: ORDIN date date (e.g., 146/RD/2020 06.01.2020 05.05.2020)
            if pat0 in ['D/LL/D', 'D/L/D', 'D/LLL/D'] and pat1 == 'DD.DD.DDDD' and pat2 == 'DD.DD.DDDD':
                ID = parts[0]  # 146/RD/2020
                DEPUN = parts[1]  # 06.01.2020 (first date)
                TERMEN = parts[2]  # 05.05.2020 (second date)
                ORDIN = None  # no separate ordinance
                SOLUTIE = None  # no separate decision date
            elif pat0 == 'D' and pat1 == 'DD.DD.DDDD' and pat2 == 'DD.DD.DDDD':
                ID = parts[0]
                DEPUN = parts[1]
                TERMEN = parts[2]
                ORDIN = SOLUTIE = None
            else:
                return None, pattern
        elif len(parts) == 2:
            pat0 = token_pattern(parts[0])
            pat1 = token_pattern(parts[1])
            if pat0 in ('D', 'D/LL/D', 'D/L/D', 'D/LLL/D') and pat1 == 'DD.DD.DDDD':
                ID = parts[0]
                DEPUN = parts[1]
                TERMEN = ORDIN = SOLUTIE = None
            else:
                return None, pattern
        else:
            return None, pattern
    else:
        return None, pattern
    return [ID, DEPUN, TERMEN, ORDIN, SOLUTIE], pattern

# --- Generate pattern for token ---
def token_pattern(token: str) -> str:
    """
    Returns letter pattern of token (e.g., D/L/DD.DD.DDDD) for structure analysis
    """
    tok = normalize_token(token)

    # 1. D (digits only) - unify all lengths into one D
    if re.fullmatch(r"\d+", tok):
        return 'D'

    # 2. DD.DD.DDDD (date)
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", tok):
        return 'DD.DD.DDDD'

    # 3. L, LL, LLL (letters)
    if re.fullmatch(r"[A-Z]{1,3}", tok):
        return 'L' * len(tok)

    # 4. D/L/DD.DD.DDDD (e.g., 123/P/01.01.2020)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{2}\.\d{2}\.\d{4}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DD.DD.DDDD"

    # 5. D/L/DD (e.g., 789/P/07)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{2}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DD"

    # 6. D/L/D (e.g., 123/P/2020)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/D"

    # 7. D/L/DDDD (e.g., 5/P/2016)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{4}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDDD"

    # 8. D/L/DDDDD (e.g., 91/P/20201)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{5}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDDDD"

    # 9. D/L/DDD (e.g., 504/P/205)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{3}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDD"

    # 9.5. D/LLL/D (e.g., 2376/ANC/2014)
    if re.fullmatch(r"\d+/[A-Z]{3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/LLL/D"

    # 10. D/L (e.g., 156/P)
    if re.fullmatch(r"\d+/[A-Z]{1,3}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}"

    # 11. D/LDDDD (e.g., 505/P2016)
    if re.fullmatch(r"\d+/[A-Z]{1,3}\d{4,5}", tok):
        m = re.match(r"(\d+)/([A-Z]+)(\d+)", tok)
        if m:
            return f"D/{'L'*len(m.group(2))}D"

    # 12. D/*L/D (e.g., 1629/*P/2024)
    if re.fullmatch(r"\d+/\*[A-Z]{1,3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/*{'L'*len(parts[1][1:])}/D"

    # 13. D/D (e.g., 672/2018)
    if re.fullmatch(r"\d+/\d{4,5}", tok):
        return "D/D"

    # 14. D/DD.DD.DDDD (e.g., 2189/17.12.2020)
    if re.fullmatch(r"\d+/\d{2}\.\d{2}\.\d{4}", tok):
        return "D/DD.DD.DDDD"

    # 15. DDL (e.g., 25P - two-digit number + letter)
    if re.fullmatch(r"\d{2}[A-Z]{1,3}", tok):
        m = re.match(r"(\d{2})([A-Z]+)", tok)
        if m:
            return f"DD{'L'*len(m.group(2))}"

    # 16. DL (e.g., 25P - single-digit number + letter)
    if re.fullmatch(r"\d+[A-Z]{1,3}", tok):
        m = re.match(r"(\d+)([A-Z]+)", tok)
        if m:
            return f"D{'L'*len(m.group(2))}"

    # fallback: raw token
    return tok

# --- Generate pattern for string ---
def row_pattern(fields: list) -> str:
    """
    Returns pattern string for entire row (e.g., D/L/DD.DD.DDDD DDD.DD.DDDD)
    """
    return ' '.join(token_pattern(f) for f in fields if f)

# --- Date validation ---
def vali_date(date_text):
    try:
        if date_text:
            datetime.strptime(date_text, '%d.%m.%Y')
        return True
    except ValueError:
        return False

# --- Write to DB for one row ---
def write_to_db(parsed):
    """Writes one row to DB"""
    if not parsed:
        return

    ID, DEPUN, TERMEN, ORDIN, SOLUTIE = parsed
    if ORDIN:
        ORDIN = normalize_token(ORDIN)   # канонизируем приказ (1099P/2025 -> 1099/P/2025) для корректной группировки refuz
    if DEPUN is None or not vali_date(DEPUN) or (TERMEN is not None and not vali_date(TERMEN)) or (SOLUTIE is not None and not vali_date(SOLUTIE)):
        return

    try:
        YEAR = int(ID.split('/')[-1]) if '/' in ID else None
        NUMBER = int(ID.split('/')[0]) if '/' in ID else None
    except Exception:
        return
    if YEAR is None or NUMBER is None:
        return  # not a valid dossier id (no /year/) — skip instead of crashing on NOT NULL

    DEPUN_DATE = datetime.strptime(DEPUN, '%d.%m.%Y').date()
    TERMEN_DATE = None
    if TERMEN and vali_date(TERMEN):
        TERMEN_DATE = datetime.strptime(TERMEN, '%d.%m.%Y').date()
    SOLUTIE_DATE = None
    if SOLUTIE and vali_date(SOLUTIE):
        SOLUTIE_DATE = datetime.strptime(SOLUTIE, '%d.%m.%Y').date()

    if ORDIN:
        if SOLUTIE_DATE:
            db.execute( 'INSERT INTO Dosar11 (id, year, number, depun, solutie, ordin, result) VALUES (?, ?, ?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET solutie=?, ordin=?, result=?',
                        (ID, YEAR, NUMBER, DEPUN_DATE, SOLUTIE_DATE, ORDIN, False,
                        SOLUTIE_DATE, ORDIN, False)
                      )
            sql_logger.info('Modified1: ORDIN SOLUTIE: ' + str(db.rowcount))
        else:
            db.execute( 'INSERT INTO Dosar11 (id, year, number, depun, ordin, result) VALUES (?, ?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET ordin=?, result=?',
                        (ID, YEAR, NUMBER, DEPUN_DATE, ORDIN, False,
                        ORDIN, False)
                      )
            sql_logger.info('Modified2: ORDIN: ' + str(db.rowcount))
    else:
        if TERMEN_DATE:
            db.execute( 'INSERT INTO Dosar11 (id, year, number, depun, termen) VALUES (?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET termen=excluded.termen WHERE termen<excluded.termen OR termen IS NULL',
                        (ID, YEAR, NUMBER, DEPUN_DATE, TERMEN_DATE)
                      )
            sql_logger.info('Modified3: TERMEN Dosar11: ' + str(db.rowcount))
            db.execute( 'INSERT OR IGNORE INTO Termen11 (id, termen, stadiu) VALUES (?, ?, ?)',
                        (ID, TERMEN_DATE, None)
                      )
            sql_logger.info('Modified4: TERMEN Termen11: ' + str(db.rowcount))
        else:
            db.execute( 'INSERT OR IGNORE INTO Dosar11 (id, year, number, depun) VALUES (?, ?, ?, ?)',
                        (ID, YEAR, NUMBER, DEPUN_DATE)
                      )
            sql_logger.info('Modified5: Dosar11: ' + str(db.rowcount))

    # Commit each record
    connection.commit()

def process_pdf(pdf_path):
    # DBG   -> .log files on disk (ANC_DEBUG). QUIET -> hide console (ANC_SILENT).
    # The two are independent: console progress shows by default even with DBG off.
    DBG = debug_enabled()
    QUIET = quiet_enabled()
    process_id = f"P{current_process()._identity[0]}" if current_process()._identity else "Main"
    process_logger = logger.bind(process=process_id)
    process_logger.remove()
    if DBG:
        log_filename = PARSE_LOG_FILE.format(process_id=process_id)
        process_logger.add(log_filename,
                             format="{message}",
                             level="INFO",
                             mode='a',
                             filter=lambda record: "process" in record["extra"] and record["extra"].get("log_type", "") != "SQL")

    if not QUIET:
        print(f"FILE: {pdf_path}")
    if DBG:
        process_logger.info(f"FILE: {pdf_path}")

    n_parsed = n_header = n_boiler = n_unknown = 0
    issues = []          # anomaly lines; the PARENT writes them to the issue log
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pnum, page in enumerate(pdf.pages, start=1):
                words = page.extract_words(x_tolerance=2, y_tolerance=2)
                clusters_raw, clusters_merged = group_words_by_line(words)
                if DBG:
                    process_logger.info(f"{pdf_path}:{pnum}")
                    for cy, group in clusters_raw:
                        parts = " ".join(f"'{w['text']}' x0={w['x0']:.2f}" for w in sorted(group, key=lambda w: w['x0']))
                        process_logger.info(f"y={cy:.1f} {parts}")

                for cy, fields in clusters_merged:
                    if not fields:
                        continue
                    line_text = ' '.join(fields)
                    # Cut шапка: column-header rows (table header keywords)...
                    if any(kw in line_text.upper() for kw in HEADER_KEYWORDS):
                        n_header += 1
                        if DBG:
                            process_logger.info(f"Header skipped: {line_text}")
                        continue
                    # ...and тапки: data-protection/address/law boilerplate lines.
                    if is_boilerplate(line_text):
                        n_boiler += 1
                        if DBG:
                            process_logger.info(f"[SKIP] Boilerplate: {line_text}")
                        continue
                    parsed, pattern = process_table_row(fields)
                    if parsed:
                        # Write to DB immediately, as in original code
                        write_to_db(parsed)
                        n_parsed += 1
                        if (not QUIET) or DBG:
                            uid = build_uid(parsed)
                            if not QUIET:                       # live console progress
                                print_table_row(parsed, uid, pattern)
                            if DBG:                             # disk log
                                process_logger.info(f"RAW: {fields} | NORM: {parsed} | UID: {uid} | PATTERN: {pattern}")
                    else:
                        n_unknown += 1
                        issues.append(f"[UNKNOWN] {pdf_path}:{pnum} | {fields} | PATTERN: {row_pattern(fields)}")
                        if DBG:
                            uid = build_uid(fields)
                            pattern = row_pattern(fields)
                            process_logger.info(f"[SKIP] Unknown structure: {fields} | UID: {uid} | PATTERN: {pattern}")

        if n_parsed == 0:
            issues.append(f"[EMPTY] {pdf_path} | no rows parsed")
    except Exception as e:
        issues.append(f"[ERROR] {pdf_path} | {e}")

    return {'file': pdf_path, 'parsed': n_parsed, 'header': n_header,
            'boiler': n_boiler, 'unknown': n_unknown, 'issues': issues}

# --- Launch via multiprocessing ---
def main():
    files = []
    for d in sorted(os.listdir(PDF_DIR)):
        try:
            datetime.strptime(d, '%Y-%m-%d')
        except ValueError:
            continue
        dir_path = os.path.join(PDF_DIR, d)
        if not os.path.isdir(dir_path):
            continue
        for filename in sorted(os.listdir(dir_path)):
            if not filename.lower().endswith('.pdf'):
                continue
            filepath = os.path.abspath(os.path.join(dir_path, filename))
            files.append(filepath)
            #print(f"Parsing file: {filepath}")
    print(f"{C_SUCCESS}Found {len(files)} PDF files{C_RESET}")

    # Parsing in multiprocessing (DB writes happen in each process)
    from multiprocessing import Pool
    with Pool(processes=4) as pool:
        results = pool.map(process_pdf, files)

    # Output results + aggregate per-file counts (returned from each worker).
    agg = {'files': 0, 'parsed': 0, 'header': 0, 'boiler': 0, 'unknown': 0}
    for r in results:
        if not isinstance(r, dict):
            continue
        for line in r.get('issues', []):   # parent-only write (no file race)
            IssueLogger.info(line)
        agg['files'] += 1
        for k in ('parsed', 'header', 'boiler', 'unknown'):
            agg[k] += r.get(k, 0)
        print(f"Processed {r['file']}: parsed={r['parsed']} header={r['header']} "
              f"boiler={r['boiler']} unknown={r['unknown']}")
    print(f"{'-'*60}")
    print(f"{C_SUCCESS}Files: {agg['files']}  Parsed rows: {agg['parsed']}{C_RESET}  "
          f"Header-skipped: {agg['header']}  Boilerplate: {agg['boiler']}  "
          f"Unknown-skipped: {agg['unknown']}")

    # Final updates
    db.execute('UPDATE Dosar11 SET result=1 WHERE result IS 0 AND ordin IN (SELECT ordin FROM Dosar11 GROUP BY ordin HAVING COUNT(*) > 1)')
    sql_logger.info('Modified UPDATE1: ' + str(db.rowcount))
    db.execute('UPDATE Dosar11 SET suplimentar=1 WHERE id IN (SELECT id FROM Termen11 GROUP BY id HAVING COUNT(*) > 1)')
    sql_logger.info('Modified UPDATE2: ' + str(db.rowcount))
    db.execute('UPDATE Dosar11 SET suplimentar=1 WHERE (JULIANDAY(termen)-JULIANDAY(depun))>365')
    sql_logger.info('Modified UPDATE3: ' + str(db.rowcount))

    # Recompute rejections (unique ordinances)
    recompute_refuzuri()

    connection.commit()
    connection.close()

if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {C_SUCCESS}{execution_time:.2f}{C_RESET} seconds")
