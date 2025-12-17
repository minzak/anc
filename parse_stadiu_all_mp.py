#!/usr/bin/python3

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

# Путь к директории с PDF-файлами
PDF_DIR = './stadiu'
#Database = './data.db'
Database = '/dev/shm/data.db'

# Имена лог файлов
SQL_LOG_FILE = '/dev/shm/sql-stadiu-' + datetime.now().strftime('%Y-%m-%d') + '.log'
PARSE_LOG_FILE = '/dev/shm/parse-stadiu-' + datetime.now().strftime('%Y-%m-%d') + '-{process_id}.log'

# Цвета для вывода в терминал
C_SUCCESS   = '\033[92m'
C_INFO      = '\033[93m'
C_DARK_GRAY = '\033[90m'
C_RESET     = '\033[0m'

# Порог объединения по вертикали для строк (по координате Y)
Y_TOLERANCE = 5  # пикселей
# Порог объединения по горизонтали для слов (по координате X)
MERGE_X_THRESHOLD = 60  # пикселей (увеличено с 15)

# Ключевые слова заголовков (для пропуска)
HEADER_KEYWORDS = ['NR', 'NR. DOSAR', 'DATA ÎNREGISTRĂРЇ', 'TERMEN', 'SOLUȚIE', 'DATĂ', 'DATA']

# --- Коды токенов для UID ---
# UID будет состоять из буквенного кода, соответствующего паттерну каждого токена (см. token_pattern)
# Если структура токена не распознана — он получает код 'Z' (UNKNOWN)
TOKEN_CODES = {
    'D': 'A',           # число (любой длины)
    'L': 'B',           # 1 буква
    'LL': 'C',          # 2 буквы
    'LLL': 'D',         # 3 буквы (ANC)
    'DD.DD.DDDD': 'E',  # дата
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
    'DDL': 'S',         # 25P (двузначное число + буква)
    'UNKNOWN': 'Z'      # fallback
}

# Удаляем все обработчики
logger.remove()

# Обработчик для консоли – только для сообщений без process и SQL
logger.add(sys.stdout, format="{message}", level="INFO")

# --- SQL логгер и setup_sql_logger ---
def setup_sql_logger(name, log_file, level=logging.INFO, mode='w'):
    log_format = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(log_format)
    l = logging.getLogger(name)
    l.setLevel(level)
    l.addHandler(handler)
    return l

# SQL логгер (глобальный как в оригинальном коде)
sql_logger = setup_sql_logger('sql_logger', SQL_LOG_FILE, mode='w')
connection = sqlite3.connect(Database)
# Устанавливаем логгер для SQL запросов. Убрать, если не нужно детально отлаживать SQL запросы
connection.set_trace_callback(sql_logger.info)
db = connection.cursor()

def normalize_token(token: str) -> str:
    token = str(token or '').strip().upper()
    # Нормализация слешей: // -> /, /// -> / и т.д.
    token = re.sub(r'/+', '/', token)
    # Нормализация пробелов вокруг слешей
    token = re.sub(r'\s*/\s*', '/', token)
    # Нормализация множественных пробелов
    token = re.sub(r'\s+', ' ', token)
    # Нормализация: 44 P 31.01.2011 -> 44/P/31.01.2011
    token = re.sub(r'(\d+)\s+([A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2/\3', token)
    # Нормализация: 40/P 26.01.2011 -> 40/P/26.01.2011
    token = re.sub(r'(\d+/[A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2', token)
    # Нормализация: 25P 18.01.2011 -> 25/P/18.01.2011
    token = re.sub(r'(\d+)([A-Z]{1,3})\s+(\d{2}\.\d{2}\.\d{4})', r'\1/\2/\3', token)
    return token

# --- Универсальная функция классификации токена ---
# Возвращает буквенный паттерн токена (например, D/L/DD.DD.DDDD) для анализа структуры
def classify_token(token: str) -> str:
    pat = token_pattern(token)
    # Сопоставляем только по основным паттернам
    if pat in TOKEN_CODES:
        return pat
    # fallback
    return 'UNKNOWN'

# Возвращает буквенный код для токена по паттерну, либо 'Z' (UNKNOWN)
def normalize_and_code(token: str) -> str:
    key = classify_token(token)
    return TOKEN_CODES.get(key, 'Z')

# --- Построение UID: буквенный код для каждого токена ---
# Результат — строка вроде 'ABCD' или 'JKL', описывающая типы токенов по позиции
def build_uid(tokens: List[str]) -> str:
    return ''.join(normalize_and_code(str(t) if t is not None else '') for t in tokens)


# --- Подсчет и маркировка отказов (рефузов) ---
#Быстро помечает в `Dosar11` отказы (refuz=1) по критерию: номер приказа `ordin` встречается ровно один раз.
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


# --- Кластеризация слов по вертикали (Y) и объединение по горизонтали (X) ---
def group_words_by_line(words: List[Dict]) -> Tuple[List[Tuple[float, List[Dict]]], List[Tuple[float, List[str]]]]:
    """
    Возвращает два списка:
    1) clusters_raw: кластеры исходных слов по Y для логирования (cy, [words])
    2) clusters_merged: кластеры после объединения текстов по X (cy, [merged_texts])
    """
    # Группируем слова по Y-координате (строкам)
    clusters_raw: List[Tuple[float, List[Dict]]] = []
    for w in words:
        placed = False
        for i, (cy, group) in enumerate(clusters_raw):
            if abs(w['top'] - cy) <= Y_TOLERANCE:
                group.append(w)
                # Пересчитываем среднюю Y-координату
                clusters_raw[i] = (sum(x['top'] for x in group)/len(group), group)
                placed = True
                break
        if not placed:
            clusters_raw.append((w['top'], [w]))

    # Сортируем кластеры по Y (сверху вниз)
    clusters_raw.sort(key=lambda x: x[0], reverse=False)

    clusters_merged: List[Tuple[float, List[str]]] = []
    for cy, group in clusters_raw:
        # Сортируем слова в группе по X-координате (слева направо)
        items = sorted(group, key=lambda w: w['x0'])

        # Объединяем слова в одну строку, если они близко по X
        merged_texts = []
        if items:
            cur_text = items[0]['text']
            cur_end_x = items[0]['x0'] + len(items[0]['text']) * 0.5

            for w in items[1:]:
                # Проверяем, является ли текущее слово или следующее датой
                is_current_date = re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", cur_text.strip())
                is_next_date = re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", w['text'].strip())

                # Если оба слова - даты, не объединяем их
                if is_current_date and is_next_date:
                    merged_texts.append(cur_text)
                    cur_text = w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5
                elif w['x0'] - cur_end_x <= MERGE_X_THRESHOLD:
                    # Слова близко - объединяем
                    cur_text += ' ' + w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5
                else:
                    # Слова далеко - начинаем новый токен
                    merged_texts.append(cur_text)
                    cur_text = w['text']
                    cur_end_x = w['x0'] + len(w['text']) * 0.5

            merged_texts.append(cur_text)

        clusters_merged.append((cy, merged_texts))

    return clusters_raw, clusters_merged

# --- Вывод табличной строки в консоль ---
def print_table_row(fields, uid, original_pattern=None):
    col_widths = [24, 18, 22, 22, 18]  # ширина колонок
    row = ''.join(
        f"{C_INFO}[{C_RESET}{str(f) if f is not None else '':>{col_widths[i]}}{C_INFO}]{C_RESET} "
        for i, f in enumerate(fields)
    )
    pattern = original_pattern if original_pattern else row_pattern(fields)
    print(f"Algo {C_SUCCESS}{uid}{C_RESET} ".ljust(20) + row + f" {C_DARK_GRAY}| PATTERN: {pattern}{C_RESET}")

# --- Разбор строки таблицы ---
def process_table_row(fields):
    # Также Удаляем "00:00:00" из полей, если присутствует
    filtered_line = [re.sub(r'/\s+', '/', (cell or '').replace('00:00:00', '').strip())
                                      for cell in fields if cell and str(cell).strip()]
    sfln = len(filtered_line)
    pattern = row_pattern(filtered_line)
    ID = DEPUN = TERMEN = ORDIN = SOLUTIE = None
    # Новые типы масок
    if sfln == 5:
        ID, DEPUN, TERMEN, ORDIN, SOLUTIE = filtered_line
    elif sfln == 4:
        ID, DEPUN, ORDIN, SOLUTIE = filtered_line
        TERMEN = None
        # Нормализация ORDIN если нужно
        if ORDIN.endswith('/P') or '/' not in ORDIN:
            year_solutie = SOLUTIE.split('.')[-1]
            if ORDIN.endswith('/P'):
                ORDIN = f"{ORDIN}/{year_solutie}"
            else:
                ORDIN = f"{ORDIN}/P/{year_solutie}"
    elif sfln == 3:
        mask_parts = pattern.split(' ')
        # Если первый токен - ORDIN, второй и третий - даты (логически связанные)
        if (
            mask_parts[0] in ['D/LL/D', 'D/L/D', 'D/LLL/D'] and
            mask_parts[1] == 'DD.DD.DDDD' and
            mask_parts[2] == 'DD.DD.DDDD'
        ):
            # Это случай типа: 146/RD/2020 06.01.2020 05.05.2020
            ID = filtered_line[0]  # 146/RD/2020
            DEPUN = filtered_line[1]  # 06.01.2020 (первая дата)
            TERMEN = filtered_line[2]  # 05.05.2020 (вторая дата)
            ORDIN = None  # нет отдельного приказа
            SOLUTIE = None  # нет отдельной даты решения
        # Если третий токен - дата (общий случай)
        elif mask_parts[2] == 'DD.DD.DDDD':
            ID = filtered_line[0]
            DEPUN = filtered_line[1]
            TERMEN = filtered_line[2]  # дата срока
            ORDIN = None   # нет приказа
            SOLUTIE = None  # нет отдельной даты решения
        # Если первый и третий токен — сложные идентификаторы, второй — дата
        elif (
            mask_parts[0] in TOKEN_CODES and
            mask_parts[1] == 'DD.DD.DDDD' and
            mask_parts[2] in TOKEN_CODES
        ):
            # Правильное распределение: ID, DEPUN, TERMEN, ORDIN, SOLUTIE
            ID = filtered_line[0]  # первый токен - ID дела
            DEPUN = filtered_line[1]  # второй токен - дата подачи
            TERMEN = None  # нет отдельного срока
            ORDIN = filtered_line[2]  # третий токен
            SOLUTIE = None

            # Если третий токен содержит дату — извлечь её
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

            # Если третий токен - сложный идентификатор
            TERMEN = None
            ORDIN = filtered_line[2]  # третий токен
            SOLUTIE = None
            # Если третий токен содержит дату — извлечь её
            if len(mask_parts) >= 3 and mask_parts[2] == 'D/L/DD.DD.DDDD':
                date_match = re.search(r'\d{2}\.\d{2}\.\d{4}', filtered_line[2])
                if date_match:
                    SOLUTIE = date_match.group(0)
    elif sfln == 2:
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
            # Случай: ORDIN дата дата (например, 146/RD/2020 06.01.2020 05.05.2020)
            if pat0 in ['D/LL/D', 'D/L/D', 'D/LLL/D'] and pat1 == 'DD.DD.DDDD' and pat2 == 'DD.DD.DDDD':
                ID = parts[0]  # 146/RD/2020
                DEPUN = parts[1]  # 06.01.2020 (первая дата)
                TERMEN = parts[2]  # 05.05.2020 (вторая дата)
                ORDIN = None  # нет отдельного приказа
                SOLUTIE = None  # нет отдельной даты решения
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
            if pat0 == 'D' and pat1 == 'DD.DD.DDDD':
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

# --- Генерация паттерна для токена ---
def token_pattern(token: str) -> str:
    """
    Возвращает буквенный паттерн токена (например, D/L/DD.DD.DDDD) для анализа структуры
    """
    tok = normalize_token(token)

    # 1. D (только цифры) - унифицируем все длины в одну D
    if re.fullmatch(r"\d+", tok):
        return 'D'

    # 2. DD.DD.DDDD (дата)
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", tok):
        return 'DD.DD.DDDD'

    # 3. L, LL, LLL (буквы)
    if re.fullmatch(r"[A-Z]{1,3}", tok):
        return 'L' * len(tok)

    # 4. D/L/DD.DD.DDDD (например, 123/P/01.01.2020)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{2}\.\d{2}\.\d{4}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DD.DD.DDDD"

    # 5. D/L/DD (например, 789/P/07)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{2}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DD"

    # 6. D/L/D (например, 123/P/2020)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/D"

    # 7. D/L/DDDD (например, 5/P/2016)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{4}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDDD"

    # 8. D/L/DDDDD (например, 91/P/20201)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{5}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDDDD"

    # 9. D/L/DDD (например, 504/P/205)
    if re.fullmatch(r"\d+/[A-Z]{1,3}/\d{3}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}/DDD"

    # 9.5. D/LLL/D (например, 2376/ANC/2014)
    if re.fullmatch(r"\d+/[A-Z]{3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/LLL/D"

    # 10. D/L (например, 156/P)
    if re.fullmatch(r"\d+/[A-Z]{1,3}", tok):
        parts = tok.split('/')
        return f"D/{'L'*len(parts[1])}"

    # 11. D/LDDDD (например, 505/P2016)
    if re.fullmatch(r"\d+/[A-Z]{1,3}\d{4,5}", tok):
        m = re.match(r"(\d+)/([A-Z]+)(\d+)", tok)
        if m:
            return f"D/{'L'*len(m.group(2))}D"

    # 12. D/*L/D (например, 1629/*P/2024)
    if re.fullmatch(r"\d+/\*[A-Z]{1,3}/\d{4,5}", tok):
        parts = tok.split('/')
        return f"D/*{'L'*len(parts[1][1:])}/D"

    # 13. D/D (например, 672/2018)
    if re.fullmatch(r"\d+/\d{4,5}", tok):
        return "D/D"

    # 14. D/DD.DD.DDDD (например, 2189/17.12.2020)
    if re.fullmatch(r"\d+/\d{2}\.\d{2}\.\d{4}", tok):
        return "D/DD.DD.DDDD"

    # 15. DDL (например, 25P - двузначное число + буква)
    if re.fullmatch(r"\d{2}[A-Z]{1,3}", tok):
        m = re.match(r"(\d{2})([A-Z]+)", tok)
        if m:
            return f"DD{'L'*len(m.group(2))}"

    # 16. DL (например, 25P - однозначное число + буква)
    if re.fullmatch(r"\d+[A-Z]{1,3}", tok):
        m = re.match(r"(\d+)([A-Z]+)", tok)
        if m:
            return f"D{'L'*len(m.group(2))}"

    # fallback: raw token
    return tok

# --- Генерация паттерна для строки ---
def row_pattern(fields: list) -> str:
    """
    Возвращает строку паттернов для всей строки (например, D/L/DD.DD.DDDD DDD.DD.DDDD)
    """
    return ' '.join(token_pattern(f) for f in fields if f)

# --- Валидация даты ---
def vali_date(date_text):
    try:
        if date_text:
            datetime.strptime(date_text, '%d.%m.%Y')
        return True
    except ValueError:
        return False

# --- Запись в БД для одной строки ---
def write_to_db(parsed):
    """Записывает одну строку в БД"""
    if not parsed:
        return

    ID, DEPUN, TERMEN, ORDIN, SOLUTIE = parsed
    if DEPUN is None or not vali_date(DEPUN) or (TERMEN is not None and not vali_date(TERMEN)) or (SOLUTIE is not None and not vali_date(SOLUTIE)):
        return

    try:
        YEAR = int(ID.split('/')[-1]) if '/' in ID else None
        NUMBER = int(ID.split('/')[0]) if '/' in ID else None
    except Exception:
        return

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

    # Коммитим каждую запись
    connection.commit()

def process_pdf(pdf_path):
    # Создаем логгер для текущего процесса как в оригинальном коде
    process_id = f"P{current_process()._identity[0]}" if current_process()._identity else "Main"
    process_logger = logger.bind(process=process_id)
    process_logger.remove()
    log_filename = PARSE_LOG_FILE.format(process_id=process_id)
    process_logger.add(log_filename,
                         format="{message}",
                         level="INFO",
                         mode='a',
                         filter=lambda record: "process" in record["extra"] and record["extra"].get("log_type", "") != "SQL")

    print(f"FILE: {pdf_path}")
    process_logger.info(f"{C_INFO}FILE: {pdf_path}{C_RESET}")

    with pdfplumber.open(pdf_path) as pdf:
        for pnum, page in enumerate(pdf.pages, start=1):
            process_logger.info(f"{pdf_path}:{pnum}")
            words = page.extract_words(x_tolerance=2, y_tolerance=2)

            clusters_raw, clusters_merged = group_words_by_line(words)
            for cy, group in clusters_raw:
                parts = " ".join(f"'{w['text']}' x0={w['x0']:.2f}" for w in sorted(group, key=lambda w: w['x0']))
                process_logger.info(f"y={cy:.1f} {parts}")

            for cy, fields in clusters_merged:
                if not fields:
                    continue
                line_text = ' '.join(fields)
                if any(kw in line_text.upper() for kw in HEADER_KEYWORDS):
                    process_logger.info(f"Header skipped: {line_text}")
                    continue
                parsed, pattern = process_table_row(fields)
                if parsed:
                    uid = build_uid(parsed)
                    print_table_row(parsed, uid, pattern)
                    process_logger.info(f"RAW: {fields} | NORM: {parsed} | UID: {uid} | PATTERN: {pattern}")

                    # Записываем в БД сразу, как в оригинальном коде
                    write_to_db(parsed)
                else:
                    uid = build_uid(fields)
                    pattern = row_pattern(fields)
                    process_logger.info(f"[SKIP] Unknown structure: {fields} | UID: {uid} | PATTERN: {pattern}")

    return f"Processed {pdf_path}"

# --- Запуск через multiprocessing ---
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

    # Парсинг в multiprocessing (запись в БД происходит в каждом процессе)
    from multiprocessing import Pool
    with Pool(processes=4) as pool:
        results = pool.map(process_pdf, files)

    # Вывод результатов
    for result in results:
        print(result)

    # Финальные апдейты
    db.execute('UPDATE Dosar11 SET result=1 WHERE result IS 0 AND ordin IN (SELECT ordin FROM Dosar11 GROUP BY ordin HAVING COUNT(*) > 1)')
    sql_logger.info('Modified UPDATE1: ' + str(db.rowcount))
    db.execute('UPDATE Dosar11 SET suplimentar=1 WHERE id IN (SELECT id FROM Termen11 GROUP BY id HAVING COUNT(*) > 1)')
    sql_logger.info('Modified UPDATE2: ' + str(db.rowcount))
    db.execute('UPDATE Dosar11 SET suplimentar=1 WHERE (JULIANDAY(termen)-JULIANDAY(depun))>365')
    sql_logger.info('Modified UPDATE3: ' + str(db.rowcount))

    # Пересчет отказов (уникальные приказы)
    recompute_refuzuri()

    connection.commit()
    connection.close()

if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {C_SUCCESS}{execution_time:.2f}{C_RESET} seconds")
