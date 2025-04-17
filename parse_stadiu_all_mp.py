#!/usr/bin/python3
# #!venv/bin/python3

import os
import re
import sys
import sqlite3
import logging
import hashlib
from loguru import logger
from datetime import datetime
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer, LTTextLine, LAParams
from multiprocessing import current_process, Pool
from typing import Generator, List, Tuple

# Константи для кольорового оформлення
C_SUCCESS = '\033[92m'  # Зелений
C_ERROR   = '\033[91m'  # Червоний
C_INFO    = '\033[93m'  # Жовтий
C_RESET   = '\033[0m'   # Скидання кольорів

Stadiu = './stadiu/'
#Database = './data.db'

Database = '/dev/shm/data.db'
# Читаем SQL-скрипт из файла
#with open('./create_tables.sql', 'r', encoding='utf-8') as f:
#    sql_script = f.read()
# Подключаемся к базе данных
#conn = sqlite3.connect(Database)
#cursor = conn.cursor()
# Выполняем все команды из SQL-скрипта
#cursor.executescript(sql_script)
# Фиксируем изменения
#conn.commit()
#conn.close()


# Удаляем все обработчики
logger.remove()

# Обработчик для консоли – только для сообщений без process и SQL
logger.add(sys.stdout, format="{message}", level="INFO")

# sql-ГГГГ-ММ-ДД.log - лог sql-транзакций
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    log_format = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(log_format)
    l = logging.getLogger(name)
    l.setLevel(level)
    l.addHandler(handler)
    return l

# SQL логгер
sql_logger = setup_logger('sql_logger', 'sql-stadiu-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
connection = sqlite3.connect(Database)
connection.set_trace_callback(sql_logger.info)
db = connection.cursor()


# Функція перевірки коректності дати
def vali_date(date_text):
    try:
        if date_text:
            datetime.strptime(date_text, '%d.%m.%Y')
        return True
    except ValueError:
        return False


def classify_token(token: str) -> str:
    if re.fullmatch(r'\d+/?RD/?\d+', token):
        return 'ID'
    if re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', token):
        return 'D'
    if re.search(r'/P(/|\s|\d)', token):
        return 'O'
    if ' ' in token and re.search(r'\d{2}\.\d{2}\.\d{4}', token):
        return 'M'
    if re.fullmatch(r'\d+', token):
        return 'N'
    if token.strip():
        return 'T'
    return 'E'


def build_type_mask(tokens: List[str], bitmask: str) -> str:
    """Формує типову маску на основі непорожніх позицій у токенах (за raw даними)."""
    return '-'.join(classify_token(t) for i, t in enumerate(tokens) if i < len(bitmask) and bitmask[i] == '1')


def build_extended_bitmask(bitmask: str, tokens: List[str]) -> str:
    """Повертає розширену бітмаску з інформацією про типи токенів."""
    return f"{bitmask}<{build_type_mask(tokens, bitmask)}>"


def classify_ord_field(value: str) -> str:
    """Класифікація поля ORDIN за форматом."""
    value = value.strip()
    if re.fullmatch(r'\d{1,4}/P/\d{4}', value):
        return 'ORDIN_DATE'
    elif re.fullmatch(r'\d{1,4}/P', value):
        return 'ORDIN_NODATE'
    elif re.search(r'/P\s+\d{2}\.\d{2}\.\d{4}', value):
        return 'ORDIN_WEIRD_SPACED'
    elif re.search(r'\d{1,3}P\s+\d{2}\.\d{2}\.\d{4}', value):
        return 'ORDIN_WEIRD_NOSEP'
    elif re.fullmatch(r'\d{2}\.\d{2}\.\d{4}', value):
        return 'DATE_ONLY'
    elif re.fullmatch(r'\d+', value):
        return 'NUMERIC_ONLY'
    elif re.fullmatch(r'\d+/P/\d{4} \d{2}\.\d{2}\.\d{4}', value):
        return 'ORDIN_MIXED'
    return 'UNKNOWN'


def normalize_bitmask(bitmask: str, target_len: int = 8) -> str:
    """Обрізає провідні нулі та/або зводить маску до фіксованої довжини (для UID)."""
    trimmed = bitmask.lstrip('0')
    if len(trimmed) > target_len:
        return trimmed[-target_len:]
    return trimmed.rjust(target_len, '0')


def generate_hybrid_uid(bitmask: str, parts: List[str], length: int = 8) -> int:
    """Гібридний UID: хеш від типової маски (ID-D-O)."""
    typemask = build_type_mask(parts, bitmask)
    # Генерируем хеш только от типовой маски
    hash_digest = hashlib.sha256(typemask.encode()).hexdigest()
    return int(hash_digest[:8], 16)


def parse_line_with_bits(line, process_logger, bitmask=None, bmext=None, parts=None):
    # Зберігаємо повний (raw) список токенів (з пустими значеннями)
    raw_line = line[:]
    # Фільтруємо список – беремо лише непорожні токени для відображення
    filtered_line = [token for token in raw_line if token.strip() != '']
    sfln = len(filtered_line)

    DEPUN = None
    TERMEN = None
    SOLUTIE = None

    if not raw_line or sfln == 0:
        process_logger.error(f"[SKIP] line due to insufficient elements: LN: {raw_line}")
        return

    # Обчислюємо бітову маску, BM_EXT і UID, використовуючи повний (raw) список
    bitmask = generate_bitwise_id(raw_line, total_bits=len(raw_line))
    bmext = build_extended_bitmask(bitmask, raw_line)
    unique = generate_hybrid_uid(bitmask, raw_line)

    process_logger.info(f"RAW: {raw_line} LN: {filtered_line} | SFLN: {sfln} | BM: {bmext} | UID: {unique}")

    try:
        # Обработка для строк с 5 элементами
        if sfln == 5:
            ID      = filtered_line[0]
            YEAR    = ID.split('/')[-1]
            DEPUN   = filtered_line[1]
            TERMEN  = filtered_line[2]
            ORDIN   = filtered_line[3]
            SOLUTIE = filtered_line[4]
            extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
            print_formatted_row(extended_line, unique)

        # Обработка для строк с 4 элементами
        elif sfln == 4:
            ID      = filtered_line[0]
            YEAR    = ID.split('/')[-1]
            DEPUN   = filtered_line[1]
            TERMEN  = None
            ORDIN   = filtered_line[2]
            SOLUTIE = filtered_line[3]
            if ORDIN.endswith('/P'):
                year_solutie = SOLUTIE.split('.')[-1]
                ORDIN = f"{ORDIN}/{year_solutie}"
            if '/' not in ORDIN:
                year_solutie = SOLUTIE.split('.')[-1]
                ORDIN = f"{ORDIN}/P/{year_solutie}"
            extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
            print_formatted_row(extended_line, unique)

        # Обработка для строк с 3 элементами
        elif sfln == 3:
            if bmext.endswith('<ID-D-D>'):
                # Пример: ['1/RD/2020', '06.01.2020', '05.05.2020']
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = filtered_line[2]
                ORDIN   = None
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<ID-D-O>'):
                #LN: ['4/RD/2021', '04.01.2021', '858/P/12.05.2023']
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = filtered_line[2]
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<T-D-O>'):
                #LN: ['1 /RD/2012', '03.01.2012', '217/P/19.04.2013'] | SFLN: 3 | BM: 101001<T-D-O> | UID: 2262805096
                ID      = filtered_line[0].replace(' ', '')
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = filtered_line[2]
                SOLUTIE = filtered_line[2].split('/')[-1]
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<T-D-D>'):
                #LN: ['338 /RD/2012', '03.01.2012', '02.05.2012'] | SFLN: 3 | BM: 10011<T-D-D> | UID: 1837862087
                ID      = filtered_line[0].replace(' ', '')
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = filtered_line[2]
                ORDIN   = None
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<ID-D-M>'):
                #LN: ['1314/RD/2010', '15.01.2010', '148 27.09.2010'] | SFLN: 3 | BM: 1001000010<ID-D-M> | UID: 2150298860
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = filtered_line[2]
                SOLUTIE = filtered_line[2].split(' ')[-1]
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<ID-D-T>') or bmext.endswith('<ID-D-N>'):
                #LN: ['34001/RD/2015', '22.06.2015', '672/2018'] | SFLN: 3 | BM: 100100001<ID-D-T>    | UID: 473801699
                #LN: ['44914/RD/2018', '22.06.2018', '2189']     | SFLN: 3 | BM: 101000000100<ID-D-N> | UID: 1174457494
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = filtered_line[2]
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<T-D-T>'):
                #LN: ['4007 /RD/2012', '12.01.2012', '58/C/10.01.2013'] | SFLN: 3 | BM: 10010001<T-D-T> | UID: 1897143605
                ID      = filtered_line[0].replace(' ', '')
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = re.sub(r'\/.*?\/', '/P/', filtered_line[2])

                # Условие: Проверяем, является ли последний элемент датой
                #LN: ['65436 /RD/2012', '10.08.2012', '2376/ANC/2014'] | SFLN: 3 | BM: 10010001<T-D-T> | UID: 1897143605
                if re.match(r'\d{2}\.\d{2}\.\d{4}', ORDIN.split('/')[-1]):  # Шаблон даты
                   SOLUTIE = ORDIN.split('/')[-1]
                else:
                   SOLUTIE = None
                #ЕЩВЩ В идеале взять дату документа и дополнить это поле.
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<M-O-D>'):
                # Пример: ['10000/RD/2020 07.02.2020', '919/P/2023', '19.05.2023']
                ID      = filtered_line[0].split(' ')[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[0].split(' ')[-1]
                TERMEN  = None
                ORDIN   = filtered_line[1]
                SOLUTIE = filtered_line[2]
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            else:
                process_logger.info(f"ELSE: Unknown 3-element structure: RAW: {raw_line} FLN: {filtered_line} | SFLN: {sfln} | BM: {bmext} | UID: {unique}")
                return

        # Обработка для строк с 2 элементами
        elif sfln == 2:
            if bmext.endswith('<ID-D>'):
                # Пример: ['16655/RD/2023', '16.05.2023']
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1]
                TERMEN  = None
                ORDIN   = None
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<ID-M>'):
                # Пример: ['48/RD/2020', '06.01.2020 05.05.2020']
                ID      = filtered_line[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[1].split(' ')[0]
                TERMEN  = filtered_line[1].split(' ')[1]
                ORDIN   = None
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<M-D>'):
                # Пример: ['10000/RD/2021 21.05.2021', '10.09.2021']
                ID      = filtered_line[0].split(' ')[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[0].split(' ')[-1]
                TERMEN  = filtered_line[1]
                ORDIN   = None
                SOLUTIE = None
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            elif bmext.endswith('<M-O>'):
                # Пример: ['10001/RD/2020 07.02.2020', '1610/P/2023 28.09.2023']
                ID      = filtered_line[0].split(' ')[0]
                YEAR    = ID.split('/')[-1]
                DEPUN   = filtered_line[0].split(' ')[1]
                TERMEN  = None
                ORDIN   = filtered_line[1].split(' ')[0]
                SOLUTIE = filtered_line[1].split(' ')[1]
                extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
                print_formatted_row(extended_line, unique)

            else:
                process_logger.info(f"ELSE: Unknown 2-element structure: RAW: {raw_line} FLN: {filtered_line} | SFLN: {sfln} | BM: {bmext} | UID: {unique}")
                return

        # Обработка для строк с 1 элементом
        elif sfln == 1 and bmext.endswith('<M>'):
            # Пример: ['10007/RD/2020 07.02.2020 21.02.2024']
            ID      = filtered_line[0].split(' ')[0]
            YEAR    = ID.split('/')[-1]
            DEPUN   = filtered_line[0].split(' ')[1]
            TERMEN  = filtered_line[0].split(' ')[2]
            ORDIN   = None
            SOLUTIE = None
            extended_line = [ID, DEPUN, TERMEN, ORDIN, SOLUTIE]
            print_formatted_row(extended_line, unique)

        else:
            process_logger.info(f"ELSE: Unknown 1-element structure: RAW: {raw_line} FLN: {filtered_line} | SFLN: {sfln} | BM: {bmext} | UID: {unique}")
            return

    except Exception as e:
        process_logger.error(f"Error processing line: {str(e)}")
        return

    # Поля таблицы Dosar:
    # id (uniq text) - номер дела
    # year (int) - год подачи
    # number (int) - номер досара
    # depun (date) - дата подачи
    # solutie (date) - дата решения
    # ordin (text) - номер приказа
    # result (int) - результат, true - приказ, false - отказ, null - ещё неизвестно
    # termen (date) - дата последнего термена
    # suplimentar (int) - флаг дозапроса, true - по делу были дозапросы, по умолчанию false - данных по дозапросу нет, считаем, что не было.

    # Если есть поля для дальнейшей записи в БД, продолжаем валидацию и запись
    if DEPUN is None or not vali_date(DEPUN) or (TERMEN is not None and not vali_date(TERMEN)) or (SOLUTIE is not None and not vali_date(SOLUTIE)):
        process_logger.info(f"FAIL: Unknown structure string: RAW: {raw_line} FLN: {filtered_line} | SFLN: {sfln} | BM: {bmext} | UID: {unique}")
        return

    NUMBER = int(ID.split("/")[0])
    DEPUN  = datetime.strptime(DEPUN, '%d.%m.%Y').date()

    if ORDIN:
        # Если есть решение, тогда есть id, год, номер, дата подачи, дата решения, номер приказа.
        # Помечаем результат как неуспешный result=False. Корректировка результата будет на более поздних этапах: по данным приказов и по поиску неуникальных приказов.
        # Если такой ID уже есть, то вносим данные приказа (возможно, повторно вносим)
        # Даты Termen нет, поэтому её не вносим, остаётся старой
        if SOLUTIE:
            db.execute( 'INSERT INTO Dosar (id, year, number, depun, solutie, ordin, result) VALUES (?, ?, ?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET solutie=?, ordin=?, result=?',
                        (ID, YEAR, NUMBER, DEPUN, SOLUTIE, ORDIN, False,
                        SOLUTIE, ORDIN, False)
                      )
            sql_logger.info('Modified1: ORDIN SOLUTIE: ' + str(db.rowcount))
        else:
            db.execute( 'INSERT INTO Dosar (id, year, number, depun, ordin, result) VALUES (?, ?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET ordin=?, result=?',
                        (ID, YEAR, NUMBER, DEPUN, ORDIN, False,
                        ORDIN, False)
                      )
            sql_logger.info('Modified2: ORDIN: ' + str(db.rowcount))
    else:
        # Если решения нет, то id, год, номер, дата подачи и, возможно, дата термена:
        # 1) Добавляем в таблицу Dosar новые данные
        # 2) Если такой ID в таблице есть, то апдейтим термен
        # 3) Добавляем в таблицу с терменами новую запись по термену, если термен указан. Если такая же пара ID+termen существует, то данные не внесутся.
        if TERMEN:
            db.execute( 'INSERT INTO Dosar (id, year, number, depun, termen) VALUES (?, ?, ?, ?, ?) '
                        'ON CONFLICT(id) DO UPDATE SET termen=excluded.termen WHERE termen<excluded.termen OR termen IS NULL',
                        (ID, YEAR, NUMBER, DEPUN, TERMEN)
                      )
            sql_logger.info('Modified3: TERMEN Dosar: ' + str(db.rowcount))

            db.execute( 'INSERT OR IGNORE INTO Termen (id, termen, stadiu) VALUES (?, ?, ?)',
                        (ID, TERMEN, STADIUPUBDATE)
                      )
            sql_logger.info('Modified4: TERMEN Termen: ' + str(db.rowcount))
        else:
        # Если термен не указан, то просто добавляем в таблицу Dosar данные о новом деле
            pass
            db.execute( 'INSERT OR IGNORE INTO Dosar (id, year, number, depun) VALUES (?, ?, ?, ?)',
                        (ID, YEAR, NUMBER, DEPUN)
                      )
            sql_logger.info('Modified5: Dosar: ' + str(db.rowcount))

    connection.commit()


def split_merged_line(line):
    """
    Якщо в рядку міститься більше одного коректного ID, розбиває його на окремі записи.
    Повертає список рядків.
    """
    record_pattern_split = re.compile(r'\d+\s*/\s*RD\s*/\s*\d{4}')
    matches = list(record_pattern_split.finditer(line))
    if len(matches) <= 1:
        return [line]
    records = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i+1].start() if i < len(matches) - 1 else len(line)
        rec = line[start:end].strip()
        records.append(rec)
    return records


def generate_bitwise_id(tokens: List[str], total_bits: int = None) -> str:
    if total_bits is None:
        total_bits = len(tokens)
    # Використовуємо raw tokens (без видалення порожніх), щоб зберегти позиції
    return ''.join('1' if token.strip() != '' else '0' for token in tokens).zfill(total_bits)


def extract_text_with_dynamic_columns_yield(filepath, process_logger) -> Generator[Tuple[List[str], str], None, None]:
    try:
        laparams = LAParams(
            line_margin=0.3,      # Увеличиваем расстояние между строками
            char_margin=2.0,      # Увеличиваем расстояние между символами
            word_margin=0.1,      # Минимальное расстояние между словами
            line_overlap=0.3,     # Стандартное перекрытие строк
            boxes_flow=0.5,       # Настраиваем поток блоков
            detect_vertical=True, # Включаем обнаружение вертикального текста
            all_texts=False       # Отключаем обработку всех текстовых блоков
        )
        for page_number, page_layout in enumerate(extract_pages(filepath, laparams=laparams), start=1):
            process_logger.info(f"PAGE: {page_number} - {filepath}")
            try:
                rows = {}
                columns = set()
                for element in page_layout:
                    if isinstance(element, LTTextContainer):
                        for text_line in element:
                            if isinstance(text_line, LTTextLine):
                                y = round(text_line.y1, 1)
                                x = text_line.x0
                                txt = text_line.get_text().strip()
                                if txt:
                                    rows.setdefault(y, []).append((x, txt))
                                    columns.add(x)
                sorted_columns = sorted(columns)
                page_lines = []
                for y in sorted(rows.keys(), reverse=True):
                    parts = [''] * len(sorted_columns)
                    for x, txt in sorted(rows[y], key=lambda t: t[0]):
                        idx = min(range(len(sorted_columns)), key=lambda i: abs(sorted_columns[i] - x))
                        parts[idx] = txt

                    # Убираем пустые элементы в начале и в конце
                    while parts and not parts[0]:
                        parts.pop(0)
                    while parts and not parts[-1]:
                        parts.pop()

                    if parts:  # Если остались непустые элементы
                        bitmask = generate_bitwise_id(parts, total_bits=len(parts))
                        page_lines.append((parts, bitmask))

                # Остальной код без изменений
                header_keywords = ['NR', 'NR. DOSAR', 'DATA ÎNREGISTRĂРЇ', 'TERMEN', 'SOLUȚIE', 'DATĂ', 'DATA']
                merged_records = []
                current_parts = []
                current_mask = ""

                for parts, mask in page_lines:
                    try:
                        line_str = ";".join(parts)

                        # Перевіряємо, чи є в рядку заголовок
                        header_index = None
                        header_found = None
                        for keyword in header_keywords:
                            if any(keyword in p for p in parts):
                                # Якщо знайшли заголовок, пропускаємо весь рядок
                                header_found = keyword
                                break

                        if header_found:
                            process_logger.info(f"[SKIP] Header found: '{header_found}' in line: {line_str}")
                            continue

                        # Перевіряємо наявність RD формату в будь-якому токені
                        rd_found = False
                        for p in parts:
                            if p.strip() and re.fullmatch(r'\d+\s*/\s*RD\s*/\s*\d{4}', p.strip()):
                                rd_found = True
                                break

                        if rd_found:
                            if current_parts:
                                merged_records.append((current_parts, current_mask))
                            current_parts = parts
                            current_mask = mask
                        else:
                            if current_parts:
                                current_parts += parts
                                max_len = max(len(current_mask), len(mask))
                                a = current_mask.ljust(max_len, '0')
                                b = mask.ljust(max_len, '0')
                                current_mask = ''.join('1' if a[i]=='1' or b[i]=='1' else '0' for i in range(max_len))
                            else:
                                process_logger.debug(f"[INFO] Skipping non-RD line without context: {line_str}")

                    except Exception as e:
                        process_logger.error(f"Error processing line: {line_str} | Error: {str(e)}")
                        continue

                if current_parts:
                    merged_records.append((current_parts, current_mask))

                for rec_parts, rec_mask in merged_records:
                    try:
                        rec_lines = split_merged_line(";".join(rec_parts))
                        for r in rec_lines:
                            # Залишаємо всі токени, навіть якщо вони порожні – важливо для збереження позицій
                            split_line = [token for token in r.split(';')]
                            if not split_line:
                                continue
                            rec_mask = generate_bitwise_id(split_line, total_bits=len(split_line))
                            yield split_line, rec_mask
                    except Exception as e:
                        process_logger.error(f"Error processing record: {rec_parts} | Error: {str(e)}")
                        continue

            except Exception as e:
                process_logger.error(f"Error processing page {page_number}: {str(e)}")
                continue

    except Exception as e:
        process_logger.error(f"Error processing file {filepath}: {str(e)}")
        return


def print_formatted_row(line, algo_type):
    """Форматує та виводить рядок."""
    col_widths = [24, 18, 22, 22, 18]  # приклад ширини колонок
    formatted_row = "".join(f"{C_INFO}[{C_RESET}{str(cell):>{col_widths[i]}}{C_INFO}]{C_RESET} " for i, cell in enumerate(line))
    print(f"{'Algo '}{C_SUCCESS}{algo_type}{C_RESET} ".ljust(32), formatted_row)


def process_pdf(filepath):
    process_id = f"P{current_process()._identity[0]}" if current_process()._identity else "Main"
    process_logger = logger.bind(process=process_id)
    process_logger.remove()
    log_filename = f"parse-stadiu-{datetime.now().strftime('%Y-%m-%d')}-{process_id}.log"
    process_logger.add(log_filename,
                         format="{message}",
                         level="INFO",
                         mode='a',
                         filter=lambda record: "process" in record["extra"] and record["extra"].get("log_type", "") != "SQL")
    try:
        for split_line, bitmask in extract_text_with_dynamic_columns_yield(filepath, process_logger):
            parse_line_with_bits(split_line, process_logger, bitmask=bitmask, parts=split_line)
        return f"Processed {filepath}"
    except Exception as e:
        process_logger.error(f"Error processing {filepath}: {str(e)}")
        return f"Error processing {filepath}"



#FOR LAST DIR ONLY
#directories = os.listdir(Stadiu)
#all_files = []
# Сортировка директорий по дате (если они имеют формат YYYY-MM-DD)
#directory = sorted(directories, key=lambda d: d, reverse=True)[0]
#print("Last directry DATE is:", directory)

# Сохраняем дату публикации стадиу для таблицы с терменами
#STADIUPUBDATE = datetime.strptime(directory, '%Y-%m-%d').date()

# Парсим файлы PDF
#for filename in sorted(os.listdir(os.path.join(Stadiu, directory))):
#    if not filename.lower().endswith('.pdf'):
#        continue

#    filepath = os.path.join(Stadiu, directory, filename)
#    all_files.append(filepath)
#    print(f"Parsing file: {filepath}")


#FOR ALL FILES ONLY
directories = sorted(os.listdir(Stadiu))
all_files = []

# Сбор всех файлов PDF
for directory in directories:
    # Сохраняем дату публикации стадиу для таблицы с терменами
    STADIUPUBDATE = datetime.strptime(directory, '%Y-%m-%d').date()
    dir_path = os.path.join(Stadiu, directory)
    for filename in sorted(os.listdir(dir_path)):
        if filename.lower().endswith('.pdf'):
            filepath = os.path.join(dir_path, filename)
            all_files.append(filepath)

## Использование multiprocessing.Pool для обработки файлов
with Pool(processes=4) as pool:  # Количество процессов, можно настроить
    results = pool.map(process_pdf, all_files)

# Вывод результатов
for result in results:
    print(result)

# Помечаем дела с неуникальным номером приказа как положительный результат, result=true
db.execute( 'UPDATE Dosar SET result=True WHERE result IS False AND ordin IN (SELECT ordin FROM Dosar GROUP BY ordin HAVING COUNT(*) > 1)' )
sql_logger.info('Modified UPDATE1: ' + str(db.rowcount))
# Помечаем дела, для которых изменялся термен, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE id IN (SELECT id FROM Termen GROUP BY id HAVING COUNT(*) > 1)' )
sql_logger.info('Modified UPDATE2: ' + str(db.rowcount))
# Помечаем дела, для которых термен отстоит от даты подачи больше, чем на 365 дней, как дела с дозапросом suplimentar=true
db.execute( 'UPDATE Dosar SET suplimentar=True WHERE (JULIANDAY(Termen)-JULIANDAY(depun))>365' )
sql_logger.info('Modified UPDATE3: ' + str(db.rowcount))

connection.commit()
connection.close()

quit()
