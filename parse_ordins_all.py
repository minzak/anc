#!/usr/bin/python3

import os
import re
import time
import sqlite3
import logging
import fitz  # pip install PyMuPDF
import time
import unicodedata
from pdfminer.high_level import extract_text
from datetime import datetime

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
    # Create or get existing logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Ensure we don't add duplicate handlers for the same log file
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

# Top-level loggers are only created when running as script to avoid
# duplicate handlers when this module is imported from other scripts.
if __name__ == '__main__':
    logger = setup_logger('main_logger', 'parse-ordins-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')
    SQLlogger = setup_logger('SQLlogger', 'sql-ordins-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')
else:
    # When imported, get existing loggers (may be configured by caller)
    logger = logging.getLogger('main_logger')
    SQLlogger = logging.getLogger('SQLlogger')

# Database setup
#Database = './data.db'
Database = '/dev/shm/data.db'
connection = sqlite3.connect(Database)
#connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()

# Constants
Ordins = './ordins/'

# Totals
total_dosars = 0
total_files = 0

def parse_date_from_filename(name: str):
    try:
        text = name
        # 1) dd[sep]mm[sep]yyyy
        m = re.search(r"\b(0?[1-9]|[12][0-9]|3[01])\D{0,3}(0?[1-9]|1[0-2])\D{0,3}(20\d{2})\b", text)
        if m:
            d, mo, y = m.groups()
            y = int(y)
            if 2000 <= y <= datetime.now().year + 1:
                return datetime.strptime(f"{str(d).zfill(2)}.{str(mo).zfill(2)}.{y}", "%d.%m.%Y")
        # 2) yyyy[sep]mm[sep]dd
        m = re.search(r"\b(20\d{2})\D{0,3}(0?[1-9]|1[0-2])\D{0,3}(0?[1-9]|[12][0-9]|3[01])\b", text)
        if m:
            y, mo, d = m.groups()
            y = int(y)
            if 2000 <= y <= datetime.now().year + 1:
                return datetime.strptime(f"{str(d).zfill(2)}.{str(mo).zfill(2)}.{y}", "%d.%m.%Y")
        # 3) dd[sep]MON[sep]yyyy (ENG/RO short months)
        months = {
            'IAN':1,'FEB':2,'MAR':3,'APR':4,'MAI':5,'IUN':6,'IUL':7,'AUG':8,'SEP':9,'OCT':10,'NOI':11,'DEC':12,
            'JAN':1,'MAY':5,'JUN':6,'JUL':7,'NOV':11,'DEC':12
        }
        m = re.search(r"\b(0?[1-9]|[12][0-9]|3[01])\D{0,3}([A-Za-z]{3})\D{0,3}(20\d{2})\b", text)
        if m:
            d, mon, y = m.groups()
            mon = mon.upper()
            if mon in months:
                y = int(y)
                if 2000 <= y <= datetime.now().year + 1:
                    return datetime.strptime(f"{str(d).zfill(2)}.{str(months[mon]).zfill(2)}.{y}", "%d.%m.%Y")
    except Exception:
        pass
    return None

#Extract ordinance date using PDFMiner.
def date_pdfminer(file_path):
    try:
        # Extract text from the PDF using PDFMiner
        full_text = extract_text(file_path)
        #logger.info(f"FULL_TEXT: {full_text[:400]}...")  # log first X00 characters

        # Поиск первых вхождений для ключевых слов
        keywords = ["ANEX", "LISTA", "1. "]
        longest_match = None
        max_length = 0

        for keyword in keywords:
            ordin_position = full_text.find(keyword)
            if ordin_position != -1:  # Если ключ найден
                substring = full_text[ordin_position:]
                if len(substring) > max_length:  # Проверяем длину текста после ключа
                    max_length = len(substring)
                    longest_match = substring

        # Используем найденное ключевое слово или весь текст, если ключи не найдены
        date_search_area = longest_match if longest_match else full_text
#        logger.info(f"SEARCH_AREA: {date_search_area}")

        # Date of the ordinance (search before the first LISTA)
        # Valid day 01-31, month 01-12, allow up to 3 non-digit separators (handles 22.08..2018)
        date_match = re.search(r'\b(0?[1-9]|[12][0-9]|3[01])\D{0,3}(0?[1-9]|1[0-2])\D{0,3}(20\d{2})\b', date_search_area)
        if date_match:
            day, month, year = date_match.groups()
            day = day.strip().zfill(2)
            month = month.strip().zfill(2)
            year = year.strip()
            try:
                # Validate year range to avoid false positives like 2037/2091
                current_year = datetime.now().year
                if 2000 <= int(year) <= current_year + 1:
                    return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
                else:
                    logger.warning(f"Invalid year in file {file_path}: {day}.{month}.{year}")
                    ordinance_date = None
            except ValueError:
                logger.warning(f"Invalid date format in file {file_path}: {day}.{month}.{year}")
                ordinance_date = None
        else:
            logger.warning(f"Date not found in file {file_path}")
            ordinance_date = None

    except Exception as e:
        print(f"Error extracting date with PDFMiner: {e}")
    return None

#Extract ordinance date using PyMuPDF.
def date_pymupdf(file_path):
    try:
        with fitz.open(file_path) as doc:
            # Extract text from the first page (limit to top section)
            first_page_text = doc[0].get_text() if len(doc) > 0 else ""

            # Limit search for date to text before the first ordin
            ordin_position = first_page_text.find("LISTA")
            date_search_area = first_page_text[:ordin_position] if ordin_position != -1 else first_page_text
            #logger.info(f"DATE_SEARCH_AREA: {date_search_area}")
            #logger.info(f"FIRST_PAGE_TEXT: {first_page_text}")

            # Date of the ordinance (search before the first LISTA)
            date_match = re.search(r'\b(0?[1-9]|[12][0-9]|3[01])\D{0,3}(0?[1-9]|1[0-2])\D{0,3}(20\d{2})\b', date_search_area)
            if date_match:
                day, month, year = date_match.groups()
                day = day.strip().zfill(2)
                month = month.strip().zfill(2)
                year = year.strip()
                try:
                    current_year = datetime.now().year
                    if 2000 <= int(year) <= current_year + 1:
                        return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
                    else:
                        logger.warning(f"Invalid year in file {file_path}: {day}.{month}.{year}")
                        ordinance_date = None
                except ValueError:
                    logger.warning(f"Invalid date format in file {file_path}: {day}.{month}.{year}")
                    ordinance_date = None

    except Exception as e:
        print(f"Error extracting date with PyMuPDF: {e}")
    return None


#Extract date, preferring PyMuPDF and falling back to PDFMiner if necessary.
def extract_date(file_path):
    date = date_pymupdf(file_path)
    if date:
        #return date.strftime('%d.%m.%Y')
        return date  # Return as datetime object
    logger.warning('PyMuPDF failed, falling back to PDFMiner.')
    date = date_pdfminer(file_path)
    #return date.strftime('%d.%m.%Y') if date else None
    return date  # Return as datetime object if found, otherwise None


# Parsing function
def parse_pdf(file_path):
    global total_dosars, total_files
    printed = False
    error_happened = False
    # Reset variables for each file (объявляем заранее, чтобы использовать в finally)
    anexa = None
    dosars = []
    try:

        # Extract ordinance number from file name as fallback
        raw_filename = os.path.basename(file_path)
        date_file = parse_date_from_filename(raw_filename)
        filename = re.sub(r'^\d{4}-\d{2}-', '', raw_filename)
        # Удаляем год-месяц (YYYY-MM-) в начале строки, если он есть
        # Оставляем только цифры после этого
        file_ordin_number = re.sub(r'^[^\d]*?(\d+).*', r'\1', filename) if re.match(r'^[^\d]*?(\d+)', filename) else None

        ordinance_date = extract_date(file_path)
        ordinance_year = ordinance_date.year if ordinance_date else 0

        with fitz.open(file_path) as doc:
            #logger.info(f"Parsing file: {file_path} F:{file_ordin_number} D:{ordinance_date}")
            # Пробуем извлечь номер ордина и год прямо из шапки первой страницы
            first_page_text = doc[0].get_text() if len(doc) > 0 else ""
            header_match = re.search(r"ORDIN[\s\S]{0,50}?Nr\.?\s*(\d+)\s*/\s*P(?:\s*/\s*(20\d{2}))?", first_page_text, re.IGNORECASE)
            if header_match:
                file_ordin_number = header_match.group(1)
                if header_match.group(2):
                    ordinance_year = int(header_match.group(2))
            # Если дата не определена ранее, попробуем взять её из первой страницы
            if not ordinance_date:
                date_match = re.search(r"\b(0?[1-9]|[12][0-9]|3[01])\D{0,3}(0?[1-9]|1[0-2])\D{0,3}(20\d{2})\b", first_page_text)
                if date_match:
                    d, m, y = date_match.groups()
                    try:
                        current_year = datetime.now().year
                        if 2000 <= int(y) <= current_year + 1:
                            ordinance_date = datetime.strptime(f"{d.zfill(2)}.{m.zfill(2)}.{y}", '%d.%m.%Y')
                            ordinance_year = ordinance_date.year
                        else:
                            logger.warning(f"Invalid year on first page {file_path}: {d}.{m}.{y}")
                    except Exception:
                        pass
            df_str = date_file.strftime('%Y-%m-%d') if date_file else 'None'
            dp_str = ordinance_date.strftime('%Y-%m-%d') if ordinance_date else 'None'
            # Если год всё ещё не определён, используем из дат: сначала из PDF, затем из имени файла
            if ordinance_year == 0:
                if ordinance_date:
                    ordinance_year = ordinance_date.year
                elif date_file:
                    ordinance_year = date_file.year

            # Предварительно посчитаем DR в текущем состоянии
            dr_preview = ordinance_date.strftime('%Y-%m-%d') if ordinance_date else 'None'
            logger.info(f"Parsing file: {file_path} | F:{file_ordin_number} DF:{df_str} DP:{dp_str} DR:{dr_preview}")

            # Iterate through pages to find ANEXA and dosars
            pages = [page.get_text() for page in doc]
            print(f"{'Parsing: ' + CWARN + file_path + CEND:.<205}", end="")
            for page_num, page_text in enumerate(pages):
                # Detect ANEXA by multiple formats or keywords
                # Create an ASCII-normalized version to handle diacritics vs plain text variants
                ascii_page = unicodedata.normalize('NFKD', page_text).encode('ascii', 'ignore').decode('ascii').lower()

                if re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?1', page_text, re.IGNORECASE) or re.search(r'\banexa\b\s*1', ascii_page):
                    anexa = 1
                elif re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?2', page_text, re.IGNORECASE) or re.search(r'\banexa\b\s*2', ascii_page):
                    anexa = 2
                # check for textual hints about domicile (handle diacritics by using ascii_page)
                elif 'domiciliului in strainatate' in ascii_page or 'domiciliu in strainatate' in ascii_page:
                    anexa = 1
                elif 'domiciliului in romania' in ascii_page or 'domiciliu in romania' in ascii_page:
                    anexa = 2

                # Extract dosars and children info for the current page
                # Use a unified, flexible pattern that matches:
                #  - 71425/2017
                #  - 8965/RD/2020
                #  - (dosar nr.75630/RD/2019)
                # The pattern allows an optional alpha segment (e.g. RD) between slashes.
                page_dosars = []
                # primary pattern: digits / optional alpha segment / year (works with or without parentheses)
                page_dosars += re.findall(r"\b(\d{2,7})\s*/\s*(?:[A-Za-z]{1,6}\s*/\s*)?(20\d{2})\b", page_text)
                # fallback: catch cases where the number is inside parentheses with prefixes like 'dosar nr.' without whitespace
                page_dosars += re.findall(r"\((?:[^0-9]*?)(\d{2,7})\s*/\s*(?:[A-Za-z/]*?)(20\d{2})\)", page_text)

                # Deduplicate while preserving order (per page)
                seen = set()
                ordered = []
                for dosar in page_dosars:
                    key = (dosar[0], dosar[1])
                    if key not in seen:
                        seen.add(key)
                        ordered.append(dosar)

                for dosar in ordered:
                    dosarnum, dosaryear = dosar

                    # Skip if the found pair equals the ordinance header (e.g., ORDIN 1081/P/2022)
                    try:
                        if file_ordin_number and int(dosarnum) == int(file_ordin_number) and int(dosaryear) == int(ordinance_year):
                            # this is the ordinance header, not a dosar
                            continue
                    except Exception:
                        pass

                    # Look for the number of children near the dosar occurrence.
                    # Handle variants: "Copii minori: 2", "Copii minori 2;", etc.
                    # The dosar in text may include an optional alpha segment (e.g. RD) between slashes.
                    try:
                        # match the dosar with optional alpha segment (e.g. 8965/RD/2020 or 8965/2020)
                        dosar_pattern = re.compile(rf'{re.escape(dosarnum)}\s*/\s*(?:[A-Za-z]{{1,6}}\s*/\s*)?{dosaryear}', re.IGNORECASE)
                        m = dosar_pattern.search(page_text)
                        child_count = 0
                        if m:
                            # search forward from the end of the dosar match for the "Copii minori" phrase
                            look_ahead = page_text[m.end():m.end()+200]
                            m2 = re.search(r'Copii\s+minori[:\s]*?(\d+)', look_ahead, re.IGNORECASE)
                            if m2:
                                child_count = int(m2.group(1))
                            else:
                                # if not found forward, search a bit backwards from the match start
                                look_back_start = max(0, m.start()-200)
                                look_back = page_text[look_back_start:m.start()]
                                m3 = re.search(r'Copii\s+minori[:\s]*?(\d+)', look_back, re.IGNORECASE)
                                if m3:
                                    child_count = int(m3.group(1))
                        else:
                            # if dosar string wasn't found exactly (rare), fallback to a page-wide search
                            m4 = re.search(r'Copii\s+minori[:\s]*?(\d+)', page_text, re.IGNORECASE)
                            if m4:
                                child_count = int(m4.group(1))
                    except Exception:
                        child_count = 0

                    dosars.append({
                        "id": f"{dosarnum}/RD/{dosaryear}",
                        "anexa": anexa,
                        "cminori": child_count,
                        "year": int(dosaryear)
                    })
                    # Log discovery; ANEXA may still be undetermined at this point (document-wide fallback runs later)
                    # logger.info(f'Found Dosar: {file_ordin_number}/P/{ordinance_year} {dosarnum}/RD/{dosaryear} Minori:{child_count} ANEXA:{anexa if anexa is not None else "pending"}')

            # Подготовим финальную дату один раз (может быть None, если даты нет)
            # If ordinance_date wasn't found in the PDF, prefer the date parsed from the filename
            if ordinance_date:
                clean_ordinance_date = ordinance_date.strftime('%Y-%m-%d')
            elif date_file:
                clean_ordinance_date = date_file.strftime('%Y-%m-%d')
                # also set ordinance_date/year for consistency further down
                ordinance_date = date_file
                ordinance_year = date_file.year
            else:
                clean_ordinance_date = None

            # If ANEXA wasn't determined per-page, do a document-wide fallback scan and
            # decide between 1 (domicile abroad) or 2 (Bucharest/local). Default to 2.
            if anexa is None:
                full_text = "\n".join(pages)
                ascii_full = unicodedata.normalize('NFKD', full_text).encode('ascii', 'ignore').decode('ascii').lower()
                if re.search(r'anexa\s*1', ascii_full) or 'in strainatate' in ascii_full or 'în străinătate' in ascii_full or 'străinătate' in ascii_full:
                    anexa = 1
                elif re.search(r'anexa\s*2', ascii_full) or 'bucurest' in ascii_full or 'in bucuresti' in ascii_full or 'municipiul bucuresti' in ascii_full or 'în România' in ascii_full:
                    anexa = 2
                else:
                    anexa = 2

            # Ensure each dosar uses the resolved anexa value (document-wide fallback may have set it)
            for dosar in dosars:
                if dosar.get('anexa') is None:
                    dosar['anexa'] = anexa

            # Process each dosar and update database; log final ANEXA when updating
            for dosar in dosars:
                logger.info(f'Ordin: {file_ordin_number}/P/{ordinance_year} Dosar {dosar["id"]} ANEXA:{dosar["anexa"]} Minori: {dosar["cminori"]}')
                db.execute('''
                    UPDATE Dosar11
                    SET solutie = IIF(Dosar11.solutie IS NULL, ?, Dosar11.solutie),
                        result = ?,
                        ordin = ?,
                        anexa = ?,
                        cminori = ?
                    WHERE id = ?
                ''', (clean_ordinance_date, True, f"{file_ordin_number}/P/{ordinance_year}", dosar["anexa"], dosar["cminori"], dosar["id"]))
                #SQLlogger.info('Modified: ' + str(db.rowcount))
                # Log the update in a more compact format
                SQLlogger.info(f"UPDATE: ID:{dosar['id']} | Result:{True} | Ordin_date {clean_ordinance_date} | Ordin:{file_ordin_number}/P/{ordinance_year} | ANEXA:{dosar['anexa']} | Minori:{dosar['cminori']} | Modified:{db.rowcount} rows")

            # Final date for DB (result)
            dr_str = clean_ordinance_date if clean_ordinance_date else 'None'
            count = len(dosars)
            color = COK if count > 0 else CRED
            print(f"{'found '}{color}{str(count).zfill(4)}{CEND}{' dosars'}")
            printed = True
            # Единый формат с Parsing file
            logger.info(f"Processed {len(dosars)} dosars from {file_path} | F:{file_ordin_number} DF:{df_str} DP:{dp_str} DR:{dr_str}\n")
            connection.commit()

    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {e}")
        error_happened = True
    finally:
        # Accumulate totals per processed file
        total_dosars += len(dosars)
        total_files += 1
        if not printed:
            # Печатаем только хвост с реальным количеством красным цветом
            print(f"{'found '}{CRED}{str(len(dosars)).zfill(4)}{CEND}{' dosars'}")


def main():
    # Main processing loop
    global connection
    for filename in os.listdir(Ordins):
        if filename.endswith(('.pdf', '.PDF')):
            parse_pdf(os.path.join(Ordins, filename))

    connection.close()
    logger.info("Processing complete.")
    logger.info(f"Processed {total_dosars} dosars in {total_files} files.")

    # Фиксируем время окончания выполнения
    end_time = time.time()
    # Вычисляем время выполнения
    execution_time = end_time - start_time
    print(f"{'Processed '}{COK}{total_dosars}{CEND}{' dosars in '}{COK}{total_files}{CEND}{' files.'}")
    print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")


if __name__ == '__main__':
    main()
