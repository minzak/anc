#!/usr/bin/python3

import os
import re
import time
import sqlite3
import logging
import fitz  # pip install PyMuPDF
from pdfminer.high_level import extract_text
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
SQLlogger = setup_logger('SQLlogger', 'sql-ordins-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')

# Database setup
Database = './data.db'
connection = sqlite3.connect(Database)
#connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()

# Constants
Ordins = './ordins/'


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
        date_match = re.search(r'(\d{1,2})\s*[.\s]?\s*(\d{1,2})\s*[.\s]?\s*(20\d{2})', date_search_area)
        if date_match:
            day, month, year = date_match.groups()
            day = day.strip().zfill(2)
            month = month.strip().zfill(2)
            year = year.strip()
            try:
                #return datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
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
            date_match = re.search(r'(\d{1,2})\s*[.\s]?\s*(\d{1,2})\s*[.\s]?\s*(20\d{2})', date_search_area)
            if date_match:
                day, month, year = date_match.groups()
                day = day.strip().zfill(2)
                month = month.strip().zfill(2)
                year = year.strip()
                try:
                    #return datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                    return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
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
    print(f"{CERR + 'PyMuPDF failed, falling back to PDFMiner.' + COK}")
    date = date_pdfminer(file_path)
    #return date.strftime('%d.%m.%Y') if date else None
    return date  # Return as datetime object if found, otherwise None


# Parsing function
def parse_pdf(file_path):
    try:
        # Reset variables for each file
        anexa = ""
        dosars = []

        # Extract ordinance number from file name as fallback
        filename = os.path.basename(file_path)
        # Удаляем год-месяц (YYYY-MM-) в начале строки, если он есть
        filename = re.sub(r'^\d{4}-\d{2}-', '', filename)
        # Оставляем только цифры после этого
        file_ordin_number = re.sub(r'^[^\d]*?(\d+).*', r'\1', filename) if re.match(r'^[^\d]*?(\d+)', filename) else None

        ordinance_date = extract_date(file_path)

        with fitz.open(file_path) as doc:
            logger.info(f"Parsing file: {file_path} F:{file_ordin_number} D:{ordinance_date}")

            # Iterate through pages to find ANEXA and dosars
            print(f"{'Parsing: ' + CWARN + file_path + CEND:.<205}", end="")
            for page_num, page_text in enumerate([page.get_text() for page in doc]):
                # Detect ANEXA by multiple formats or keywords
                if re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?1', page_text, re.IGNORECASE):
                    anexa = 1
                elif re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?2', page_text, re.IGNORECASE):
                    anexa = 2
                elif "domiciliului în străinătate" in page_text:
                    anexa = 1
                elif "domiciliului în România" in page_text:
                    anexa = 2

                # Extract dosars and children info for the current page
                page_dosars = re.findall(r'\((\d+)[/][A-Za-z/]*(\d+)\)', page_text)
                for dosar in page_dosars:
                    dosarnum, dosaryear = dosar

                    # Look for the number of children directly after the dosar
                    child_count_match = re.search(rf'\({dosarnum}/{dosaryear}\).*?Copii minori:\s*(\d+)', page_text)
                    child_count = int(child_count_match.group(1)) if child_count_match else 0

                    dosars.append({
                        "id": f"{dosarnum}/RD/{dosaryear}",
                        "anexa": anexa,
                        "cminori": child_count,
                        "year": int(dosaryear)
                    })
                    logger.info(f'Ordin: {file_ordin_number}/P/{ordinance_date.year} Dosar {dosarnum}/RD/{dosaryear} ANEXA{anexa} Minori: {child_count}')

            # Process each dosar and update database
            for dosar in dosars:
                db.execute('''
                    UPDATE Dosar
                    SET solutie = IIF(Dosar.solutie IS NULL, ?, Dosar.solutie),
                        result = ?,
                        ordin = ?,
                        anexa = ?,
                        cminori = ?
                    WHERE id = ?
                ''', (ordinance_date, True, f"{file_ordin_number}/P/{ordinance_date.year}", dosar["anexa"], dosar["cminori"], dosar["id"]))
                #SQLlogger.info('Modified: ' + str(db.rowcount))
                # Log the update in a more compact format
                SQLlogger.info(f"UPDATE: ID:{dosar['id']} | Result:{True} | Ordin_date {ordinance_date} | Ordin:{file_ordin_number}/P/{ordinance_date.year} | ANEXA:{dosar['anexa']} | Minori:{dosar['cminori']} | Modified:{db.rowcount} rows")

            print(f"{'found ' + COK + str(len(dosars)).zfill(4) + CEND + ' dosars'}")
            logger.info(f"Processed {len(dosars)} dosars from {file_path}")
            connection.commit()

    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {e}")

# Main processing loop
for filename in os.listdir(Ordins):
    if filename.endswith(('.pdf', '.PDF')):
        parse_pdf(os.path.join(Ordins, filename))

connection.close()
logger.info("Processing complete.")

# Фиксируем время окончания выполнения
end_time = time.time()
# Вычисляем время выполнения
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
