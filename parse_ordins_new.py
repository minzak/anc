#!/usr/bin/python3
#!venv/bin/python3

import os
import re
import time
import pycurl
import logging
import sqlite3
import fitz  # pip install PyMuPDF
from io import BytesIO  # Убедимся, что модуль io корректно импортирован
from datetime import datetime
from bs4 import BeautifulSoup


# Фиксируем время начала выполнения
start_time = time.time()

# Константы
CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

Ordins = './ordins/'
OrdineUrl = "https://cetatenie.just.ro/ordine-articolul-1-1/"
DownloadUrl = 'https://cetatenie.just.ro/storage/'
Headers = 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'

# Logging setup
def setup_logger(name, log_file, level=logging.INFO, mode='w'):
    LogFormat = logging.Formatter('%(message)s')
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(LogFormat)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

logger = setup_logger('main_logger', 'parse-ordins-new-' + datetime.now().strftime("%Y-%m-%d") + '.log', mode='w')

SQLlogger = setup_logger('SQLlogger', 'sql-ordins-new-'+datetime.now().strftime("%Y-%m-%d")+'.log', mode='w')

# Database setup
Database = './data.db'
connection = sqlite3.connect(Database)
db = connection.cursor()

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

# Проверка валидности PDF
def is_valid_pdf(filepath):
    try:
        with fitz.open(filepath) as doc:
            return True
    except Exception:
        return False

# Выкачивание новых файлов
buffer = BytesIO()
r = pycurl.Curl()
r.setopt(pycurl.URL, OrdineUrl)
r.setopt(pycurl.USERAGENT, Headers)

# Clear buffer before writing
clear_buffer(buffer)

r.setopt(pycurl.WRITEDATA, buffer)
r.perform()
r.close()
html_content = buffer.getvalue().decode('utf-8')
soup = BeautifulSoup(html_content, 'html.parser')

# Сбор ссылок на файлы для обработки
link_hrefs = [link.get('href') for link in soup.find_all('a', href=re.compile(DownloadUrl))]
links = [(href, Ordins + href.replace(DownloadUrl, '').replace('/', '-')) for href in link_hrefs]

new_files = []
missing_files = []

for OrdineUrl, FileName in links:
    if os.path.isfile(FileName):
        # Вывод всех существуюющих файлов
        #print(f"{OrdineUrl + CVIOLET + ' -> ' + CWARN + FileName + CEND:.<210}{CWARN}Already exists{CEND}")
        continue
    print(f"{OrdineUrl + CVIOLET + ' -> ' + CWARN + FileName + CEND:.<210}", end="")
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, OrdineUrl)
    curl.setopt(pycurl.USERAGENT, Headers)
    buffer = BytesIO()

    # Clear buffer before writing
    clear_buffer(buffer)

    curl.setopt(pycurl.WRITEDATA, buffer)
    curl.perform()
    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    if status_code == 200:
        with open(FileName, 'wb') as file_handle:
            file_handle.write(buffer.getvalue())
        if is_valid_pdf(FileName):
            new_files.append(FileName)
            print(f"{COK + str(status_code) + ' Success' + CEND}")
        else:
            print(f"{CRED}Invalid PDF file: {FileName}{CEND}")
            os.remove(FileName)
    else:
        print(f"{CRED + str(status_code) + ' Download Error' + CEND}")
        missing_files.append(OrdineUrl)
    curl.close()

#Extract ordinance date using PDFMiner.
def date_pdfminer(file_path):
    try:
        full_text = extract_text(file_path)
        keywords = ["ANEX", "LISTA", "1. "]
        longest_match = None
        max_length = 0

        for keyword in keywords:
            ordin_position = full_text.find(keyword)
            if ordin_position != -1:
                substring = full_text[ordin_position:]
                if len(substring) > max_length:
                    max_length = len(substring)
                    longest_match = substring

        date_search_area = longest_match if longest_match else full_text
        date_match = re.search(r'(\d{1,2})\s*[.\s]?\s*(\d{1,2})\s*[.\s]?\s*(20\d{2})', date_search_area)
        if date_match:
            day, month, year = date_match.groups()
            day = day.strip().zfill(2)
            month = month.strip().zfill(2)
            year = year.strip()
            try:
                return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
            except ValueError:
                logger.warning(f"Invalid date format in file {file_path}: {day}.{month}.{year}")
    except Exception as e:
        print(f"Error extracting date with PDFMiner: {e}")
    return None

#Extract ordinance date using PyMuPDF.
def date_pymupdf(file_path):
    try:
        with fitz.open(file_path) as doc:
            first_page_text = doc[0].get_text() if len(doc) > 0 else ""
            ordin_position = first_page_text.find("LISTA")
            date_search_area = first_page_text[:ordin_position] if ordin_position != -1 else first_page_text
            date_match = re.search(r'(\d{1,2})\s*[.\s]?\s*(\d{1,2})\s*[.\s]?\s*(20\d{2})', date_search_area)
            if date_match:
                day, month, year = date_match.groups()
                day = day.strip().zfill(2)
                month = month.strip().zfill(2)
                year = year.strip()
                try:
                    return datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y')
                except ValueError:
                    logger.warning(f"Invalid date format in file {file_path}: {day}.{month}.{year}")
    except Exception as e:
        print(f"Error extracting date with PyMuPDF: {e}")
    return None

#Extract date, preferring PyMuPDF and falling back to PDFMiner if necessary.
def extract_date(file_path):
    date = date_pymupdf(file_path)
    if date:
        return date
    print(f"{CERR + 'PyMuPDF failed, falling back to PDFMiner.' + COK}")
    date = date_pdfminer(file_path)
    return date

# Parsing function
def parse_pdf(file_path):
    try:
        anexa = ""
        dosars = []
        filename = os.path.basename(file_path)
        filename = re.sub(r'^\d{4}-\d{2}-', '', filename)
        file_ordin_number = re.sub(r'^[^\d]*?(\d+).*', r'\1', filename) if re.match(r'^[^\d]*?(\d+)', filename) else None
        ordinance_date = extract_date(file_path)

        with fitz.open(file_path) as doc:
            logger.info(f"Parsing file: {file_path} F:{file_ordin_number} D:{ordinance_date.strftime('%Y-%m-%d')}")

            print(f"{'Parsing: ' + CWARN + file_path + CEND:.<205}", end="")
            for page_num, page_text in enumerate([page.get_text() for page in doc]):
                if re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?1', page_text, re.IGNORECASE):
                    anexa = 1
                elif re.search(r'ANEX(A|\u0102)\s*(NR\.\s*)?2', page_text, re.IGNORECASE):
                    anexa = 2
                elif "domiciliului în străinătate" in page_text:
                    anexa = 1
                elif "domiciliului în România" in page_text:
                    anexa = 2

                page_dosars = re.findall(r'\((\d+)[/][A-Za-z/]*(\d+)\)', page_text)
                for dosar in page_dosars:
                    dosarnum, dosaryear = dosar
                    child_count_match = re.search(rf'\({dosarnum}/{dosaryear}\).*?Copii minori:\s*(\d+)', page_text)
                    child_count = int(child_count_match.group(1)) if child_count_match else 0

                    dosars.append({
                        "id": f"{dosarnum}/RD/{dosaryear}",
                        "anexa": anexa,
                        "cminori": child_count,
                        "year": int(dosaryear)
                    })
                    logger.info(f'Ordin: {file_ordin_number}/P/{ordinance_date.year} Dosar {dosarnum}/RD/{dosaryear} ANEXA{anexa} Minori: {child_count}')

            for dosar in dosars:
                db.execute('''
                    UPDATE Dosar11
                    SET solutie = IIF(Dosar11.solutie IS NULL, ?, Dosar11.solutie),
                        result = ?,
                        ordin = ?,
                        anexa = ?,
                        cminori = ?
                    WHERE id = ?
                ''', (ordinance_date, True, f"{file_ordin_number}/P/{ordinance_date.year}", dosar["anexa"], dosar["cminori"], dosar["id"]))
                SQLlogger.info(f"UPDATE: ID:{dosar['id']} | Result:{True} | Ordin_date {ordinance_date} | Ordin:{file_ordin_number}/P/{ordinance_date.year} | ANEXA:{dosar['anexa']} | Minori:{dosar['cminori']} | Modified:{db.rowcount} rows")

            print(f"{'found ' + COK + str(len(dosars)).zfill(4) + CEND + ' dosars'}")
            logger.info(f"Processed {len(dosars)} dosars from {file_path}")
            connection.commit()

    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {e}")


logger.info('Start parsing ordins at ' + datetime.now().strftime("%Y-%m-%d %M:%S"))

# Парсинг только новых файлов
for filename in new_files:
    parse_pdf(filename)

connection.close()
logger.info("Processing complete.")

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Parsing PDF time: '}{COK}{execution_time:.2f}{CEND} seconds")
