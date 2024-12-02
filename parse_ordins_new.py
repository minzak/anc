#!/usr/bin/python3

import os
import re
import time
import pycurl
import sqlite3
import logging
import requests
import fitz  # pip install PyMuPDF
from io import BytesIO
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
Database = './data.db'

# Параметры логирования
LogFormat = logging.Formatter('%(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(LogFormat)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

def is_valid_pdf(filepath):
    try:
        with fitz.open(filepath) as doc:
            return True
    except Exception:
        return False

logger = setup_logger('main_logger', 'parse-ordins.log')
SQLlogger = setup_logger('SQLlogger', 'sql-'+datetime.now().strftime("%Y-%m-%d")+'.log')

# Инициализация базы данных
connection = sqlite3.connect(Database)
connection.set_trace_callback(SQLlogger.info)
db = connection.cursor()

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

# Вывод отсутствующих файлов
#if missing_files:
#    print(f"\n{CRED}The following files could not be downloaded:{CEND}")
#    for missing in missing_files:
#        print(f"{missing}")

# Парсинг новых файлов
logger.info('Start parsing ordins at ' + datetime.now().strftime("%Y-%m-%d %M:%S"))

for filename in new_files:
    if not filename.endswith(('.pdf', '.PDF')):
        continue
    date = ""
    try:
        with fitz.open(filename) as doc:
            text = ""
            dosarcounter = 0
            print(f"{'Parsing: ' + CWARN + filename + CEND:.<205}", end="")
            for page in doc:
                text += page.get_text()
                if date == "":
                    match = re.search(r'\d{1,2}\.\d{1,2}\.20\d{2}', text)
                    if match:
                        date = match.group()
                        date = datetime.strptime(date, '%d.%m.%Y').date()
            dosars = re.findall(r'\((\d+)[\/][A-Za-z/]*(\d+)\)', text)
            logger.info('Ordin date is ' + (date.strftime('%Y-%m-%d') if date else 'unknown'))
            for dosar in dosars:
                dosarnum, dosaryear = dosar
                logger.info(f'Found dosar {dosarnum}/{dosaryear}')
                db.execute('UPDATE Dosar SET solutie = IIF(solutie IS NULL, ?, solutie), result = True WHERE id == ?',
                           (date, f'{dosarnum}/RD/{dosaryear}'))
                SQLlogger.info('Modified: ' + str(db.rowcount))
                dosarcounter += 1
            logger.info(f'In {filename} found {dosarcounter} dosars')
            print(f"{'found ' + COK + str(dosarcounter).zfill(4) + CEND + ' dosars'}")
    except Exception as e:
        logger.error(f"Error parsing file {filename}: {e}")
        print(f"{CRED}Error parsing file {filename}: {e}{CEND}")
    connection.commit()

# Завершаем транзакции
connection.commit()
connection.close()

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Execution time: '}{COK}{execution_time:.2f}{CEND} seconds")

quit()
