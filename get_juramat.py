#!/usr/bin/python3

import os
import re
import time
import pycurl
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

Ordins = './juramat/'
OrdineUrl = "https://cetatenie.just.ro/juramant/"
DownloadUrl = 'https://cetatenie.just.ro/storage/'
Headers = 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

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
#links = [(href, Ordins + href.replace(DownloadUrl, '').replace('/', '-')) for href in link_hrefs]
links = [(href, Ordins + href.replace(DownloadUrl, '').replace('/', '-')) for href in link_hrefs if href.endswith(".pdf")]

new_files = []
missing_files = []

print("Getting Non exist files...")

for OrdineUrl, FileName in links:
    if os.path.isfile(FileName):
        # Вывод всех существуюющих файлов
        #print(f"{OrdineUrl} {CVIOLET}-> {CWARN}{FileName}{CEND}".ljust(186, '.') + f"{CVIOLET}Skipping{CEND}")
        continue
    print(f"{OrdineUrl} {CVIOLET}-> {CWARN}{FileName}{CEND}".ljust(186, '.'), end="")
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
            print(f"{COK}{str(status_code)} Success{CEND}")
        else:
            print(f"{CRED}Invalid PDF file: {FileName}{CEND}")
            os.remove(FileName)
    else:
        print(f"{CRED}{str(status_code)} Download Error{CEND}")
        missing_files.append(OrdineUrl)
    curl.close()

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {COK}{execution_time:.2f}{CEND} seconds")

quit()
