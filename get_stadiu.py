#!/usr/bin/python3

import os
import re
import time
import hashlib
import pycurl
import requests
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
import shutil
from urllib.parse import urljoin, urlparse
import random

# Константы
BASE_URL = "https://cetatenie.just.ro/stadiu-dosar/"
HEADERS = 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
REFERER = 'https://cetatenie.just.ro/stadiu-dosar/'
BASE_DIR = "./stadiu/"
TEMP_DIR = "./temp/"

# Path to certificate (adjust if needed)
CERTIFICATE_PATH = './crt/cetatenie-just-ro-chain.pem'

# Environment cookie override if needed
ENV_COOKIE = os.environ.get('COOKIE', '').strip()

# Цвета для вывода
C_SUCCESS = '\033[92m'  # Зеленый
C_ERROR = '\033[91m'  # Красный
C_INFO = '\033[93m'  # Желтый
C_VIOLET = '\033[95m'  # Фиолетовый
C_RESET = '\033[0m'  # Сброс цвета

# Фиксируем время начала выполнения
start_time = time.time()

# Функции
def get_file_hash(filepath):
    """Вычислить хэш файла для проверки изменений"""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def extract_year_from_filename(filename):
    """Извлечь год из имени файла"""
    match = re.search(r'(20\d{2})', filename)
    return match.group(1) if match else None

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

def _solve_res_cookie(html_text: str):
    try:
        m = re.search(r"['\"]([0-9A-F]{40})['\"]", html_text, flags=re.IGNORECASE)
        if not m:
            return None
        c_hex = m.group(1).upper()
        n1 = int(c_hex[0], 16)
        for i in range(0, 500000):
            dg = hashlib.sha1((c_hex + str(i)).encode('utf-8')).digest()
            if n1 + 1 < len(dg) and dg[n1] == 0xB0 and dg[n1 + 1] == 0x0B:
                return f"{c_hex}{i}"
        return None
    except Exception:
        return None

# Fetch HTML with challenge handling
html_content = ''
session = requests.Session()
session.headers.update({
    'User-Agent': HEADERS,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1'
})
try:
    r_lp = session.get(BASE_URL, timeout=30, allow_redirects=True, verify=CERTIFICATE_PATH)
    if r_lp.status_code in (200, 403, 503) and 'Verifying your browser' in r_lp.text:
        val = _solve_res_cookie(r_lp.text)
        if val:
            session.cookies.set('res', val, domain=urlparse(BASE_URL).hostname)
            r_lp = session.get(BASE_URL, timeout=30, allow_redirects=True, verify=CERTIFICATE_PATH)
    html_content = r_lp.text
except Exception:
    buffer = BytesIO()
    r = pycurl.Curl()
    r.setopt(pycurl.URL, BASE_URL)
    r.setopt(pycurl.USERAGENT, HEADERS)
    clear_buffer(buffer)
    r.setopt(pycurl.WRITEDATA, buffer)
    r.perform()
    r.close()
    html_content = buffer.getvalue().decode('utf-8', errors='ignore')
    if 'Verifying your browser' in html_content:
        val = _solve_res_cookie(html_content)
        if val:
            session.cookies.set('res', val, domain=urlparse(BASE_URL).hostname)
            r_lp = session.get(BASE_URL, timeout=30, allow_redirects=True, verify=CERTIFICATE_PATH)
            if r_lp.status_code == 200:
                html_content = r_lp.text

if not html_content:
    print(f"{C_ERROR}Ошибка при загрузке страницы {BASE_URL}{C_RESET}")
    exit()

soup = BeautifulSoup(html_content, 'html.parser')

# Find the section for ARTICOLUL 11
articolul_11_section = soup.find("div", id="articolul-11")
if not articolul_11_section:
    # Fallback to first eael-tab-content-item
    sections = soup.find_all("div", class_="eael-tab-content-item")
    if sections:
        articolul_11_section = sections[0]  # Assuming first is ARTICOLUL 11
if not articolul_11_section:
    print(f"{C_ERROR}Не найдена секция ARTICOLUL 11.{C_RESET}")
    exit()

# Extract PDF links
links = [
    urljoin(BASE_URL, link.get('href')) for link in articolul_11_section.find_all('a', href=True)
    if link.get('href').endswith((".pdf", ".xlsx"))
]

# Сканирование всех существующих файлов
existing_files_by_year = {}
existing_hashes = {}
for folder in sorted(os.listdir(BASE_DIR)):
    folder_path = os.path.join(BASE_DIR, folder)
    if os.path.isdir(folder_path):
        print(f"{C_INFO}Scanning folder: {folder}{C_RESET}")
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            file_year = extract_year_from_filename(file)
            if file_year:
                file_hash = get_file_hash(file_path)
                # Store all files for the year in a list
                if file_year not in existing_files_by_year:
                    existing_files_by_year[file_year] = []
                existing_files_by_year[file_year].append(file_path)
                existing_hashes[file_path] = file_hash
                print(f"File: {file}, Year: {C_SUCCESS}{file_year}{C_RESET}, HASH: {C_INFO}{file_hash}{C_RESET}")
print(f"{C_SUCCESS}Scanning directories complete.{C_RESET}")

os.makedirs(TEMP_DIR, exist_ok=True)

def _cookie_header_from_session(sess: requests.Session) -> str:
    if sess is None:
        return ''
    try:
        return '; '.join([f"{c.name}={c.value}" for c in sess.cookies])
    except Exception:
        return ''

for link in links:
    filename = os.path.basename(link)
    if not filename:
        print(f"{C_INFO}Пропущен некорректный URL: {link}{C_RESET}")
        continue

    file_year = extract_year_from_filename(filename)
    if not file_year:
        print(f"{C_INFO}Пропущен файл без указания года: {filename}{C_RESET}")
        continue

    temp_filepath = os.path.join(TEMP_DIR, filename)
    print(f"{link} {C_VIOLET}-> {C_INFO}{temp_filepath}{C_RESET}".ljust(160, '.'), end="")

    status_code = None
    content_bytes = None

    #time.sleep(random.uniform(0.7, 1.8))  # Random delay

    # Try with requests
    try:
        headers = {
            'User-Agent': HEADERS,
            'Referer': REFERER,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Priority': 'u=0, i'
        }
        if ENV_COOKIE:
            headers['Cookie'] = ENV_COOKIE
        resp = session.get(link, headers=headers, timeout=60, allow_redirects=True, verify=CERTIFICATE_PATH)
        status_code = resp.status_code
        if status_code == 200:
            content_bytes = resp.content
        elif 'Verifying your browser' in resp.text:
            val = _solve_res_cookie(resp.text)
            if val:
                session.cookies.set('res', val, domain=urlparse(link).hostname)
                resp = session.get(link, headers=headers, timeout=60, allow_redirects=True, verify=CERTIFICATE_PATH)
                status_code = resp.status_code
                if status_code == 200:
                    content_bytes = resp.content
    except Exception:
        pass

    # Fallback to pycurl
    if status_code != 200:
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, link)
        curl.setopt(pycurl.USERAGENT, HEADERS)
        curl.setopt(pycurl.REFERER, REFERER)
        cookie_header = _cookie_header_from_session(session)
        if cookie_header:
            curl.setopt(pycurl.COOKIE, cookie_header)
        if ENV_COOKIE:
            curl.setopt(pycurl.COOKIE, ENV_COOKIE)
        curl.setopt(pycurl.HTTPHEADER, [
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language: en-US,en;q=0.5',
            'Connection: keep-alive',
            'DNT: 1',
            'Upgrade-Insecure-Requests: 1',
            'Sec-Fetch-Dest: document',
            'Sec-Fetch-Mode: navigate',
            'Sec-Fetch-Site: same-origin',
            'Sec-Fetch-User: ?1',
            'Priority: u=0, i'
        ])
        curl.setopt(pycurl.ACCEPT_ENCODING, 'gzip, deflate, br, zstd')
        buffer = BytesIO()
        clear_buffer(buffer)
        curl.setopt(pycurl.WRITEDATA, buffer)
        curl.perform()
        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        if status_code == 200:
            content_bytes = buffer.getvalue()
        curl.close()

    if status_code == 200 and content_bytes is not None:
            with open(temp_filepath, 'wb') as f:
                f.write(content_bytes)
            temp_hash = get_file_hash(temp_filepath)
            if file_year in existing_files_by_year:
                # Check hash against all files for the year
                for old_filepath in existing_files_by_year[file_year]:
                    old_hash = existing_hashes.get(old_filepath)
                    if temp_hash == old_hash:
                        print(f"{C_RESET}Skipping file for {file_year} already present with same HASH.{C_RESET}")
                        os.remove(temp_filepath)
                        break
                else:
                    # No matching hash found, proceed to save the file
                    today = datetime.now().strftime("%Y-%m-%d")
                    new_folder = os.path.join(BASE_DIR, today)
                    os.makedirs(new_folder, exist_ok=True)
                    final_filepath = os.path.join(new_folder, filename)
                    shutil.move(temp_filepath, final_filepath)
                    print(f"{C_SUCCESS}{status_code} Success{C_RESET}")
            else:
                # No existing files for this year, save the file
                today = datetime.now().strftime("%Y-%m-%d")
                new_folder = os.path.join(BASE_DIR, today)
                os.makedirs(new_folder, exist_ok=True)
                final_filepath = os.path.join(new_folder, filename)
                shutil.move(temp_filepath, final_filepath)
                print(f"{C_SUCCESS}{status_code} Success{C_RESET}")
    elif status_code == 404:
        print(f"{C_ERROR}{status_code} Download Error{C_RESET}")
    else:
        print(f"{C_ERROR}Ошибка при скачивании файла: {filename}{C_RESET}")

# Удаляем временную папку
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Execution time: '}{C_SUCCESS}{execution_time:.2f}{C_RESET} seconds")
quit()
