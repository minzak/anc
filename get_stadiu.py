#!/usr/bin/python3

import os
import re
import time
import hashlib
import pycurl
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
import shutil

# Константы
BASE_URL = "https://cetatenie.just.ro/stadiu-dosar/"
HEADERS = "Mozilla/5.0"
BASE_DIR = "./stadiu/"
TEMP_DIR = "./temp/"

# Цвета для вывода
C_SUCCESS = '\033[92m'  # Зеленый
C_ERROR = '\033[91m'  # Красный
C_INFO = '\033[93m'  # Желтый
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

def download_file_with_pycurl(url, dest):
    """Скачать файл с помощью pycurl"""
    buffer = BytesIO()
    curl = pycurl.Curl()
    curl.setopt(pycurl.URL, url)
    curl.setopt(pycurl.USERAGENT, HEADERS)
    curl.setopt(pycurl.WRITEDATA, buffer)
    try:
        curl.perform()
        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        curl.close()

        if status_code == 200:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, 'wb') as f:
                f.write(buffer.getvalue())
            return status_code
        else:
            return status_code
    except pycurl.error as e:
        print(f"{C_ERROR}pycurl error: {e}{C_RESET}")
        return None

# Сканирование всех существующих файлов
existing_files_by_year = {}
existing_hashes = {}
for folder in sorted(os.listdir(BASE_DIR)):
    folder_path = os.path.join(BASE_DIR, folder)
    if os.path.isdir(folder_path):
        print(f"{C_INFO}Scaning folder: {folder}{C_RESET}")
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            file_year = extract_year_from_filename(file)
            if file_year:
                file_hash = get_file_hash(file_path)
                existing_files_by_year[file_year] = file_path
                existing_hashes[file_path] = file_hash
                print(f"File: {file}, Year: {C_SUCCESS}{file_year}{C_RESET}, HASH: {C_INFO}{file_hash}{C_RESET}")
print(f"{C_SUCCESS}Scaning directories complete.{C_RESET}")

# Скачивание новых файлов
buffer = BytesIO()
curl = pycurl.Curl()
curl.setopt(pycurl.URL, BASE_URL)
curl.setopt(pycurl.USERAGENT, HEADERS)
curl.setopt(pycurl.WRITEDATA, buffer)
try:
    curl.perform()
    curl.close()
    html_content = buffer.getvalue().decode('utf-8')
except pycurl.error as e:
    print(f"{C_ERROR}Ошибка при загрузке страницы {BASE_URL}: {e}{C_RESET}")
    exit()

soup = BeautifulSoup(html_content, 'html.parser')
articolul_11_section = soup.find("div", id="articolul-11-tab")
if not articolul_11_section:
    print(f"{C_ERROR}Не найдена секция ARTICOLUL 11.{C_RESET}")
    exit()

#Вместо поиска по фиксированному id, можно найти все контейнеры с классом "eael-tab-content-item" и затем отфильтровать ссылки, заканчивающиеся на ".pdf".
#pdf_links = []
#for section in soup.find_all("div", class_="eael-tab-content-item"):
#    pdf_links.extend([
#        link.get('href')
#        for link in section.find_all('a', href=True)
#        if link.get('href').endswith(".pdf")
#    ])

links = [
    link.get('href') for link in articolul_11_section.find_all('a', href=True)
    if link.get('href').endswith(".pdf")
]

os.makedirs(TEMP_DIR, exist_ok=True)

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
    status_code = download_file_with_pycurl(link, temp_filepath)

    if status_code == 200:
        temp_hash = get_file_hash(temp_filepath)
        print(f"{link} -> {C_INFO}{temp_filepath}{C_RESET}".ljust(160, '.'), end="")
        if file_year in existing_files_by_year:
            old_filepath = existing_files_by_year[file_year]
            old_hash = existing_hashes.get(old_filepath)
            if temp_hash == old_hash:
                print(f"{C_RESET}Skipping file for {file_year} already present with same HASH.{C_RESET}")
                os.remove(temp_filepath)
                continue

        # Перемещаем файл в целевую папку
        today = datetime.now().strftime("%Y-%m-%d")
        new_folder = os.path.join(BASE_DIR, today)
        os.makedirs(new_folder, exist_ok=True)
        final_filepath = os.path.join(new_folder, filename)
        shutil.move(temp_filepath, final_filepath)
        print(f"{C_SUCCESS}200 Success{C_RESET}")
    elif status_code == 404:
        print(f"{C_ERROR}404 Download Error{C_RESET}")
    else:
        print(f"{C_ERROR}Ошибка при скачивании файла: {filename}{C_RESET}")

# Удаляем временную папку
shutil.rmtree(TEMP_DIR)

# Вычисляем время выполнения
end_time = time.time()
execution_time = end_time - start_time
print(f"{'Execution time: '}{C_SUCCESS}{execution_time:.2f}{C_RESET} seconds")
