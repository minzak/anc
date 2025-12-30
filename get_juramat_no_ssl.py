#!/usr/bin/env python3

import os
import re
import time
import pycurl
import requests
import fitz  # pip install PyMuPDF
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import hashlib
import urllib3
#import random

import sys
sys.dont_write_bytecode = True

# Отключение предупреждений о небезопасных SSL соединениях
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
Headers = 'Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0'
REFERER = 'https://cetatenie.just.ro/juramant/'

# Path to certificate (adjust if needed)
CERTIFICATE_PATH = './crt/cetatenie-just-ro_chain.pem'

# Environment cookie override if needed
ENV_COOKIE = os.environ.get('COOKIE', '').strip()

def clear_buffer(buffer):
    buffer.seek(0)
    buffer.truncate(0)

def is_valid_pdf(filepath):
    try:
        with fitz.open(filepath) as doc:
            return True
    except Exception:
        return False

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
    'User-Agent': Headers,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1'
})

try:
    # Отключаем проверку SSL сертификатов
    r_lp = session.get(OrdineUrl, timeout=30, allow_redirects=True, verify=False)
    if r_lp.status_code in (200, 403, 503) and 'Verifying your browser' in r_lp.text:
        val = _solve_res_cookie(r_lp.text)
        if val:
            session.cookies.set('res', val, domain=urlparse(OrdineUrl).hostname)
            r_lp = session.get(OrdineUrl, timeout=30, allow_redirects=True, verify=False)
    html_content = r_lp.text
except Exception:
    buffer = BytesIO()
    r = pycurl.Curl()
    r.setopt(pycurl.URL, OrdineUrl)
    r.setopt(pycurl.USERAGENT, Headers)
    # Отключаем проверку SSL для pycurl
    r.setopt(pycurl.SSL_VERIFYPEER, 0)
    r.setopt(pycurl.SSL_VERIFYHOST, 0)
    clear_buffer(buffer)
    r.setopt(pycurl.WRITEDATA, buffer)
    r.perform()
    r.close()
    html_content = buffer.getvalue().decode('utf-8', errors='ignore')
    if 'Verifying your browser' in html_content:
        val = _solve_res_cookie(html_content)
        if val:
            session.cookies.set('res', val, domain=urlparse(OrdineUrl).hostname)
            r_lp = session.get(OrdineUrl, timeout=30, allow_redirects=True, verify=False)
            if r_lp.status_code == 200:
                html_content = r_lp.text

soup = BeautifulSoup(html_content, 'html.parser')

# Collect PDF links
link_hrefs = []
for a in soup.find_all('a'):
    href = a.get('href')
    if not href:
        continue
    abs_url = urljoin(OrdineUrl, href)
    parsed = urlparse(abs_url)
    path_lower = parsed.path.lower()
    if path_lower.endswith('.pdf') or ('/storage/' in path_lower and '.pdf' in path_lower):
        link_hrefs.append(abs_url)

# Regex fallback for PDFs in HTML
regex_links = re.findall(r'https?://[^\s\"\']+?\.pdf', html_content, flags=re.IGNORECASE)
for u in regex_links:
    if u not in link_hrefs:
        link_hrefs.append(u)

# Dedupe
seen = set()
unique_hrefs = [u for u in link_hrefs if u not in seen and not seen.add(u)]

links = []
for href in unique_hrefs:
    rel = urlparse(href).path.replace('/storage/', '').lstrip('/')
    dest = Ordins + rel.replace('/', '-')
    links.append((href, dest))

print(f"Discovered {COK}{len(links)}{CEND} PDF links on juramant page.")

new_files = []
missing_files = []

print("Getting Non exist files...")

def _cookie_header_from_session(sess: requests.Session) -> str:
    if sess is None:
        return ''
    try:
        return '; '.join([f"{c.name}={c.value}" for c in sess.cookies])
    except Exception:
        return ''

for OrdineUrl, FileName in links:
    print(f"{OrdineUrl} {CVIOLET}-> {CWARN}{FileName}{CEND}".ljust(186, '.'), end="")
    if os.path.isfile(FileName):
        print(f"{CVIOLET}Skipping{CEND}")
        continue
    status_code = None
    content_bytes = None

    #time.sleep(random.uniform(0.7, 1.8))  # Random delay to avoid bans

    # Try with requests
    try:
        headers = {
            'User-Agent': Headers,
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
        # Отключаем проверку SSL сертификатов
        resp = session.get(OrdineUrl, headers=headers, timeout=60, allow_redirects=True, verify=False)
        status_code = resp.status_code
        if status_code == 200:
            content_bytes = resp.content
        elif 'Verifying your browser' in resp.text:
            val = _solve_res_cookie(resp.text)
            if val:
                session.cookies.set('res', val, domain=urlparse(OrdineUrl).hostname)
                resp = session.get(OrdineUrl, headers=headers, timeout=60, allow_redirects=True, verify=False)
                status_code = resp.status_code
                if status_code == 200:
                    content_bytes = resp.content
    except Exception:
        pass

    # Fallback to pycurl
    if status_code != 200:
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, OrdineUrl)
        curl.setopt(pycurl.USERAGENT, Headers)
        curl.setopt(pycurl.REFERER, REFERER)
        # Отключаем проверку SSL для pycurl
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        curl.setopt(pycurl.SSL_VERIFYHOST, 0)
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
        os.makedirs(os.path.dirname(FileName), exist_ok=True)
        with open(FileName, 'wb') as file_handle:
            file_handle.write(content_bytes)
        if is_valid_pdf(FileName):
            new_files.append(FileName)
            print(f"{COK}{str(status_code)} Success{CEND}")
        else:
            print(f"{CRED}Invalid PDF file: {FileName}{CEND}")
            os.remove(FileName)
    else:
        print(f"{CRED}{str(status_code) if status_code is not None else '000'} Download Error{CEND}")
        missing_files.append(OrdineUrl)

print(f"\nNew files: {COK}{len(new_files)}{CEND}; Missing/failed: {COK}{len(missing_files)}{CEND}")
# Execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {COK}{execution_time:.2f}{CEND} seconds")

quit()
