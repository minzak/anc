#!/usr/bin/python3

import os
import requests
import re
import pycurl
from bs4 import BeautifulSoup
from io import BytesIO

CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

Ordins = './ordins/'
OrdineUrl = "https://cetatenie.just.ro/ordine-articolul-1-1/"
DownloadUrl = 'https://cetatenie.just.ro/storage/'
Headers = 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'

buffer = BytesIO()

r = pycurl.Curl()
r.setopt(pycurl.URL, OrdineUrl)
r.setopt(pycurl.USERAGENT, Headers)
#r.setopt(pycurl.VERBOSE, 1)
r.setopt(r.WRITEDATA, buffer)
r.perform()
r.close()

# Проходимся по всем ссылкам со страницы с приказами по 11 артикулу OrdineUrl
# Выкачиваем все ссылки, которые содержат в адресе DownloadUrl
soup = BeautifulSoup(buffer.getvalue().decode('utf-8'), 'html.parser')

buffer.seek(0)
buffer.truncate(0)

for link in soup.find_all('a', href=re.compile(DownloadUrl)):
    OrdineUrl = link.get('href')
    FileName = Ordins+OrdineUrl.replace(DownloadUrl, '').replace('/', '-')
    if not os.path.isfile(FileName):
        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, OrdineUrl)
        curl.setopt(pycurl.USERAGENT, Headers)
        curl.setopt(r.WRITEDATA, buffer)
        curl.perform()
        file = buffer.getvalue()
        print(f"{OrdineUrl + CVIOLET + ' -> ' + CWARN + FileName + CEND:.<190}", end ="")
        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        if status_code == 200:
            with open(FileName, 'wb') as file_handle:
               file_handle.write(file)
               file_handle.flush()
               os.fsync(file_handle.fileno())
               buffer.seek(0)
               buffer.truncate(0)
            print(f"{COK + str(status_code) + ' Success' + CEND}")
        else:
            print(f"{CRED + str(status_code) + ' Download Error' + CEND}")
        curl.close()
    else:
        pass
quit()
