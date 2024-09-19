#!/usr/bin/python3

import os
import requests
import re
from bs4 import BeautifulSoup

CRED    = '\033[91m'
COK     = '\033[92m'
CWARN   = '\033[93m'
CVIOLET = '\033[95m'
CEND    = '\033[0m'

Ordins = './ordins/'
OrdineUrl = "https://cetatenie.just.ro/ordine-articolul-1-1/"
DownloadUrl = 'https://cetatenie.just.ro/storage/'
Headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0'}
r = requests.get(OrdineUrl, headers=Headers)

# Проходимся по всем ссылкам со страницы с приказами по 11 артикулу OrdineUrl
# Выкачиваем все ссылки, которые содержат в адресе DownloadUrl
soup = BeautifulSoup(r.text, 'html.parser')
for link in soup.find_all('a', href=re.compile(DownloadUrl)):
    OrdineUrl = link.get('href')
    FileName = Ordins+OrdineUrl.replace(DownloadUrl, '').replace('/', '-')
    if not os.path.isfile(FileName):
        r = requests.get(OrdineUrl, headers=Headers)
        print(f"{OrdineUrl + CVIOLET + ' -> ' + CWARN + FileName + CEND:.<190}", end ="")
        if r.status_code == 200:
            with open(FileName, 'wb') as file_handle:
                file_handle.write(r.content)
            print(f"{COK + 'Success' + CEND}")
        else:
            print(f"{CRED + 'Download Error' + CEND}")
    else:
        pass
quit()
