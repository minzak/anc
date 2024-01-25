#!/usr/bin/python3


import os
import requests
import re
from bs4 import BeautifulSoup

Ordins = './ordins/'

OrdineUrl = "https://cetatenie.just.ro/ordine-articolul-1-1/"
Headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'}
r = requests.get(OrdineUrl, headers=Headers)

# Проходимся по всем ссылкам со страницы с приказами по 11 артикулу https://cetatenie.just.ro/ordine-articolul-1-1/
# Выкачиваем все ссылки, которые содержат в адресе cetatenie.just.ro/wp-content/uploads
soup = BeautifulSoup(r.text, 'html.parser')
for link in soup.find_all('a', href=re.compile('cetatenie.just.ro/wp-content/uploads')):
    OrdineUrl = link.get('href')
    FileName = Ordins+OrdineUrl.replace('https://cetatenie.just.ro/wp-content/uploads/', '').replace('/', '-')
    if not os.path.isfile(FileName):
        r = requests.get(OrdineUrl, headers=Headers)
        if r.status_code == 200:
            with open(FileName, 'wb') as file_handle:
                file_handle.write(r.content)
            print(OrdineUrl, '=>', FileName + ' ... ', end="")
            print('Success')
        else:
            print(OrdineUrl, '=>', FileName + ' ... ', end="")
            print('Download Error')
    else:
        pass
quit()
