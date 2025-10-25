#!/bin/bash

#openssl s_client -connect cetatenie.just.ro:443 -showcerts </dev/null 2>/dev/null > all_cert.txt
#cat all_cert.txt | openssl x509 -outform PEM > cetatenie-just-ro.pem

#openssl s_client -connect cetatenie.just.ro:443 -showcerts </dev/null 2>/dev/null| openssl x509 -outform PEM > cetatenie-just-ro.pem

# Извлечь все сертификаты из цепочки
#openssl s_client -connect cetatenie.just.ro:443 -showcerts </dev/null 2>/dev/null | \
#sed -n '/-----BEGIN CERTIFICATE-----/,/-----END CERTIFICATE-----/p' > letsencrypt-chain.pem

# Скачать ISRG Root X1 (корневой сертификат Let's Encrypt)
curl -o cetatenie-just-ro_chain.pem https://letsencrypt.org/certs/isrgrootx1.pem
