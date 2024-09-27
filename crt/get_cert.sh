#!/bin/bash

#openssl s_client -connect cetatenie.just.ro:443 -showcerts </dev/null 2>/dev/null > all_cert.txt
#cat all_cert.txt | openssl x509 -outform PEM > cetatenie-just-ro.pem

openssl s_client -connect cetatenie.just.ro:443 -showcerts </dev/null 2>/dev/null| openssl x509 -outform PEM > cetatenie-just-ro.pem
