#!/bin/bash

curl https://curl.se/ca/cacert.pem -o cacert.pem.crt
sudo update-ca-certificates --fresh
