#!/bin/sh

which certtool >/dev/null 2>&1 || {
  echo "certtool not found, please install GnuTLS."
  exit 1
}

certtool --generate-privkey --outfile key.pem --key-type rsa --sec-param medium
certtool --generate-self-signed --load-privkey key.pem --template $(dirname $0)/certtool.tpl --outfile cert.pem --stdout-info
