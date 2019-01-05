#!/usr/bin/env bash

# Small script to generate certs for testing of TLS
# Uses cfssl for cert generation: https://github.com/cloudflare/cfssl
# go get -u github.com/cloudflare/cfssl/cmd/cfssl
# go get -u github.com/cloudflare/cfssl/cmd/cfssljson

cfssl genkey -initca csr.json | cfssljson -bare ca
cfssl gencert -ca ca.pem -ca-key ca-key.pem -config ca-conf.json csr.json | cfssljson -bare host
cat host-key.pem >> host.pem
