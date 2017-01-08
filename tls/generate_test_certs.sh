#!/usr/bin/env bash

# Small script to generate certs for testing of TLS
# Uses cfssl for cert generation: https://github.com/cloudflare/cfssl

cfssl genkey -initca csr.json | cfssljson -bare ca
cfssl gencert -ca ca.pem -ca-key ca-key.pem csr.json -config ca-conf.json | cfssljson -bare host
cat host-key.pem >> host.pem
