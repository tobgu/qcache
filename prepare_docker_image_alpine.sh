#!/bin/sh

apk add --update python-dev py-pip wget alpine-sdk

# Build and install modified numpy that can handle larger queries
wget --no-check-certificate https://pypi.python.org/packages/source/n/numpy/numpy-1.10.1.tar.gz
tar xvzf numpy-1.10.1.tar.gz
cd numpy-1.10.1
sed -i '/#define NPY_MAXARGS 32/c\#define NPY_MAXARGS 256' numpy/core/include/numpy/ndarraytypes.h

# Need to apply a small patch for numpy to build with musl
patch -p1 < ../musl-bd611864247f545397823f2b566f1361148bb2fd/dev-python/numpy/files/numpy-1.10.1-musl-fix.patch

# Build, install, remove
python setup.py build install
cd ..
rm -rf numpy*
rm -rf musl-*

# Other pre-reqs
pip install pandas==0.19.2
pip install numexpr==2.6.0
pip install tornado==4.4.2
pip install docopt==0.6.2
pip install lz4==0.8.2
pip install six

# Remove packages and stuff installed in the previous steps. These are not needed to run QCache.
apk del python-dev wget alpine-sdk
apk add --update libstdc++
rm -rf /var/cache/apk/*
rm -rf /usr/lib/python2.7/site-packages/pandas/io/tests
rm -rf /tmp/*
rm -rf /root/.cache/*