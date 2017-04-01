#!/bin/sh

apk add --update wget alpine-sdk git
apk add python3-dev py3-pip --update-cache --repository http://nl.alpinelinux.org/alpine/edge/main

# Build and install modified numpy that can handle larger queries
wget --no-check-certificate https://pypi.python.org/packages/b7/9d/8209e555ea5eb8209855b6c9e60ea80119dab5eff5564330b35aa5dc4b2c/numpy-1.12.0.zip
unzip numpy-1.12.0.zip
cd numpy-1.12.0
sed -i '/#define NPY_MAXARGS 32/c\#define NPY_MAXARGS 256' numpy/core/include/numpy/ndarraytypes.h

# For the below line, see:
# http://serverfault.com/questions/771211/docker-alpine-and-matplotlib
# https://github.com/docker-library/python/issues/112
# https://wired-world.com/?p=100
ln -s /usr/include/locale.h /usr/include/xlocale.h

# Build, install, remove
python3 setup.py build install
cd ..
rm -rf numpy*

# Dev requirement
git clone https://github.com/pandas-dev/pandas/
pip3 install Cython==0.25.2
cd pandas

# Use specific commit that we know works reasonably well
git reset --hard 1b53d8864af0ed936f84d0935e2cc360dc9f8de7
python3 setup.py install

# Remove repo
cd ..
rm -rf pandas

# Other pre-reqs
pip3 install numexpr==2.6.2
pip3 install tornado==4.4.2
pip3 install docopt==0.6.2
pip3 install lz4==0.8.2
pip3 install blosc==1.5.0
pip3 install pyzmq==16.0.2
pip3 install setproctitle==1.1.10
pip3 install six

# Remove packages and stuff installed in the previous steps. These are not needed to run QCache.
apk del python-dev wget alpine-sdk git
apk add --update libstdc++
rm -rf /var/cache/apk/*
rm -rf /usr/lib/python3.5/site-packages/pandas/io/tests
rm -rf /tmp/*
rm -rf /root/.cache/*