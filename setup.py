# -*- coding: utf-8 -*-
import re
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand


REQUIRES = [
    'docopt==0.6.2',
    'numexpr==2.6.2',
    'numpy==1.12.0',
    'pandas>=0.19.0',
    'tornado==4.4.2',
    'lz4==0.8.2',
    'blosc==1.5.0',
    'pyzmq==16.0.2',
    'setproctitle==1.1.10',
]


def find_version(fname):
    '''Attempts to find the version number in the file names fname.
    Raises RuntimeError if not found.
    '''
    version = ''
    with open(fname, 'r') as fp:
        reg = re.compile(r'__version__ = [\'"]([^\'"]*)[\'"]')
        for line in fp:
            m = reg.match(line)
            if m:
                version = m.group(1)
                break
    if not version:
        raise RuntimeError('Cannot find version information')
    return version

__version__ = find_version("qcache/__init__.py")


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content

setup(
    name='qcache',
    version=__version__,
    description='In memory cache server with analytical query capabilities',
    long_description=read("README.rst"),
    author='Tobias Gustafsson',
    author_email='tobias.l.gustafsson@gmail.com',
    url='https://github.com/tobgu/qcache',
    install_requires=REQUIRES,
    license="MIT",
    zip_safe=False,
    keywords='qcache',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=["qcache", "qcache.qframe", "qcache.cache"],
    entry_points={
        'console_scripts': [
            "qcache = qcache:main"
        ]
    }
)
