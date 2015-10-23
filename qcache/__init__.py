#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''QCache

Usage:
  qcache  [-h] [--port=PORT] [--size=MAX_SIZE]

Options:
  -h --help                     Show this screen
  -p PORT --port=PORT           Port [default: 8888]
  -s MAX_SIZE --size=MAX_SIZE   Max size [default: 1000000000]
'''

from docopt import docopt
from qcache.app import run

__version__ = "0.0.1"
__author__ = "Tobias Gustafsson"
__license__ = "MIT"


def main():
    """
    Main entry point for the qcache server.
    """
    args = docopt(__doc__, version=__version__)
    run(port=args['--port'], max_cache_size=args['--size'])

if __name__ == '__main__':
    main()
