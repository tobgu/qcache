#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''QCache

Usage:
  qcache [-hd] [--port=PORT] [--size=MAX_SIZE] [--age=MAX_AGE] [--statistics-buffer-size=BUFFER_SIZE]
         [--cert-file=PATH_TO_CERT] [--basic-auth=<USER>:<PASSWORD>]

Options:
  -h --help                     Show this screen
  -p PORT --port=PORT           Port [default: 8888]
  -s MAX_SIZE --size=MAX_SIZE   Max cache size, bytes [default: 1000000000]
  -a MAX_AGE --age=MAX_AGE      Max age of cached item, seconds. 0 = never expire. [default: 0]
  -b BUFFER_SIZE --statistics-buffer-size=BUFFER_SIZE  Number of entries to store in statistics
                                                       ring buffer. [default: 1000]
  -c PATH_TO_CERT --cert-file=PATH_TO_CERT   Path to PEM file containing private key and certificate for SSL
  -d --debug   Run in debug mode
  -ba <USER>:<PASSWORD> --basic-auth=<USER>:<PASSWORD>   Enable basic auth, requires that SSL is enabled.
'''

from docopt import docopt
from qcache.app import run

__version__ = "0.4.2"
__author__ = "Tobias Gustafsson"
__license__ = "MIT"


def main():
    """
    Main entry point for the qcache server.
    """
    args = docopt(__doc__, version=__version__)

    # Should be possible to solve this without casting to int...
    if '--version' in args:
        print __version__
    else:
        run(port=int(args['--port']),
            max_cache_size=int(args['--size']),
            max_age=int(args['--age']),
            statistics_buffer_size=int(args['--statistics-buffer-size']),
            debug=args['--debug'],
            certfile=args['--cert-file'],
            basic_auth=args['--basic-auth'])

if __name__ == '__main__':
    main()
