#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""QCache

Usage:
  qcache [-hd] [--port=PORT] [--size=MAX_SIZE] [--age=MAX_AGE] [--statistics-buffer-size=BUFFER_SIZE]
         [--cert-file=PATH_TO_CERT] [--ca-file=PATH_TO_CA] [--basic-auth=<USER>:<PASSWORD>]
         [--api-workers=API_WORKERS] [--cache-shards=CACHE_SHARDS]
         [--l2-cache-size=L2_CACHE_SIZE]

Options:
  -h --help                     Show this screen
  -p PORT --port=PORT           Port [default: 8888]
  -s MAX_SIZE --size=MAX_SIZE   Max cache size, bytes [default: 1000000000]
  -a MAX_AGE --age=MAX_AGE      Max age of cached item, seconds. 0 = never expire. [default: 0]
  -b BUFFER_SIZE --statistics-buffer-size=BUFFER_SIZE  Number of entries to store in statistics
                                                       ring buffer. [default: 1000]
  -c PATH_TO_CERT --cert-file=PATH_TO_CERT   Path to PEM file containing private key and certificate for SSL
  -ca PATH_TO_CA --ca-file=PATH_TO_CA   Path to CA file, if provided client certificates will be checked against this ca
  -d --debug   Run in debug mode
  -ba <USER>:<PASSWORD> --basic-auth=<USER>:<PASSWORD>   Enable basic auth, requires that SSL is enabled.
  --api-workers=API_WORKERS   Number of processes that handle API calls, serialization/deserialization of
                              data etc. [default: 1]
  --cache-shards=CACHE_SHARDS   Number of cache shards [default: 1]
  --l2-cache-size=L2_CACHE_SIZE  Layer 2 cache size [default: 0]
"""

from docopt import docopt
from qcache.app import run

__version__ = "0.9.0rc1-py3"
__author__ = "Tobias Gustafsson"
__license__ = "MIT"


def main():
    """
    Main entry point for the qcache server.
    """
    args = docopt(__doc__, version=__version__)

    # Should be possible to solve this without casting to int...
    if '--version' in args:
        print(__version__)
    else:
        run(port=int(args['--port']),
            max_cache_size=int(args['--size']),
            max_age=int(args['--age']),
            statistics_buffer_size=int(args['--statistics-buffer-size']),
            debug=args['--debug'],
            certfile=args['--cert-file'],
            cafile=args['--ca-file'],
            basic_auth=args['--basic-auth'],
            api_workers=int(args['--api-workers']),
            cache_shards=int(args['--cache-shards']),
            l2_cache_size=int(args['--l2-cache-size']))

if __name__ == '__main__':
    main()
