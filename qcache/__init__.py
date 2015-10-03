#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''qcache
Usage:
  qcache -h | --help

Options:
  -h --help     Show this screen.
'''

# '''qcache
#
# Usage:
#   qcache ship new <name>...
#   qcache ship <name> move <x> <y> [--speed=<kn>]
#   qcache ship shoot <x> <y>
#   qcache mine (set|remove) <x> <y> [--moored|--drifting]
#   qcache -h | --help
#   qcache --version
#
# Options:
#   -h --help     Show this screen.
#   --version     Show version.
#   --speed=<kn>  Speed in knots [default: 10].
#   --moored      Moored (anchored) mine.
#   --drifting    Drifting mine.
# '''

from docopt import docopt
from qcache.app import run

__version__ = "0.0.1"
__author__ = "Tobias Gustafsson"
__license__ = "MIT"


def main():
    '''Main entry point for the qcache server.'''
    # args = docopt(__doc__, version=__version__)
    run()

if __name__ == '__main__':
    main()