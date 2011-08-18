###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2011, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################
import logging
import sys

def initLogging(options):
    if options.quiet:
       level = logging.ERROR
    elif options.debug:
       level = logging.DEBUG
    else:
       level = logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-6s: %(message)s',
        stream=sys.stderr
    )
    logging.getLogger('').setLevel(level)

def addLoggingOptions(parser):
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                    help="No logging messages will be displayed", default=False)
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                    help="Verbose logging", default=False)
    return parser