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
import pkg_resources
import json
from contextlib import closing

log = logging.getLogger(__name__)

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

def get_zenpack_schemas():
    """
    Loads ZenPack QJS schema files from the ZenPacks/<organization>/<name>/protocols/*.qjs files.
    """
    schemas = []
    for zp in pkg_resources.iter_entry_points('zenoss.zenpacks'):
        protocols_dirs = zp.name.split('.')
        protocols_dirs.append('protocols')
        protocols_path = '/'.join(protocols_dirs)
        if zp.dist.resource_isdir(protocols_path):
            qjs_files = [f for f in zp.dist.resource_listdir(protocols_path) if f.endswith('.qjs')]
            for qjs_file in qjs_files:
                qjs_path = protocols_path + '/' + qjs_file
                try:
                    with closing(zp.dist.get_resource_stream(__name__, qjs_path)) as f:
                        schemas.append(json.load(f))
                except Exception as e:
                    log.warn('Failed to load ZenPack schema %s: %s', qjs_file, e)
    return schemas
