###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published by
# the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

PROTODIR = protobufs
PROTOC = protoc
default: build

build:
	mkdir -p ${PROTODIR} && touch ${PROTODIR}/__init__.py
	$(PROTOC) -I . --python_out=${PROTODIR} ./*.proto

clean:
	rm -rf ${PROTODIR}
	rm -rf build
	rm -rf dist