##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


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
