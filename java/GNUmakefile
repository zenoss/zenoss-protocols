#============================================================================
#
# Copyright (C) Zenoss, Inc. 2023 all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
#============================================================================

default: build

build:
	@docker run --rm \
		-v ${PWD}/..:/mnt/build \
		-v ${HOME}/.m2:/home/build/.m2 \
		-w /mnt/build \
		zenoss/maven:java21-2 \
		-f java/pom.xml -Dprotoc.path=/usr/bin/protoc clean install

deploy:
	@docker run --rm \
			-v ${PWD}/..:/mnt/build \
			-v ${HOME}/.m2:/home/build/.m2 \
			-w /mnt/build \
			zenoss/maven:java21-2 \
			-f java/pom.xml -Dprotoc.path=/usr/bin/protoc -Drepo.login=${NEXUS_USER} -Drepo.pwd=${NEXUS_PASSWORD} clean deploy

clean:
	@rm -rf amqp-api/target \
	        amqp-samples/target \
	        protobufs/target