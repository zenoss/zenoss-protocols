/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.protobufs;

/**
 * Constants for dealing with protobufs.
 */
public final class ProtobufConstants {
    private ProtobufConstants() {
    }

    /**
     * MIME Content type (unofficial) for specifying protobuf encoded messages.
     */
    public static final String CONTENT_TYPE_PROTOBUF = "application/x-protobuf";

    /**
     * Header field (used in HTTP and AMQP traffic) to designate the full name
     * of the serialized protobuf.
     */
    public static final String HEADER_PROTOBUF_FULLNAME = "X-Protobuf-FullName";
}
