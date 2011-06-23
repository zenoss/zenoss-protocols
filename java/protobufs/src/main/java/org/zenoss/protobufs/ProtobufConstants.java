/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
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
