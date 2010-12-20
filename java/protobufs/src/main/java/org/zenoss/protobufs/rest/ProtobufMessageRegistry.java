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
package org.zenoss.protobufs.rest;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

/**
 * Contains a registry of supported protobuf Messages which are supported for
 * deserialization.
 */
public interface ProtobufMessageRegistry {
    /**
     * Returns the Message corresponding to the message descriptor's full name.
     * 
     * @param fullName
     *            The full name of the message.
     * @return The message instance, or null if the message full name is not
     *         supported.
     * @see Descriptor#getFullName()
     */
    public Message getMessageByFullName(String fullName);

    /**
     * Returns an extension registry used when decoding messages.
     * 
     * @return An extension registry used to decode messages.
     * @see Message.Builder#mergeFrom(java.io.InputStream,
     *      com.google.protobuf.ExtensionRegistryLite)
     */
    public ExtensionRegistry getExtensionRegistry();
}
