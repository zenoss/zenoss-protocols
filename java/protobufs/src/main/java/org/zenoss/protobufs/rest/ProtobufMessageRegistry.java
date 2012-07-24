/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
