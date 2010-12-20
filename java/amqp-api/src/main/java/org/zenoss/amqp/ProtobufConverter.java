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
package org.zenoss.amqp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.zenoss.protobufs.ProtobufConstants;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * Message converter which can be used to perform encoding/decoding of Google
 * protobuf messages. The content type of serialized messages is set to
 * {@link ProtobufConstants#CONTENT_TYPE_PROTOBUF}, and the message header
 * {@link ProtobufConstants#HEADER_PROTOBUF_FULLNAME} is set to the
 * {@link Descriptor#getFullName()} in order to be properly decoded by the
 * receiver.
 */
public class ProtobufConverter implements MessageConverter<Message> {

    private final Map<String, Message> messagesByFullName;
    /**
     * Creates a protobuf converter which knows how to encode/decode the
     * specified protobuf message types.
     * 
     * @param messages
     *            Protobuf message types this converter supports.
     * @throws IllegalArgumentException
     *             If messages is empty.
     */
    public ProtobufConverter(Message... messages) {
        this(Arrays.asList(messages));
    }

    /**
     * Creates a protobuf converter which knows how to encode/decode the
     * specified protobuf message types.
     * 
     * @param messages
     *            The protobuf message types this converter supports.
     * @throws NullPointerException
     *             If messages is null.
     * @throws IllegalArgumentException
     *             If messages is empty.
     */
    public ProtobufConverter(List<Message> messages) {
        if (messages == null) {
            throw new NullPointerException("Messages must be non-null");
        }
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("Messages must be non-empty");
        }
        this.messagesByFullName = new HashMap<String, Message>(messages.size());
        for (Message message : messages) {
            this.messagesByFullName.put(message.getDescriptorForType()
                    .getFullName(), message);
        }
    }

    @Override
    public Message fromBytes(byte[] bytes, MessageProperties properties)
            throws Exception {
        if (!ProtobufConstants.CONTENT_TYPE_PROTOBUF.equals(properties.getContentType())) {
            throw new RuntimeException("Expected content type: "
                    + ProtobufConstants.CONTENT_TYPE_PROTOBUF);
        }
        final Object fullNameObj = properties.getHeaders().get(
                ProtobufConstants.HEADER_PROTOBUF_FULLNAME);
        if (!(fullNameObj instanceof String)) {
            throw new RuntimeException("Expected header: "
                    + ProtobufConstants.HEADER_PROTOBUF_FULLNAME);
        }
        final String fullName = (String) fullNameObj;
        Message msg = messagesByFullName.get(fullName);
        if (msg != null) {
            return msg.newBuilderForType().mergeFrom(bytes).build();
        }
        throw new UnregisteredProtobufException(fullName);
    }

    @Override
    public byte[] toBytes(Message message,
            MessagePropertiesBuilder propertyBuilder) throws Exception {
        String messageFullName = message.getDescriptorForType().getFullName();
        if (!messagesByFullName.containsKey(messageFullName)) {
            throw new IllegalArgumentException("Protobuf converter was passed a message of type " +
                    messageFullName + " but can't handle it");
        }
        propertyBuilder.setContentType(ProtobufConstants.CONTENT_TYPE_PROTOBUF);
        propertyBuilder.addHeader(ProtobufConstants.HEADER_PROTOBUF_FULLNAME, message
                .getDescriptorForType().getFullName());
        return message.toByteArray();
    }
}
