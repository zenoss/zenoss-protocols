/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 or (at your
 * option) any later version as published by the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.amqp;

/**
 * Interface which can be used to convert a message to/from its raw byte[]
 * encoding for simplifying consumers and producers.
 * 
 * @param <T>
 *            The underlying type of a message body.
 */
public interface MessageConverter<T> {
    /**
     * Converts a message from the raw byte[] representation to a more
     * convenient type. If the message fails encoding or this method returns
     * null, then an exception of type {@link MessageDecoderException} is thrown
     * to the consumer containing the original message which failed to be
     * decoded.
     * 
     * @param bytes
     *            The raw byte[] representation of the message body.
     * @param properties
     *            The properties of the message.
     * @return The decoded message body type.
     * @throws Exception
     *             If the message cannot be converted.
     * @see MessageDecoderException
     */
    public T fromBytes(byte[] bytes, MessageProperties properties)
            throws Exception;

    /**
     * Converts a message from its native type to a byte[] for encoding.
     * 
     * @param message
     *            The message body type.
     * @param propertyBuilder
     *            Message property builder.
     * @return The raw byte[] representation of the message body.
     * @throws Exception
     *             If the message cannot be converted.
     */
    public byte[] toBytes(T message, MessagePropertiesBuilder propertyBuilder)
            throws Exception;
}
