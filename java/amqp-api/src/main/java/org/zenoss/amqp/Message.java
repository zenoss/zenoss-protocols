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

/**
 * Interface representing a message published to an exchange or consumed from a
 * queue.
 * 
 * @param <T>
 *            The underlying data type of the message body.
 */
public interface Message<T> {
    /**
     * Returns the envelope wrapping the message.
     * 
     * @return The message envelope.
     */
    public MessageEnvelope getEnvelope();

    /**
     * Properties of the message.
     * 
     * @return Message properties.
     */
    public MessageProperties getProperties();

    /**
     * The message body (can be converted using a {@link MessageConverter}) or
     * returned as the raw bytes.
     * 
     * @return The message body.
     */
    public T getBody();
}
