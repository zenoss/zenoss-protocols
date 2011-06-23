/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
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
