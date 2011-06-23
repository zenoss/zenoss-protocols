/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp.impl;

import org.zenoss.amqp.Message;
import org.zenoss.amqp.MessageEnvelope;
import org.zenoss.amqp.MessageProperties;

/**
 * Default implementation of a message.
 * 
 * @param <T>
 *            The underlying type of the message body.
 */
class DefaultMessage<T> implements Message<T> {

    private final T body;
    private final MessageProperties properties;
    private final MessageEnvelope envelope;

    /**
     * Creates a new {@link DefaultMessage} with the message properties, body,
     * and envelope.
     * 
     * @param body
     *            Message body (cannot be null).
     * @param properties
     *            Message properties.
     * @param envelope
     *            Message envelope.
     * @throws NullPointerException
     *             If the message body is null.
     */
    DefaultMessage(T body, MessageProperties properties,
            MessageEnvelope envelope) throws NullPointerException {
        if (body == null) {
            throw new NullPointerException("Cannot write null body");
        }
        this.body = body;
        this.properties = properties;
        this.envelope = envelope;
    }

    @Override
    public MessageEnvelope getEnvelope() {
        return this.envelope;
    }

    @Override
    public MessageProperties getProperties() {
        return this.properties;
    }

    @Override
    public T getBody() {
        return this.body;
    }

    /**
     * Convenience method to create messages with type-safety.
     * 
     * @param <T>
     *            Type of message body.
     * @param body
     *            Message body.
     * @param properties
     *            Message properties.
     * @return The constructed message.
     */
    static <T> Message<T> newMessage(T body, MessageProperties properties,
            MessageEnvelope envelope) {
        return new DefaultMessage<T>(body, properties, envelope);
    }

    @Override
    public String toString() {
        return String.format("Message [body=%s, properties=%s, envelope=%s]",
                body, properties, envelope);
    }

}
