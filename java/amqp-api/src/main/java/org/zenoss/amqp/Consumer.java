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

import java.util.concurrent.TimeUnit;

/**
 * A consumer created on a {@link Channel} which consumes messages from a queue
 * (optionally performing message decoding using a {@link MessageConverter}.
 * 
 * @param <T>
 *            The underlying datatype of the message body
 */
public interface Consumer<T> {
    /**
     * Consumes the next message from the queue.
     * 
     * @return The next message from the queue.
     * @throws AmqpException
     *             If an error occurs reading from the queue.
     */
    public Message<T> nextMessage() throws AmqpException;

    /**
     * Waits up to the specified amount of time for a message to arrive on the
     * queue.
     * 
     * @param waitTime
     *            The amount of time to wait for a message to arrive on the
     *            queue.
     * @param unit
     *            Unit of time.
     * @return The next message from the queue, or null if no messages were
     *         available in the specified time.
     * @throws AmqpException
     *             If an error occurs reading from the queue.
     */
    public Message<T> nextMessage(long waitTime, TimeUnit unit)
            throws AmqpException;

    /**
     * Cancels the consumer so the server sends no more messages to the client.
     * 
     * @throws AmqpException
     *             If an error occurs canceling the consumer.
     */
    public void cancel() throws AmqpException;

    /**
     * Returns the queue this consumer is consuming from.
     * 
     * @return The queue this consumer is consuming from.
     */
    public Queue getQueue();

    /**
     * Acknowledges a message.
     * 
     * @param message
     *            Message to acknowledge.
     * @throws AmqpException
     *             If an error occurs sending the acknowledgment.
     */
    public void ackMessage(Message<T> message) throws AmqpException;

    /**
     * Rejects the message.
     * 
     * @param message
     *            Message to reject.
     * @param requeue
     *            Whether the message should be re-queued.
     * @throws AmqpException
     *             If an error occurs rejecting the message.
     */
    public void rejectMessage(Message<?> message, boolean requeue)
            throws AmqpException;
}
