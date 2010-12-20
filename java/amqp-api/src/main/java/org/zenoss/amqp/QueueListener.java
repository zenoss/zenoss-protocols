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
 * Abstract class which is called back when a message is read from a queue.
 */
public abstract class QueueListener {

    /**
     * Method which is called when a message is is received on the queue. The
     * default behavior of this method is to acknowledge the message when the
     * {@link #handle(com.google.protobuf.Message)} method completes without
     * errors, and reject the message (and *NOT* re-queue) when an exception
     * occurs. This method will likely need to be overridden by subclasses to
     * customize when a message needs to be re-queued.
     * 
     * @param message
     *            The message received by the consumer.
     * @param consumer
     *            The consumer which received the message.
     * @throws Exception
     *             If an exception is thrown when processing a message, it is
     *             re-thrown and the connection is restarted.
     */
    protected void receive(final Message<com.google.protobuf.Message> message,
            final Consumer<com.google.protobuf.Message> consumer)
            throws Exception {
        try {
            handle(message.getBody());
            consumer.ackMessage(message);
        } catch (Exception e) {
            consumer.rejectMessage(message, false);
            throw e;
        }
    }

    /**
     * Processes the message read from the queue.
     * 
     * @param message
     *            The read message.
     * @throws Exception
     *             If an exception occurs while processing the message.
     */
    public abstract void handle(com.google.protobuf.Message message)
            throws Exception;
}
