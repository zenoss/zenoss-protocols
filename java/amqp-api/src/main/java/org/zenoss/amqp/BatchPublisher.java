/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

import java.io.Closeable;

/**
 * An interface which is used to publish messages to an exchange using AMQP
 * transactions.
 * 
 * @param <T>
 *            The underlying type of the message body.
 */
public interface BatchPublisher<T> extends Publisher<T>, Closeable {
    /**
     * Commits any pending transactions on the channel.
     * 
     * @throws AmqpException
     *             If transactions cannot be committed.
     */
    public void commit() throws AmqpException;

    /**
     * Rolls back any pending transactions on this channel.
     * 
     * @throws AmqpException
     *             If transactions cannot be rolled back.
     */
    public void rollback() throws AmqpException;
}
