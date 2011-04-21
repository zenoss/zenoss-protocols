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
