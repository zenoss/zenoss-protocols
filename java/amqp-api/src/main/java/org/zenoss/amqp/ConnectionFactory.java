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

import org.zenoss.amqp.impl.ConnectionFactoryImpl;

/**
 * ConnectionFactory class used to establish connections to AMQP servers.
 */
public abstract class ConnectionFactory {

    /**
     * Returns an instance of a {@link ConnectionFactory} which can be used to
     * create connections to AMQP servers.
     * 
     * @return An instance of a {@link ConnectionFactory}.
     */
    public static ConnectionFactory newInstance() {
        return new ConnectionFactoryImpl();
    }

    /**
     * Creates a new {@link Connection} to the specified {@link AmqpServerUri}.
     * 
     * @param uri
     *            URI of AMQP server to connect to.
     * @return An established connection to the server.
     * @throws AmqpException
     *             If a connection cannot be established.
     */
    public abstract Connection newConnection(AmqpServerUri uri)
            throws AmqpException;
}
