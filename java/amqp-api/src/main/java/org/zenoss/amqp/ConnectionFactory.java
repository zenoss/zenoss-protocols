/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
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
