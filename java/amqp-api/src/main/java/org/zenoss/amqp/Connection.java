/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp;

import java.io.Closeable;
import java.util.Map;

/**
 * Represents a connection to an AMQP server.
 */
public interface Connection extends Closeable {
    /**
     * Opens a channel used to perform operations on the AMQP server.
     * 
     * @return A channel used to perform operations on the AMQP server.
     */
    public Channel openChannel() throws AmqpException;

    /**
     * Returns properties of the AMQP server sent during connection
     * establishment.
     * 
     * @return Properties of the AMQP server.
     */
    public Map<String, Object> getServerProperties();

    /**
     * Returns properties of the AMQP client sent during connection
     * establishment.
     * 
     * @return Properties of the AMQP client.
     */
    public Map<String, Object> getClientProperties();
}
