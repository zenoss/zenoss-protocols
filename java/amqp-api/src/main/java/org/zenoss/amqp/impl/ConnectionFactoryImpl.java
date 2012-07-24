/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp.impl;

import java.io.IOException;

import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;

public class ConnectionFactoryImpl extends ConnectionFactory {

    @Override
    public Connection newConnection(AmqpServerUri uri) throws AmqpException {
        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost(uri.getHostname());
        factory.setPort(uri.getPort());
        if (uri.getUsername() != null) {
            factory.setUsername(uri.getUsername());
        }
        if (uri.getPassword() != null) {
            factory.setPassword(new String(uri.getPassword()));
        }
        factory.setVirtualHost(uri.getVhost());
        if (uri.isSsl()) {
            try {
                factory.useSslProtocol();
            } catch (Exception e) {
                throw new AmqpException(e);
            }
        }
        // TODO: Support these parameters via AmqpServerUri?
        // factory.setRequestedChannelMax(?);
        // factory.setRequestedFrameMax(?);
        // factory.setRequestedHeartbeat(?);
        // factory.setClientProperties(?);
        try {
            return new ConnectionImpl(factory.newConnection());
        } catch (IOException e) {
            throw new AmqpException(e);
        }
    }

}
