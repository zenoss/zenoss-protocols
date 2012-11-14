/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;
import org.zenoss.utils.Zenoss;

import java.util.Properties;

public class ConnectionFactoryImpl extends ConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactoryImpl.class);

    private static int DEFAULT_HEARTBEAT = -1;

    static {
        // Attempt to load the default heartbeat from the global.conf file on the system
        BufferedInputStream bis = null;
        try {
            File globalConf = new File(Zenoss.zenPath("etc", "global.conf"));
            if (globalConf.isFile()) {
                bis = new BufferedInputStream(new FileInputStream(globalConf));
                Properties props = new Properties();
                props.load(bis);
                String amqpHeartbeatStrVal = props.getProperty("amqpheartbeat");
                if (amqpHeartbeatStrVal != null) {
                    DEFAULT_HEARTBEAT = Integer.parseInt(amqpHeartbeatStrVal.trim());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to load AMQP heartbeat from global.conf: {}", e.getLocalizedMessage());
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (Exception e) {
                    // Ignore errors on close
                }
            }
        }
    }

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

        if (DEFAULT_HEARTBEAT > 0) {
            logger.info("Setting AMQP connection heartbeat to {}", DEFAULT_HEARTBEAT);
            factory.setRequestedHeartbeat(DEFAULT_HEARTBEAT);
        } else {
            logger.info("No AMQP connection heartbeat");
        }
        // factory.setClientProperties(?);
        try {
            return new ConnectionImpl(factory.newConnection());
        } catch (IOException e) {
            throw new AmqpException(e);
        }
    }

}
