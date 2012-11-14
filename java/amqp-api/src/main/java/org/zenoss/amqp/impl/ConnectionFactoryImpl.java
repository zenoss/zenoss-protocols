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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;

import java.io.*;

public class ConnectionFactoryImpl extends ConnectionFactory {

    private static final Logger logger = LoggerFactory
            .getLogger(ConnectionFactoryImpl.class.getName());

    private Integer heartbeat = null;

    private int getHeartbeat() {

        if (this.heartbeat != null) {
            return (int)this.heartbeat;
        }

        // set the default heartbeat up
        this.heartbeat = 0;

        FileInputStream fstream = null;
        BufferedReader br = null;
        try {
            String line;
            String filename = String.format("%s/etc/global.conf", System.getenv("ZENHOME"));

            fstream = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(fstream));
            while((line = br.readLine()) != null) {
                if (line.startsWith("amqpheartbeat")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >=2) {
                        Integer myheartbeat = Integer.valueOf(parts[1]);
                        if (myheartbeat != null) {
                            if (myheartbeat >= 0) {
                                this.heartbeat = myheartbeat;
                                break;
                            }
                        }
                    }
                }

            }
        } catch (Exception ex) {
            logger.warn("There was a problem reading amqpconnectionheartbeat from $ZENHOME/etc/global.conf: {}", ex.toString());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {}
            } else if (fstream != null)
                try {
                    fstream.close();
                } catch (IOException ex) {}
        }
        return (int)this.heartbeat;
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

        int myhearbeat = this.getHeartbeat();
        if (myhearbeat > 0) {
            logger.info("Setting amqpconnectionheartbeat to {}", myhearbeat);
            factory.setRequestedHeartbeat(myhearbeat);
        } else {
            logger.info("No amqpconnectionheartbeat on AMQP connection");
        }
        // factory.setClientProperties(?);
        try {
            return new ConnectionImpl(factory.newConnection());
        } catch (IOException e) {
            throw new AmqpException(e);
        }
    }

}
