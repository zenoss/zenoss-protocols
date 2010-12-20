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
package org.zenoss.amqp.impl;

import java.io.IOException;
import java.util.Map;

import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.Channel;
import org.zenoss.amqp.Connection;

import com.rabbitmq.client.ShutdownSignalException;

class ConnectionImpl implements Connection {

    private com.rabbitmq.client.Connection wrapped;

    ConnectionImpl(com.rabbitmq.client.Connection wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void close() throws IOException {
        try {
            this.wrapped.close();
        } catch (ShutdownSignalException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Channel openChannel() throws AmqpException {
        try {
            return new ChannelImpl(this.wrapped.createChannel());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public Map<String, Object> getServerProperties() {
        return wrapped.getServerProperties();
    }

    @Override
    public Map<String, Object> getClientProperties() {
        return wrapped.getClientProperties();
    }

}
