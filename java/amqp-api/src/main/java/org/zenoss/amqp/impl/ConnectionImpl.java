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
