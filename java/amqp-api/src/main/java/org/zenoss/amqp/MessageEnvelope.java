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

/**
 * The message envelope (delivery tag, redelivery, exchange name, and routing
 * key).
 */
public interface MessageEnvelope {
    /**
     * Returns the delivery tag for the message (used to acknowledge or reject
     * the message).
     * 
     * @return The delivery tag for the message.
     */
    public long getDeliveryTag();

    /**
     * Returns true if the message was re-delivered.
     * 
     * @return Whether the message was re-delivered.
     */
    public boolean isRedeliver();

    /**
     * The name of the exchange that the message where the message was
     * published.
     * 
     * @return The name of the exchange where the message was published.
     */
    public String getExchangeName();

    /**
     * The routing key used to publish the message.
     * 
     * @return The routing key used to publish the message.
     */
    public String getRoutingKey();
}
