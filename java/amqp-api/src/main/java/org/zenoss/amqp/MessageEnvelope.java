/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
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
