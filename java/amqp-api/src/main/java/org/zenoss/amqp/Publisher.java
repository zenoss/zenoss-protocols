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
 * An interface which is used to publish messages to an exchange with a routing
 * key.
 * 
 * @param <T>
 *            The underlying type of the message body.
 */
public interface Publisher<T> {

    /**
     * Publishes the message body to the exchange with the specified routing
     * key.
     * 
     * @param body
     *            Message body.
     * @param routingKey
     *            The routing key to use to publish the message.
     * @throws AmqpException
     *             If the message can't be published.
     */
    public void publish(T body, String routingKey) throws AmqpException;

    /**
     * Publishes the message to the exchange with the specified message
     * properties and routing key.
     * 
     * @param body
     *            The body of the message to publish.
     * @param propertiesBuilder
     *            A builder containing properties for the message.
     * @param routingKey
     *            The routing key to use to publish the message.
     * @throws AmqpException
     *             If the message can't be published.
     */
    public void publish(T body, MessagePropertiesBuilder propertiesBuilder,
            String routingKey) throws AmqpException;

    /**
     * Returns the exchange this publisher is publishing to.
     * 
     * @return The exchange this publisher is publishing to.
     */
    public Exchange getExchange();
}
