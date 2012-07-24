/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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

    /**
     * Returns the channel this publisher is publishing to.
     *
     * @return The channel this publisher is publishing to.
     */
    public Channel getChannel();
}
