/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 or (at your
 * option) any later version as published by the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.amqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Bean class which represents a binding of a queue to an exchange via a routing
 * key (and optional arguments).
 */
public class Binding {
    private final Queue queue;
    private final Exchange exchange;
    private final String routingKey;
    private final Map<String, Object> arguments;

    /**
     * Creates a binding of the queue to the exchange via the routing key.
     * 
     * @param queue
     *            Queue to bind.
     * @param exchange
     *            Exchange to bind the queue to.
     * @param routingKey
     *            Routing key used for binding.
     * @throws NullPointerException
     *             If the queue, exchange, or routing key are null.
     */
    public Binding(Queue queue, Exchange exchange, String routingKey)
            throws NullPointerException {
        this(queue, exchange, routingKey, null);
    }

    /**
     * Creates a binding of the queue to the exchange via the routing key and
     * optional arguments used to create the binding.
     * 
     * @param queue
     *            Queue to bind.
     * @param exchange
     *            Exchange to bind the queue to.
     * @param routingKey
     *            Routing key used for binding.
     * @param arguments
     *            Optional arguments used to create the binding.
     * @throws NullPointerException
     *             If the queue, exchange, or routing key are null.
     */
    public Binding(Queue queue, Exchange exchange, String routingKey,
            Map<String, Object> arguments) throws NullPointerException {
        if (queue == null || exchange == null || routingKey == null) {
            throw new NullPointerException();
        }
        this.queue = queue;
        this.exchange = exchange;
        this.routingKey = routingKey;
        if (arguments == null || arguments.isEmpty()) {
            this.arguments = Collections.emptyMap();
        } else {
            Map<String, Object> args = new HashMap<String, Object>();
            args.putAll(arguments);
            this.arguments = Collections.unmodifiableMap(args);
        }
    }

    /**
     * Returns the queue.
     * 
     * @return The queue.
     */
    public Queue getQueue() {
        return queue;
    }

    /**
     * Returns the exchange.
     * 
     * @return The exchange.
     */
    public Exchange getExchange() {
        return exchange;
    }

    /**
     * Returns the routing key.
     * 
     * @return The routing key.
     */
    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * Returns an immutable map containing arguments used to create the binding.
     * 
     * @return An immutable map containing arguments used to create the binding.
     */
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('[');
        sb.append("queue=").append(queue.getName());
        sb.append(",exchange=").append(exchange.getName());
        sb.append(",routingKey=").append(routingKey);
        sb.append(']');
        return sb.toString();
    }
}
