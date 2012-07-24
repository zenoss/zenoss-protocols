/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing an AMQP queue.
 */
public class Queue {
    private final String name;
    private final boolean durable;
    private final boolean exclusive;
    private final boolean autoDelete;
    private final Map<String, Object> arguments;

    /**
     * Creates a queue with the specified name, durable, exclusive, and
     * autoDelete settings.
     * 
     * @param name
     *            Queue name.
     * @param durable
     *            True if the queue should be persisted after a restart.
     * @param exclusive
     *            If this queue is exclusive to a connection.
     * @param autoDelete
     *            If the queue should be deleted when no longer in use.
     * @throws IllegalArgumentException
     *             If the queue name is null or empty.
     */
    public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete)
            throws IllegalArgumentException {
        this(name, durable, exclusive, autoDelete, null);
    }

    /**
     * Creates a queue with the specified name, durable, exclusive, autoDelete,
     * and optional arguments.
     * 
     * @param name
     *            Queue name.
     * @param durable
     *            True if the queue should be persisted after a restart.
     * @param exclusive
     *            If this queue is exclusive to a connection.
     * @param autoDelete
     *            If the queue should be deleted when no longer in use.
     * @param arguments
     *            Arguments to specify when creating the queue.
     * @throws IllegalArgumentException
     *             If the queue name is null or empty.
     */
    public Queue(String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments)
            throws IllegalArgumentException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name cannot be null");
        }
        this.name = name;
        this.durable = durable;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
        if (arguments == null || arguments.isEmpty()) {
            this.arguments = Collections.emptyMap();
        } else {
            this.arguments = new HashMap<String, Object>(arguments);
        }
    }

    /**
     * Returns the queue name.
     * 
     * @return The queue name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns whether the queue is persisted after a restart.
     * 
     * @return Whether the queue is persisted after a restart.
     */
    public boolean isDurable() {
        return durable;
    }

    /**
     * Returns whether the queue is exclusively for use by this connection.
     * 
     * @return Whether the queue is exclusively for use by this connections.
     */
    public boolean isExclusive() {
        return exclusive;
    }

    /**
     * Returns whether the queue should be deleted automatically after no longer
     * in use.
     * 
     * @return Whether the queue should be deleted automatically after no longer
     *         in use.
     */
    public boolean isAutoDelete() {
        return autoDelete;
    }

    /**
     * Returns optional arguments which are used when defining the queue.
     * 
     * @return Optional arguments used to define the queue.
     */
    public Map<String, Object> getArguments() {
        return Collections.unmodifiableMap(arguments);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('[');
        sb.append("name=").append(this.name);
        sb.append(",durable=").append(this.durable);
        sb.append(",exclusive=").append(this.exclusive);
        sb.append(",autodelete=").append(this.autoDelete);
        if (!arguments.isEmpty()) {
            sb.append(",arguments=").append(this.arguments);
        }
        sb.append(']');
        return sb.toString();
    }
}
