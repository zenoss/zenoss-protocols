/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing an AMQP exchange.
 */
public class Exchange {
    /**
     * The type of AMQP exchange.
     */
    public enum Type {
        /**
         * A direct AMQP exchange which requires an exact match of the routing
         * key for messages to be delivered.
         */
        DIRECT("direct"),

        /**
         * A fanout AMQP exchange in which all messages are delivered to all
         * bound queues regardless of the routing key.
         */
        FANOUT("fanout"),

        /**
         * A topic AMQP exchange in which messages are matched against a routing
         * key to determine if they are delivered.
         */
        TOPIC("topic"),

        /**
         * A headers AMQP exchange in which message headers determine which queues the message should be delivered to.
         */
        HEADERS("headers");

        private final String name;

        private Type(String name) {
            this.name = name;
        }

        /**
         * Returns the exchange type's name (suitable for passing over AMQP
         * transport.
         * 
         * @return Exchange type's name (suitable for passing over AMQP
         *         transport).
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the {@link Exchange.Type} corresponding to the specified
         * name.
         * 
         * @param name
         *            Exchange type name.
         * @return Corresponding exchange type, or null if the name doesn't
         *         match a defined type.
         */
        public static Type fromName(String name) {
            Type type = null;
            for (Type t : values()) {
                if (t.name.equals(name)) {
                    type = t;
                }
            }
            return type;
        }
    }

    private final String name;
    private final Type type;
    private final boolean durable;
    private final boolean autoDelete;
    private final Map<String, Object> arguments;

    /**
     * Create an exchange with the specified name, type, durable, and autoDelete
     * settings.
     * 
     * @param name
     *            The name of the exchange.
     * @param type
     *            The type of the exchange.
     * @param durable
     *            If the exchange should persist following a restart.
     * @param autoDelete
     *            If the exchange should automatically be deleted when no longer
     *            in use.
     * @throws NullPointerException
     *             If the exchange name or type is null.
     */
    public Exchange(String name, Type type, boolean durable, boolean autoDelete)
            throws NullPointerException {
        this(name, type, durable, autoDelete, null);
    }

    /**
     * Create an exchange with the specified name, type, durable, autoDelete,
     * and optional arguments.
     * 
     * @param name
     *            The name of the exchange.
     * @param type
     *            The type of the exchange.
     * @param durable
     *            If the exchange should persist following a restart.
     * @param autoDelete
     *            If the exchange should automatically be deleted when no longer
     *            in use.
     * @param arguments
     *            Optional arguments used when defining the exchange.
     * @throws NullPointerException
     *             If the exchange name or type is null.
     */
    public Exchange(String name, Type type, boolean durable, boolean autoDelete, Map<String, Object> arguments)
            throws NullPointerException {
        if (name == null || type == null) {
            throw new NullPointerException();
        }
        this.name = name;
        this.type = type;
        this.durable = durable;
        this.autoDelete = autoDelete;
        if (arguments == null || arguments.isEmpty()) {
            this.arguments = Collections.emptyMap();
        } else {
            Map<String, Object> args = new HashMap<String, Object>();
            args.putAll(arguments);
            this.arguments = Collections.unmodifiableMap(args);
        }
    }

    /**
     * Returns the name of the exchange.
     * 
     * @return The name of the exchange.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type of the exchange.
     * 
     * @return The type of the exchange.
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns true if the exchange persists following a restart of the server.
     * 
     * @return True if the exchange persists following a restart of the server.
     */
    public boolean isDurable() {
        return durable;
    }

    /**
     * Returns true if the exchange should be automatically deleted when no
     * longer in use.
     * 
     * @return True if the exchange should be automatically deleted when no
     *         longer in use.
     */
    public boolean isAutoDelete() {
        return autoDelete;
    }

    /**
     * Returns an immutable map of arguments used to create the exchange.
     * 
     * @return An immutable map of arguments used to create the exchange.
     */
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('[');
        sb.append("name=").append(name);
        sb.append(",type=").append(type);
        sb.append(",durable=").append(durable);
        sb.append(",autodelete=").append(autoDelete);
        if (!arguments.isEmpty()) {
            sb.append(",arguments=").append(this.arguments);
        }
        sb.append(']');
        return sb.toString();
    }
}
