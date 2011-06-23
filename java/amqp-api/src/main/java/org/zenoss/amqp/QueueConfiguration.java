/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */

package org.zenoss.amqp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.Message;

/**
 * Represents the configuration for communicating with a specific queue.
 */
public class QueueConfiguration {

    private final Queue queue;
    private final List<Binding> bindings;
    private final List<Message> messages;

    /**
     * Creates a new {@link QueueConfiguration} with the specified queue, list
     * of bindings, and list of messages.
     * 
     * @param queue
     *            Queue.
     * @param bindings
     *            List of bindings.
     * @param messages
     *            List of messages consumed from the queue.
     */
    public QueueConfiguration(Queue queue, Collection<Binding> bindings,
            Collection<Message> messages) {
        if (queue == null || bindings == null || messages == null) {
            throw new NullPointerException();
        }
        this.queue = queue;
        this.bindings = new ArrayList<Binding>(bindings);
        this.messages = new ArrayList<Message>(messages);
    }

    /**
     * Returns the queue where messages are consumed.
     * 
     * @return The queue.
     */
    public Queue getQueue() {
        return this.queue;
    }

    /**
     * Returns the bindings of the queue to the exchange(s).
     * 
     * @return The bindings.
     */
    public List<Binding> getBindings() {
        return Collections.unmodifiableList(this.bindings);
    }

    /**
     * Returns the messages which are consumed from the queue.
     * 
     * @return The protobuf messages which are published to the exchange and
     *         consumed from the queue.
     */
    public List<Message> getMessages() {
        return Collections.unmodifiableList(this.messages);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('[');
        sb.append("queue=").append(queue);
        sb.append(",bindings=").append(bindings);
        sb.append(",messages=[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(messages.get(i).getDescriptorForType().getFullName());
        }
        sb.append(']');
        return sb.toString();
    }
}
