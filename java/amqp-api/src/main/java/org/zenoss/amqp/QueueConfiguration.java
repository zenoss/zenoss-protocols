/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents the configuration for communicating with a specific queue.
 */
public class QueueConfiguration {

    private final String identifier;
    private final Queue queue;
    private final List<Binding> bindings;
    private final List<Message> messages;

    /**
     * Creates a new {@link QueueConfiguration} with the specified queue, list
     * of bindings, and list of messages.
     *
     * @param identifier The identifier for the queue in the .qjs file.
     * @param queue
     *            Queue.
     * @param bindings
     *            List of bindings.
     * @param messages
     *            List of messages consumed from the queue.
     */
    public QueueConfiguration(String identifier, Queue queue, Collection<Binding> bindings,
                              Collection<Message> messages) {
        if (identifier == null || queue == null || bindings == null || messages == null) {
            throw new NullPointerException();
        }
        this.identifier = identifier;
        this.queue = queue;
        this.bindings = new ArrayList<Binding>(bindings);
        this.messages = new ArrayList<Message>(messages);
    }

    /**
     * Returns the identifier for the queue in the .qjs file.
     *
     * @return The identifier for the queue in the .qjs file.
     */
    public String getIdentifier() {
        return identifier;
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
        sb.append("identifier=").append(identifier);
        sb.append(",queue=").append(queue);
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
