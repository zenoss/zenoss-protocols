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
 * Represents a configuration of an exchange (stored in *.qjs file).
 */
public class ExchangeConfiguration {

    private final String identifier;
    private final Exchange exchange;
    private final List<Message> messages;

    /**
     * Creates a new {@link ExchangeConfiguration} with the specified exchange,
     * messages, and description.
     *
     * @param identifier The identifier for the exchange in the .qjs file.
     * @param exchange
     *            Exchange.
     * @param messages
     *            Messages which can be published to exchange.
     */
    public ExchangeConfiguration(String identifier, Exchange exchange, Collection<Message> messages) {
        if (identifier == null || exchange == null || messages == null) {
            throw new NullPointerException();
        }
        this.identifier = identifier;
        this.exchange = exchange;
        this.messages = new ArrayList<Message>(messages);
    }

    /**
     * The identifier for this configuration in the .qjs file.
     *
     * @return The identifier for the exchange.
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Returns the exchange.
     * 
     * @return The exchange.
     */
    public Exchange getExchange() {
        return this.exchange;
    }

    /**
     * Returns the types of messages which can be published to the exchange.
     * 
     * @return The types of messages which can be published to the exchange.
     */
    public List<Message> getMessages() {
        return Collections.unmodifiableList(this.messages);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append('[');
        sb.append("identifier=").append(identifier);
        sb.append(",exchange=").append(exchange);
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
