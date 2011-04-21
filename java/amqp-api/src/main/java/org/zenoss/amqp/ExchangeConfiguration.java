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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.Message;

/**
 * Represents a configuration of an exchange (stored in *.qjs file).
 */
public class ExchangeConfiguration {

    private final Exchange exchange;
    private final List<Message> messages;

    /**
     * Creates a new {@link ExchangeConfiguration} with the specified exchange
     * and messages.
     * 
     * @param exchange
     *            Exchange.
     * @param messages
     *            Messages which can be published to exchange.
     */
    public ExchangeConfiguration(Exchange exchange, Collection<Message> messages) {
        if (exchange == null || messages == null) {
            throw new NullPointerException();
        }
        this.exchange = exchange;
        this.messages = new ArrayList<Message>(messages);
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
        sb.append("exchange=").append(exchange);
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
