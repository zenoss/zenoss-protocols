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

import java.util.Date;
import java.util.Map;

/**
 * Returns properties which can be specified for a message.
 */
public interface MessageProperties {
    /**
     * Returns the message content type.
     * 
     * @return The message content type.
     */
    public String getContentType();

    /**
     * Returns the message content encoding.
     * 
     * @return The message content encoding.
     */
    public String getContentEncoding();

    /**
     * The message headers.
     * 
     * @return The message headers.
     */
    public Map<String, Object> getHeaders();

    /**
     * Returns the message delivery mode.
     * 
     * @return The message delivery mode.
     */
    public MessageDeliveryMode getDeliveryMode();

    /**
     * Returns the message priority (0-9).
     * 
     * @return The message priority.
     */
    public int getPriority();

    /**
     * Returns the message correlation id.
     * 
     * @return The message correlation id.
     */
    public String getCorrelationId();

    /**
     * Returns the message reply-to.
     * 
     * @return The message reply-to.
     */
    public String getReplyTo();

    /**
     * Returns the message expiration.
     * 
     * @return The message's expiration.
     */
    public String getExpiration();

    /**
     * Returns the message id.
     * 
     * @return The message id.
     */
    public String getMessageId();

    /**
     * Returns the message timestamp.
     * 
     * @return The message timestamp.
     */
    public Date getTimestamp();

    /**
     * Returns the message type.
     * 
     * @return The message type.
     */
    public String getType();

    /**
     * Returns the message's user id.
     * 
     * @return The message's user id.
     */
    public String getUserId();

    /**
     * Returns the message's app id.
     * 
     * @return The message's app id.
     */
    public String getAppId();
}
