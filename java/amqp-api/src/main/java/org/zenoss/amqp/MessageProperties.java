/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


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
