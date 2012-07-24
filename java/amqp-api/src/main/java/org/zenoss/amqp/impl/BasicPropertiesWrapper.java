/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp.impl;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.zenoss.amqp.MessageDeliveryMode;
import org.zenoss.amqp.MessageProperties;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.impl.LongString;

/**
 * Wraps a RabbitMQ {@link BasicProperties} in the {@link MessageProperties}
 * interface.
 */
class BasicPropertiesWrapper implements MessageProperties {

    private final BasicProperties properties;
    private final Map<String, Object> headers;

    BasicPropertiesWrapper(BasicProperties basicProperties) {
        this.properties = basicProperties;
        /*
         * We want to convert header values of type LongString (RabbitMQ
         * specific) to String.
         */
        if (basicProperties.getHeaders() != null
                && !basicProperties.getHeaders().isEmpty()) {
            headers = new HashMap<String, Object>();
            for (Map.Entry<String, Object> entry : basicProperties.getHeaders()
                    .entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                if (val instanceof LongString) {
                    try {
                        headers.put(key,
                                new String(((LongString) val).getBytes(),
                                        "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException("UTF-8 encoding required", e);
                    }
                } else {
                    headers.put(key, val);
                }
            }
        } else {
            headers = Collections.emptyMap();
        }
    }

    @Override
    public String getContentType() {
        return properties.getContentType();
    }

    @Override
    public String getContentEncoding() {
        return properties.getContentEncoding();
    }

    @Override
    public Map<String, Object> getHeaders() {
        return headers;
    }

    @Override
    public MessageDeliveryMode getDeliveryMode() {
        Integer deliveryMode = properties.getDeliveryMode();
        return (deliveryMode != null) ? MessageDeliveryMode
                .fromMode(deliveryMode.intValue()) : null;
    }

    @Override
    public int getPriority() {
        return (properties.getPriority() != null) ? properties.getPriority()
                .intValue() : 0;
    }

    @Override
    public String getCorrelationId() {
        return properties.getCorrelationId();
    }

    @Override
    public String getReplyTo() {
        return properties.getReplyTo();
    }

    @Override
    public String getExpiration() {
        return properties.getExpiration();
    }

    @Override
    public String getMessageId() {
        return properties.getMessageId();
    }

    @Override
    public Date getTimestamp() {
        return properties.getTimestamp();
    }

    @Override
    public String getType() {
        return properties.getType();
    }

    @Override
    public String getUserId() {
        return properties.getUserId();
    }

    @Override
    public String getAppId() {
        return properties.getAppId();
    }

    @Override
    public String toString() {
        return String
                .format("MessageProperties [contentType=%s, contentEncoding=%s, headers=%s, deliveryMode=%s, priority=%s, correlationId=%s, replyTo=%s, expiration=%s, messageId=%s, timestamp=%s, type=%s, userId=%s, appId=%s]",
                        getContentType(), getContentEncoding(), getHeaders(),
                        getDeliveryMode(), getPriority(), getCorrelationId(),
                        getReplyTo(), getExpiration(), getMessageId(),
                        getTimestamp(), getType(), getUserId(), getAppId());
    }

}
