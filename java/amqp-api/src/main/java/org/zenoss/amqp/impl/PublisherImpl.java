/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp.impl;

import com.rabbitmq.client.AMQP.BasicProperties;
import org.zenoss.amqp.*;

class PublisherImpl<T> implements Publisher<T> {

    protected final ChannelImpl channel;
    protected final Exchange exchange;
    protected final MessageConverter<T> converter;

    PublisherImpl(ChannelImpl channel, Exchange exchange) {
        this(channel, exchange, null);
    }

    PublisherImpl(ChannelImpl channel, Exchange exchange,
                  MessageConverter<T> converter) {
        this.channel = channel;
        this.exchange = exchange;
        this.converter = converter;
    }

    @Override
    public void publish(T body, String routingKey) throws AmqpException {
        publish(body, null, routingKey);
    }

    @Override
    public void publish(T body, MessagePropertiesBuilder propertiesBuilder,
                        String routingKey) throws AmqpException {
        if (propertiesBuilder == null) {
            propertiesBuilder = MessagePropertiesBuilder.newBuilder();
        }

        propertiesBuilder.setDeliveryMode(exchange.getDeliveryMode());
                
        try {
            final byte[] rawBody;
            if (converter != null) {
                rawBody = this.converter.toBytes(body, propertiesBuilder);
            } else {
                rawBody = (byte[]) body;
            }
            synchronized (this.channel) {
                this.channel.getWrapped().basicPublish(exchange.getName(),
                        routingKey, convertProperties(propertiesBuilder.build()),
                        rawBody);
            }
        } catch (Exception e) {
            throw new AmqpException(e);
        }
    }

    private BasicProperties convertProperties(MessageProperties properties) {
        if (properties == null) {
            return null;
        }
        // TODO: Figure out a better way to share this data and not duplicate
        BasicProperties.Builder props = new BasicProperties.Builder();
        props.appId(properties.getAppId());
        // props.setClusterId(?);
        props.contentEncoding(properties.getContentEncoding());
        props.contentType(properties.getContentType());
        props.correlationId(properties.getCorrelationId());
        if (properties.getDeliveryMode() != null) {
            props.deliveryMode(properties.getDeliveryMode().getMode());
        }
        props.expiration(properties.getExpiration());
        props.headers(properties.getHeaders());
        props.messageId(properties.getMessageId());
        props.priority(properties.getPriority());
        props.replyTo(properties.getReplyTo());
        props.timestamp(properties.getTimestamp());
        props.type(properties.getType());
        props.userId(properties.getUserId());
        return props.build();
    }

    @Override
    public Exchange getExchange() {
        return this.exchange;
    }

    @Override
    public Channel getChannel() {
        return this.channel;
    }
}
