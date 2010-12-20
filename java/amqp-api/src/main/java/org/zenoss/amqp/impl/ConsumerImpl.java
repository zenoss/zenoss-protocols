/*
 * This program is part of Zenoss Core, an open source monitoring platform.
 * Copyright (C) 2010, Zenoss Inc.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published by
 * the Free Software Foundation.
 * 
 * For complete information please visit: http://www.zenoss.com/oss/
 */
package org.zenoss.amqp.impl;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import org.zenoss.amqp.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

class ConsumerImpl<T> implements Consumer<T> {

    private final ChannelImpl channel;
    private final Queue queue;
    private final boolean noAck;
    private final MessageConverter<T> converter;
    private QueueingConsumer consumer;
    private String consumerTag;

    ConsumerImpl(ChannelImpl channel, Queue queue, boolean noAck) {
        this(channel, queue, noAck, null);
    }

    ConsumerImpl(ChannelImpl channel, Queue queue, boolean noAck,
            MessageConverter<T> converter) {
        this.channel = channel;
        this.queue = queue;
        this.noAck = noAck;
        this.converter = converter;
    }

    @Override
    public Message<T> nextMessage() throws AmqpException {
        return nextMessage(0, TimeUnit.SECONDS);
    }

    @Override
    public Message<T> nextMessage(long waitTime, TimeUnit unit)
            throws AmqpException {
        synchronized (this.channel) {
            final long timeInMillis = unit.toMillis(waitTime);
            if (consumer == null) {
                consumer = new QueueingConsumer(this.channel.getWrapped());
            }
            if (consumerTag == null) {
                try {
                    consumerTag = this.channel.getWrapped().basicConsume(
                            queue.getName(), this.noAck, consumer);
                } catch (IOException e) {
                    throw new AmqpException(e);
                }
            }
            try {
                final Delivery delivery;
                if (timeInMillis > 0) {
                    delivery = consumer.nextDelivery(timeInMillis);
                    if (delivery == null) {
                        return null;
                    }
                } else {
                    delivery = consumer.nextDelivery();
                }
                return createMessage(delivery);
            } catch (ShutdownSignalException e) {
                throw new AmqpException(e);
            } catch (InterruptedException e) {
                throw new AmqpException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Message<T> createMessage(Delivery delivery) throws AmqpException {
        final T body;
        final MessageProperties properties = new BasicPropertiesWrapper(
                delivery.getProperties());
        final MessageEnvelope envelope = new EnvelopeWrapper(
                delivery.getEnvelope());
        if (converter == null) {
            body = (T) delivery.getBody();
        } else {
            try {
                body = converter.fromBytes(delivery.getBody(), properties);
            } catch (Exception e) {
                /* Throw exception with original received message on failure */
                throw new MessageDecoderException(DefaultMessage.newMessage(
                        delivery.getBody(), properties, envelope), e);
            }
            /* Throw exception if we failed to convert the message */
            if (body == null) {
                throw new MessageDecoderException(DefaultMessage.newMessage(
                        delivery.getBody(), properties, envelope));
            }
        }
        return DefaultMessage.newMessage(body, properties, envelope);
    }

    @Override
    public void cancel() throws AmqpException {
        synchronized (this.channel) {
            try {
                this.channel.getWrapped().basicCancel(consumerTag);
            } catch (IOException e) {
                throw new AmqpException(e);
            } catch (ShutdownSignalException e) {
                throw new AmqpException(e);
            }
        }
    }

    @Override
    public Queue getQueue() {
        return this.queue;
    }

    @Override
    public void ackMessage(Message<T> message) throws AmqpException {
        if (!this.noAck) {
            this.channel.ackMessage(message.getEnvelope().getDeliveryTag());
        }
    }

    @Override
    public void rejectMessage(Message<?> message, boolean requeue)
            throws AmqpException {
        this.channel.rejectMessage(message.getEnvelope().getDeliveryTag(),
                requeue);
    }
}
