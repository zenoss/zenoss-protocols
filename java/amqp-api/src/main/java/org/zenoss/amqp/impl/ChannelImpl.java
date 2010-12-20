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

import java.io.IOException;

import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.BatchPublisher;
import org.zenoss.amqp.Binding;
import org.zenoss.amqp.Channel;
import org.zenoss.amqp.Consumer;
import org.zenoss.amqp.Exchange;
import org.zenoss.amqp.MessageConverter;
import org.zenoss.amqp.Publisher;
import org.zenoss.amqp.Queue;

import com.rabbitmq.client.ShutdownSignalException;

class ChannelImpl implements Channel {

    private com.rabbitmq.client.Channel wrapped;
    private volatile boolean transactionsEnabled = false;

    ChannelImpl(com.rabbitmq.client.Channel wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            this.wrapped.close();
        } catch (ShutdownSignalException e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized void declareQueue(Queue queue) throws AmqpException {
        try {
            this.wrapped.queueDeclare(queue.getName(), queue.isDurable(),
                    queue.isExclusive(), queue.isAutoDelete(),
                    queue.getArguments());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void deleteQueue(Queue queue) throws AmqpException {
        try {
            this.wrapped.queueDelete(queue.getName());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void purgeQueue(Queue queue) throws AmqpException {
        try {
            this.wrapped.queuePurge(queue.getName());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void declareExchange(Exchange exchange)
            throws AmqpException {
        try {
            this.wrapped.exchangeDeclare(exchange.getName(), exchange.getType()
                    .getName(), exchange.isDurable(), exchange.isAutoDelete(),
                    exchange.getArguments());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void deleteExchange(Exchange exchange)
            throws AmqpException {
        try {
            this.wrapped.exchangeDelete(exchange.getName());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public Publisher<byte[]> createPublisher(Exchange exchange)
            throws AmqpException {
        return new PublisherImpl<byte[]>(this, exchange);
    }

    @Override
    public <T> Publisher<T> createPublisher(Exchange exchange,
            MessageConverter<T> converter) throws AmqpException {
        return new PublisherImpl<T>(this, exchange, converter);
    }

    @Override
    public BatchPublisher<byte[]> createBatchPublisher(Exchange exchange)
            throws AmqpException {
        return new BatchPublisherImpl<byte[]>(this, exchange);
    }

    @Override
    public <T> BatchPublisher<T> createBatchPublisher(Exchange exchange,
            MessageConverter<T> converter) throws AmqpException {
        return new BatchPublisherImpl<T>(this, exchange, converter);
    }

    @Override
    public synchronized void bindQueue(Binding binding) throws AmqpException {
        try {
            this.wrapped.queueBind(binding.getQueue().getName(), binding
                    .getExchange().getName(), binding.getRoutingKey(), binding
                    .getArguments());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void unbindQueue(Binding binding) throws AmqpException {
        try {
            this.wrapped.queueUnbind(binding.getQueue().getName(), binding
                    .getExchange().getName(), binding.getRoutingKey(), binding
                    .getArguments());
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public Consumer<byte[]> createConsumer(Queue queue) throws AmqpException {
        return createConsumer(queue, false);
    }

    @Override
    public <T> Consumer<T> createConsumer(Queue queue,
            MessageConverter<T> converter) throws AmqpException {
        return createConsumer(queue, converter, false);
    }

    @Override
    public boolean isTransactionsEnabled() {
        return this.transactionsEnabled;
    }

    @Override
    public synchronized void enableTransactions() throws AmqpException {
        try {
            this.wrapped.txSelect();
            this.transactionsEnabled = true;
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    public synchronized void ackMessage(long deliveryTag) throws AmqpException {
        try {
            this.wrapped.basicAck(deliveryTag, false);
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    public synchronized void rejectMessage(long deliveryTag, boolean requeue)
            throws AmqpException {
        try {
            this.wrapped.basicReject(deliveryTag, requeue);
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void commitTransaction() throws AmqpException {
        try {
            this.wrapped.txCommit();
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    @Override
    public synchronized void rollbackTransaction() throws AmqpException {
        try {
            this.wrapped.txRollback();
        } catch (IOException e) {
            throw new AmqpException(e);
        } catch (ShutdownSignalException e) {
            throw new AmqpException(e);
        }
    }

    com.rabbitmq.client.Channel getWrapped() {
        return wrapped;
    }

    @Override
    public Consumer<byte[]> createConsumer(Queue queue, boolean noAck)
            throws AmqpException {
        return new ConsumerImpl<byte[]>(this, queue, noAck);
    }

    @Override
    public <T> Consumer<T> createConsumer(Queue queue,
            MessageConverter<T> converter, boolean noAck) {
        return new ConsumerImpl<T>(this, queue, noAck, converter);
    }

}
