/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

import java.io.Closeable;

/**
 * Represents an AMQP channel which is opened on a {@link Connection} and used
 * to perform operations on the message server.
 */
public interface Channel extends Closeable {
    /**
     * Declares the specified queue on the server.
     * 
     * @param queue
     *            Message queue to define.
     * @throws AmqpException
     *             If the queue cannot be defined.
     */
    public void declareQueue(Queue queue) throws AmqpException;

    /**
     * Deletes the specified queue on the server.
     * 
     * @param queue
     *            Message queue to delete.
     * @throws AmqpException
     *             If the queue cannot be deleted.
     */
    public void deleteQueue(Queue queue) throws AmqpException;

    /**
     * Purges any items on the specified queue.
     * 
     * @param queue
     *            Queue to purge.
     * @throws AmqpException
     *             If the queue cannot be purged.
     */
    public void purgeQueue(Queue queue) throws AmqpException;

    /**
     * Declares the specified exchange.
     * 
     * @param exchange
     *            Exchange to define.
     * @throws AmqpException
     *             If the exchange cannot be defined.
     */
    public void declareExchange(Exchange exchange) throws AmqpException;

    /**
     * Deletes the specified exchange.
     * 
     * @param exchange
     *            Exchange to delete.
     * @throws AmqpException
     *             If the exchange cannot be deleted.
     */
    public void deleteExchange(Exchange exchange) throws AmqpException;

    /**
     * Creates a publisher which can be used to publish messages to the exchange
     * with the routing key. Messages are expected to already contain byte[]
     * encoded message bodies.
     * 
     * @param exchange
     *            Exchange to publish messages to.
     * @return A publisher which can be used to publish messages to the exchange
     *         with the routing key.
     * @throws AmqpException
     *             If the publisher cannot be created.
     */
    public Publisher<byte[]> createPublisher(Exchange exchange)
            throws AmqpException;

    /**
     * Creates a publisher which can be used to publish messages to the exchange
     * with the routing key. Messages are converted to the byte[] encoding via
     * the specified message converter.
     * 
     * @param <T>
     *            Underlying type of message body.
     * @param exchange
     *            Exchange to publish messages to.
     * @param converter
     *            Message converter which knows how to convert the message body
     *            to its byte[] encoding.
     * @return A publisher which can be used to publish messages to the exchange
     *         with the routing key.
     * @throws AmqpException
     *             If the publisher cannot be created.
     */
    public <T> Publisher<T> createPublisher(Exchange exchange,
            MessageConverter<T> converter) throws AmqpException;

    /**
     * Creates a batch publisher which can be used to publish messages to the
     * exchange. Messages are expected to already contain byte[] encoded message
     * bodies.
     * 
     * @param exchange
     *            Exchange to publish messages to.
     * @return A publisher which can be used to publish messages to the exchange
     *         with the routing key.
     * @throws AmqpException
     *             If the publisher cannot be created.
     */
    public BatchPublisher<byte[]> createBatchPublisher(Exchange exchange)
            throws AmqpException;

    /**
     * Creates a batch publisher which can be used to publish messages to the
     * exchange. Messages are converted to the byte[] encoding via the specified
     * message converter.
     * 
     * @param <T>
     *            Underlying type of message body.
     * @param exchange
     *            Exchange to publish messages to.
     * @param converter
     *            Message converter which knows how to convert the message body
     *            to its byte[] encoding.
     * @return A publisher which can be used to publish messages to the exchange
     *         with the routing key.
     * @throws AmqpException
     *             If the publisher cannot be created.
     */
    public <T> BatchPublisher<T> createBatchPublisher(Exchange exchange,
            MessageConverter<T> converter) throws AmqpException;

    /**
     * Creates the binding of queue to exchange with the routing key.
     * 
     * @param binding
     *            Object representing the binding of queue to exchange with
     *            routing key.
     * @throws AmqpException
     *             If the binding cannot be created.
     */
    public void bindQueue(Binding binding) throws AmqpException;

    /**
     * Deletes the binding of queue to exchange with routing key.
     * 
     * @param binding
     *            Object representing the binding of queue to exchange with
     *            routing key.
     * @throws AmqpException
     *             If the binding cannot be deleted.
     */
    public void unbindQueue(Binding binding) throws AmqpException;

    /**
     * Creates a consumer to consume raw byte[] messages from the specified
     * queue. All messages should be acknowledged with a call to
     * {@link Consumer#ackMessage(Message)} or
     * {@link Consumer#rejectMessage(Message, boolean)}
     * 
     * @param queue
     *            The queue to consume from.
     * @return The consumer used to consume messages from the queue.
     * @throws AmqpException
     *             If the consumer cannot be created.
     */
    public Consumer<byte[]> createConsumer(Queue queue) throws AmqpException;

    /**
     * @param queue
     *            The queue to consume from.
     * @param noAck
     *            Whether messages should require an explicit acknowledgment.
     * @return The consumer used to consume messages from the queue. If noAck is
     *         false, then all messages should be acknowledged with a call to
     *         {@link Consumer#ackMessage(Message)} or
     *         {@link Consumer#rejectMessage(Message, boolean)}.
     * @throws AmqpException
     */
    public Consumer<byte[]> createConsumer(Queue queue, boolean noAck)
            throws AmqpException;

    /**
     * Creates a consumer to consume messages from the queue and perform
     * decoding of the message bodies using the specified message converter.
     * 
     * @param <T>
     *            The underlying message body data type.
     * @param queue
     *            The queue to consume from.
     * @param converter
     *            The converter used to decode the message body.
     * @return The consumer used to consume messages from the queue. All
     *         messages should be acknowledged with a call to
     *         {@link Consumer#ackMessage(Message)} or
     *         {@link Consumer#rejectMessage(Message, boolean)}
     * @throws AmqpException
     *             If the consumer cannot be created.
     */
    public <T> Consumer<T> createConsumer(Queue queue,
            MessageConverter<T> converter) throws AmqpException;

    /**
     * Creates a consumer to consume messages from the queue and perform
     * decoding of the message bodies using the specified message converter.
     * 
     * @param <T>
     *            The underlying message body data type.
     * @param queue
     *            The queue to consume from.
     * @param converter
     *            The converter used to decode the message body.
     * @param noAck
     *            Whether messages should require an explicit acknowledgment.
     * @return The consumer used to consume messages from the queue. If noAck is
     *         false, then all messages should be acknowledged with a call to
     *         {@link Consumer#ackMessage(Message)} or
     *         {@link Consumer#rejectMessage(Message, boolean)}
     */
    public <T> Consumer<T> createConsumer(Queue queue,
            MessageConverter<T> converter, boolean noAck);

    /**
     * Returns true if the channel has enabled transactions via a call to
     * {@link #enableTransactions()}.
     * 
     * @return True if transactions are enabled on the channel, false otherwise.
     */
    public boolean isTransactionsEnabled();

    /**
     * Enables transactions on the channel.
     * 
     * @throws AmqpException
     *             If transactions cannot be enabled.
     */
    public void enableTransactions() throws AmqpException;

    /**
     * Commits any pending transactions on the channel.
     * 
     * @throws AmqpException
     *             If transactions cannot be committed.
     */
    public void commitTransaction() throws AmqpException;

    /**
     * Rolls back any pending transactions on this channel.
     * 
     * @throws AmqpException
     *             If transactions cannot be rolled back.
     */
    public void rollbackTransaction() throws AmqpException;
}
