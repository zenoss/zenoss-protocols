/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp;

/**
 * Abstract class which is called back when a message is read from a queue.
 */
public abstract class QueueListener {

    private static final int DEFAULT_PREFETCH_SIZE = 0;
    private static final int DEFAULT_PREFETCH_COUNT = 1;

    protected Consumer<com.google.protobuf.Message> consumer;
    private int timeout = 0;

    protected void configureChannel(Channel channel) throws AmqpException {
        /* Set a default QOS on the channel */
        channel.setQos(DEFAULT_PREFETCH_SIZE, DEFAULT_PREFETCH_COUNT);
    }

    /**
     * @param consumer
     *            The consumer which receives the messages.
     */
    public void setConsumer(Consumer<com.google.protobuf.Message> consumer) {
        this.consumer = consumer;
    }

    /**
     * Get the queue reading timeout (in milliseconds) for this listener
     * @return timeout (in milliseconds)
     */
    public int getTimeout(){
        return timeout;
    }

    /**
     * Set the queue reading timeout (in milliseconds) for this listener
     * @param timeout
     *          timeout (in milliseconds)
     */
    public void setTimeout(int timeout){
        this.timeout = timeout;
    }

    /**
     * Method which is called when a message is is received on the queue. The
     * default behavior of this method is to acknowledge the message when the
     * {@link #handle(com.google.protobuf.Message)} method completes without
     * errors, and reject the message (and *NOT* re-queue) when an exception
     * occurs. This method will likely need to be overridden by subclasses to
     * customize when a message needs to be re-queued.
     * 
     * @param message
     *            The message received by the consumer.
     * @throws Exception
     *             If an exception is thrown when processing a message, it is
     *             re-thrown and the connection is restarted.
     */
    protected void receive(final Message<com.google.protobuf.Message> message) throws Exception {
        try {
            handle(message.getBody());
            handled(message);
        } catch (Exception e) {
            failed(message);
            throw e;
        }
    }

    /**
     * See: {@Link #receive(Message<com.google.protobuf.Message>)}
    */
    protected void receive(final Message<com.google.protobuf.Message> message,
                        final Consumer<com.google.protobuf.Message> consumer)
            throws Exception {
        this.setConsumer(consumer);
        this.receive(message);
    }

    /**
     * Method which is called when no more messages remain on the queue at the moment.
     */
    public void queueEmptied() throws Exception {}

    /**
     * Processes the message read from the queue.
     * 
     * @param message
     *            The read message.
     * @throws Exception
     *             If an exception occurs while processing the message.
     */
    protected abstract void handle(com.google.protobuf.Message message)
            throws Exception;

    /**
     * Acknowledge this message
     * @param message
     * @throws AmqpException
     */
    protected void handled(final Message message) throws AmqpException {
        this.consumer.ackMessage(message);
    }

    /**
     * Reject this message, do not re-queue
     * @param message
     * @throws AmqpException
     */
    protected void failed(final Message message) throws AmqpException {
        this.consumer.rejectMessage(message, false);
    }

    /**
     * Reject this message and requeue
     * @param message
     * @throws AmqpException
     */
    protected void unhandled(final Message message) throws AmqpException {
        this.consumer.rejectMessage(message, true);
    }
}
