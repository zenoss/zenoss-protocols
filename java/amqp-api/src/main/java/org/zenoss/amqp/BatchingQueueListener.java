package org.zenoss.amqp;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * **************************************************************************
 * <p/>
 * Copyright (C) Zenoss, Inc. 2013, all rights reserved.
 * <p/>
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * <p/>
 * **************************************************************************
 */
public abstract class BatchingQueueListener extends QueueListener {
    protected int batchSize = DEFAULT_PREFETCH_COUNT;

    private static final int DEFAULT_PREFETCH_SIZE = 0;
    private static final int DEFAULT_PREFETCH_COUNT = 100;

    private List<org.zenoss.amqp.Message<Message>> batch = new ArrayList<org.zenoss.amqp.Message<Message>>(batchSize);

    /**
     * Set the size of batches to read for this listener
     *
     * @param batchSize batch size
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    protected void configureChannel(Channel channel) throws AmqpException {
        /* Set a default QOS on the channel */
        channel.setQos(DEFAULT_PREFETCH_SIZE, Math.min(DEFAULT_PREFETCH_COUNT, batchSize));
    }

    /**
     * Method which is called when a message is is received on the queue. The
     * default behavior of this method is to enqueue the message for handling
     * when the batch reaches {@link #batchSize}, then call {@Link #handleBatch} if
     * necessary.
     *
     * @param message The message received by the consumer.
     * @throws Exception If an exception is thrown when processing a message, it is
     *                   re-thrown and the connection is restarted.
     */
    @Override
    protected void receive(final org.zenoss.amqp.Message<Message> message) throws Exception {
        batch.add(message);
        if (batch.size() >= batchSize) {
            this.processBatch();
        }
    }

     private void processBatch() throws Exception {
     try {
     this.handle(batch);
     } finally {
     batch.clear();
     }
     }

     /**
     * Method that processes the queued batch of messages. By default, this just
     * calls {@Link #handle} on each message. This method is responsible for
     * acknowledging or rejecting every message in the Iterable.
     */
    protected void handle(Collection<org.zenoss.amqp.Message<Message>> messages) throws Exception {
        List<org.zenoss.amqp.Message<Message>> succeeded = new ArrayList<org.zenoss.amqp.Message<Message>>(batchSize);
        List<org.zenoss.amqp.Message<Message>> failed = new ArrayList<org.zenoss.amqp.Message<Message>>(batchSize);
        Exception lastException = null;

        try {
            handleBatch(messages, succeeded, failed);
        } catch (Exception e) {
            lastException = e;
        }

        for (org.zenoss.amqp.Message<Message> message : succeeded) {
            try {
                this.handled(message);
            } catch (AmqpException e) {
                //failed to acknowledge the message, but it has been processed
                lastException = e;
            }
        }

        for (org.zenoss.amqp.Message<Message> message : failed) {
            try {
                this.failed(message);
            } catch (AmqpException e) {
                //failed to un-ack the message, and it has not been processed
            }
        }

        if (failed.size() > 0) {
            throw new Exception(String.format("Batch listener encountered %d failures", failed.size()), lastException);
        }
    }

    /**
     * Handles a batch of messages, calling {@Link #handle} for each one.
     *
     * @param messages
     * @param succeeded
     * @param failed
     * @throws Exception
     */
    protected void handleBatch(Collection<org.zenoss.amqp.Message<Message>> messages, List<org.zenoss.amqp.Message<Message>> succeeded, List<org.zenoss.amqp.Message<Message>> failed) throws Exception {
        Exception lastException = null;

        for (org.zenoss.amqp.Message<Message> message : messages) {
            try {
                handle(message.getBody());
                succeeded.add(message);
            } catch (Exception e) {
                failed.add(message);
                lastException = e;
            }
        }

        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * Method which is called when no more messages remain on the queue at the moment.
     */
    public void queueEmptied() throws Exception {
        this.processBatch();
    }

}
