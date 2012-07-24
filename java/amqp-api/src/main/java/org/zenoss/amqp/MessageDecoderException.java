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
 * Special subclass of {@link AmqpException} returned by
 * {@link MessageConverter} when decoding a message fails (by returning null or
 * throwing an exception). This allows client code to deal appropriately with
 * unsupported message types.
 */
public class MessageDecoderException extends AmqpException {

    private static final long serialVersionUID = 1L;
    private Message<byte[]> rawMessage;

    /**
     * Creates a {@link MessageDecoderException} with the specified raw message.
     * 
     * @param rawMessage
     *            Raw message that failed to be converted.
     */
    public MessageDecoderException(Message<byte[]> rawMessage) {
        super();
        this.rawMessage = rawMessage;
    }

    /**
     * Creates a {@link MessageDecoderException} with the specified raw message
     * and exception message.
     * 
     * @param rawMessage
     *            Raw message that failed to be converted.
     * @param message
     *            Exception message describing why conversion failed.
     */
    public MessageDecoderException(Message<byte[]> rawMessage, String message) {
        super(message);
        this.rawMessage = rawMessage;
    }

    /**
     * Creates a {@link MessageDecoderException} with the specified raw message
     * and exception cause.
     * 
     * @param rawMessage
     *            Raw message that failed to be converted.
     * @param cause
     *            Underlying cause of conversion failure.
     */
    public MessageDecoderException(Message<byte[]> rawMessage, Throwable cause) {
        super(cause);
        this.rawMessage = rawMessage;
    }

    /**
     * Creates a {@link MessageDecoderException} with the specified raw message
     * and exception message.
     * 
     * @param rawMessage
     *            Raw message that failed to be converted.
     * @param message
     *            Exception message describing why conversion failed.
     * @param cause
     *            Underlying cause of conversion failure.
     */
    public MessageDecoderException(Message<byte[]> rawMessage, String message,
            Throwable cause) {
        super(message, cause);
        this.rawMessage = rawMessage;
    }

    /**
     * Returns the original message with byte[] body that failed to be
     * converted.
     * 
     * @return The original message that failed conversion.
     */
    public Message<byte[]> getRawMessage() {
        return rawMessage;
    }
}
