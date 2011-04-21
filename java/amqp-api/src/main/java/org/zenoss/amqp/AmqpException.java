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

/**
 * Exception thrown by AMQP operations.
 */
public class AmqpException extends Exception {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    /**
     * Creates an AMQP exception with no message or cause.
     */
    public AmqpException() {
        super();
    }

    /**
     * Creates an AMQP exception with the specified message and cause.
     * 
     * @param message
     *            Exception message.
     * @param cause
     *            Exception cause.
     */
    public AmqpException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an AMQP exception with the specified message.
     * 
     * @param message
     *            Exception message.
     */
    public AmqpException(String message) {
        super(message);
    }

    /**
     * Creates an AMQP exception with the specified cause.
     * 
     * @param cause
     *            Exception cause.
     */
    public AmqpException(Throwable cause) {
        super(cause);
    }

}
