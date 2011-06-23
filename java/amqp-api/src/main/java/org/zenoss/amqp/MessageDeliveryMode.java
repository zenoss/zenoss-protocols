/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

/**
 * Enumeration representing the message delivery mode.
 */
public enum MessageDeliveryMode {
    /**
     * Message delivery mode which specifies that the message is transient and
     * won't be persisted to disk for reliability.
     */
    NON_PERSISTENT(1),

    /**
     * Message delivery mode which specifies the message should be persisted to
     * disk for reliability.
     */
    PERSISTENT(2);

    private final int mode;

    private MessageDeliveryMode(int mode) {
        this.mode = mode;
    }

    /**
     * Returns the numeric message delivery mode.
     * 
     * @return Numeric message delivery mode.
     */
    public int getMode() {
        return mode;
    }

    /**
     * Returns the message delivery mode from the numeric mode, or null if the
     * numeric mode doesn't match.
     * 
     * @param mode
     *            The numeric mode.
     * @return The corresponding {@link MessageDeliveryMode}, or null if the
     *         numeric mode is unknown.
     */
    public static MessageDeliveryMode fromMode(int mode) {
        for (MessageDeliveryMode m : values()) {
            if (m.mode == mode) {
                return m;
            }
        }
        return null;
    }
}
