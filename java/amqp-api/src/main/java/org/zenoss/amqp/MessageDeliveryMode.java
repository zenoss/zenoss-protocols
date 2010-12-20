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