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

import java.io.IOException;
import java.io.InputStream;

/**
 * Helper class used to retrieve the {@link QueueConfig} object for Zenoss.
 */
public class ZenossQueueConfig {
    private static volatile QueueConfig sZenossQueueConfig = null;

    /**
     * Returns the {@link QueueConfig} object for the zenoss.qjs file.
     * 
     * @return The {@link QueueConfig} object for the zenoss.qjs file.
     * @throws IOException
     */
    public static QueueConfig getConfig() throws IOException {
        if (sZenossQueueConfig == null) {
            InputStream is = null;
            try {
                is = ZenossQueueConfig.class
                        .getResourceAsStream("/org/zenoss/protobufs/zenoss.qjs");
                sZenossQueueConfig = new QueueConfig(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }
        return sZenossQueueConfig;
    }
}
