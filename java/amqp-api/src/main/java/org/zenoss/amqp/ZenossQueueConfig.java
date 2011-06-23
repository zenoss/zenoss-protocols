/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
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
