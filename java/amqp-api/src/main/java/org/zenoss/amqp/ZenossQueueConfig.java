/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

import org.zenoss.utils.ZenPacks;
import org.zenoss.utils.ZenossException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class used to retrieve the {@link QueueConfig} object for Zenoss.
 */
public class ZenossQueueConfig {
    private static volatile QueueConfig sZenossQueueConfig = null;

    /**
     * Returns the {@link QueueConfig} object for merged .qjs files, including those
     * defined in ZenPacks.
     *
     * @return The {@link QueueConfig} object for the merged .qjs files.
     * @throws IOException
     */
    public static QueueConfig getConfig() throws IOException {
        if (sZenossQueueConfig == null) {
            List<InputStream> streams = new ArrayList<InputStream>();
            try {
                InputStream is = ZenossQueueConfig.class.getResourceAsStream("/org/zenoss/protobufs/zenoss.qjs");
                if (is != null) {
                    streams.add(is);
                }
                try {
                    for (String path : ZenPacks.getQueueConfigPaths()) {
                        streams.add(new BufferedInputStream(new FileInputStream(path)));
                    }
                } catch (ZenossException ignored) {
                    // Don't load from ZenPacks, I guess
                }
                sZenossQueueConfig = new QueueConfig(streams);
            } finally {
                for (InputStream is : streams) {
                    if (is != null) {
                        try {
                            is.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
            }
        }
        return sZenossQueueConfig;
    }
}
