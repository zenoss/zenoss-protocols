/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.amqp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.utils.ZenPacks;
import org.zenoss.utils.Zenoss;
import org.zenoss.utils.ZenossException;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class used to retrieve the {@link QueueConfig} object for Zenoss.
 */
public class ZenossQueueConfig {
    private static final Logger logger = LoggerFactory.getLogger(ZenossQueueConfig.class);
    private static volatile QueueConfig sZenossQueueConfig = null;

    private static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.warn("Failed to close {}: {}", closeable, e.getLocalizedMessage());
            }
        }
    }

    /**
     * Returns the {@link QueueConfig} object for merged .qjs files, including those
     * defined in ZenPacks.
     *
     * @return The {@link QueueConfig} object for the merged .qjs files.
     * @throws IOException
     */
    public static QueueConfig getConfig() throws IOException {
        if (sZenossQueueConfig == null) {
            synchronized (ZenossQueueConfig.class) {
                if (sZenossQueueConfig == null) {
                    sZenossQueueConfig = loadConfig();
                }
            }
        }
        return sZenossQueueConfig;
    }

    private static QueueConfig loadConfig() throws IOException {
        QueueConfig queueConfig = null;
        List<InputStream> streams = new ArrayList<InputStream>();
        try {
            InputStream is = ZenossQueueConfig.class.getResourceAsStream("/src/zenoss.qjs");
            if (is != null) {
                streams.add(is);
            }
            try {
                for (String path : ZenPacks.getQueueConfigPaths()) {
                    try {
                        streams.add(new BufferedInputStream(new FileInputStream(path)));
                    } catch (IOException e) {
                        logger.warn("Failed to load ZenPack queue configuration from {}: {}", path,
                                e.getLocalizedMessage());
                    }
                }
            } catch (ZenossException e) {
                logger.warn("Failed to determine paths to ZenPack queue configuration: {}",
                        e.getLocalizedMessage());
            }
            queueConfig = new QueueConfig(streams);
        } finally {
            for (InputStream is : streams) {
                close(is);
            }
        }
        InputStream msgConfStream = null;
        try {
            File confFile = new File(Zenoss.zenPath("etc", "messaging.conf"));
            if (confFile.isFile()) {
                msgConfStream = new BufferedInputStream(new FileInputStream(confFile));
                queueConfig.loadProperties(msgConfStream);
            }
        } catch (ZenossException e) {
            logger.warn("Failed to determine path to messaging.conf: {}", e.getLocalizedMessage());
        } catch (IOException e) {
            logger.warn("Failed to load messaging.conf configuration: {}", e.getLocalizedMessage());
        } finally {
            close(msgConfStream);
        }
        return queueConfig;
    }
}
