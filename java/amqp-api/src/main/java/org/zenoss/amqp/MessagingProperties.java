/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.amqp;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MessagingProperties {

    private static final Joiner dotJoiner = Joiner.on(".");
    private final Properties properties = new Properties();

    public MessagingProperties() {
    }

    public MessagingProperties(InputStream inputStream) throws IOException {
        this.load(inputStream);
    }

    public void load(InputStream inputStream) throws IOException {
        properties.load(inputStream);
    }

    public String getQueueProperty(String identifier, String key) {
        return getQueueProperty(identifier, key, null);
    }

    public String getQueueProperty(String identifier, String key, String defaultValue) {
        return getProperty("queue", identifier, key, defaultValue);
    }

    public String getExchangeProperty(String identifier, String key) {
        return getExchangeProperty(identifier, key, null);
    }

    public String getExchangeProperty(String identifier, String key, String defaultValue) {
        return getProperty("exchange", identifier, key, defaultValue);
    }

    private String getProperty(String type, String identifier, String key, String defaultValue) {
        // Get configured value
        String result = properties.getProperty(
                dotJoiner.join(type, identifier, key),
                // Get configured default
                properties.getProperty(
                        dotJoiner.join(type, "default", key),
                        // Return passed-in default
                        defaultValue)
        );
        if (result != null) {
            result = result.trim();
        }
        return result;
    }

}
