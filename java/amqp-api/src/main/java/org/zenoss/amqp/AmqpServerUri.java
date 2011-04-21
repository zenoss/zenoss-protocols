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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In order to represent configuration of AMQP connections, a URI scheme has
 * been defined to centralize all information needed to connect to a AMQP
 * server. The URI scheme is as follows:
 * 
 * <code>amqp[s]://[user:pass]@hostname[:port][/vhost][?properties]</code>
 * 
 * The URI scheme must be specified as either <code>amqp</code> (
 * {@link #SCHEME_AMQP}) for unencrypted or <code>amqps</code> (
 * {@link #SCHEME_AMQPS}) for SSL.
 */
public class AmqpServerUri {
    private static final Logger logger = LoggerFactory
            .getLogger(AmqpServerUri.class);
    public static final String SCHEME_AMQP = "amqp";
    public static final String SCHEME_AMQPS = "amqps";
    public static final int DEFAULT_PORT = 5672;
    public static final int DEFAULT_SSL_PORT = 5671;
    private final boolean isSsl;
    private final String hostname;
    private final int port;
    private final String username;
    private final char[] password;
    private final String vhost;
    private final Map<String, String> properties;

    /**
     * Creates a {@link AmqpServerUri} from the String URI.
     * 
     * @param uri
     *            String URI.
     * @throws URISyntaxException
     *             If the uri is invalid.
     */
    public AmqpServerUri(String uri) throws URISyntaxException {
        this(new URI(uri));
    }

    /**
     * Creates a {@link AmqpServerUri} from the URI.
     * 
     * @param uri
     *            URI used to create {@link AmqpServerUri}.
     * @throws IllegalArgumentException
     *             If the scheme is invalid or required information is missing.
     */
    public AmqpServerUri(URI uri) throws IllegalArgumentException {
        // Normalize URI if necessary
        uri = uri.normalize();
        if (SCHEME_AMQPS.equals(uri.getScheme())) {
            isSsl = true;
        } else if (SCHEME_AMQP.equals(uri.getScheme())) {
            isSsl = false;
        } else {
            throw new IllegalArgumentException("Invalid AMQP scheme: "
                    + uri.getScheme());
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Host must be specified");
        }
        this.hostname = uri.getHost();
        if (uri.getPort() == -1) {
            if (isSsl) {
                this.port = DEFAULT_SSL_PORT;
            } else {
                this.port = DEFAULT_PORT;
            }
        } else {
            this.port = uri.getPort();
        }

        if (uri.getUserInfo() == null) {
            this.username = null;
            this.password = null;
        } else {
            String[] userPass = uri.getUserInfo().split(":", 2);
            if (userPass.length == 2) {
                this.username = userPass[0];
                this.password = userPass[1].toCharArray();
            } else {
                this.username = uri.getUserInfo();
                this.password = null;
            }
        }
        if (uri.getPath() != null && uri.getPath().length() > 1) {
            if (uri.getPath().indexOf('/', 1) != -1) {
                throw new IllegalArgumentException(
                        "Too many components in path");
            }
            this.vhost = uri.getPath();
        } else {
            this.vhost = "/";
        }
        this.properties = new HashMap<String, String>();
        if (uri.getQuery() != null) {
            String[] queryProperties = uri.getQuery().split(";");
            for (String queryProperty : queryProperties) {
                String[] nameVal = queryProperty.split("=", 2);
                if (nameVal.length < 2) {
                    continue;
                }
                String name = nameVal[0];
                String value = nameVal[1];
                if (name.length() > 0) {
                    properties.put(name, value);
                }
            }
        }
    }

    public boolean isSsl() {
        return isSsl;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public char[] getPassword() {
        return (password == null) ? password : password.clone();
    }

    public String getVhost() {
        return vhost;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    private static boolean objectsEqual(Object o1, Object o2) {
        return (o1 == null) ? o2 == null : o1.equals(o2);
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode = 37 * hashCode + Boolean.valueOf(this.isSsl).hashCode();
        hashCode = 37 * hashCode + this.hostname.hashCode();
        hashCode = 37 * hashCode + this.port;
        if (this.username != null) {
            hashCode = 37 * hashCode + this.username.hashCode();
        }
        if (this.password != null) {
            hashCode = 37 * hashCode + Arrays.hashCode(this.password);
        }
        hashCode = 37 * hashCode + this.vhost.hashCode();
        hashCode = 37 * hashCode + this.properties.hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AmqpServerUri)) {
            return false;
        }
        AmqpServerUri other = (AmqpServerUri) obj;
        return this.isSsl == other.isSsl
                && this.hostname.equals(other.hostname)
                && this.port == other.port
                && objectsEqual(this.username, other.username)
                && Arrays.equals(this.password, other.password)
                && this.vhost.equals(other.vhost)
                && this.properties.equals(other.properties);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.isSsl) {
            sb.append(SCHEME_AMQPS);
        } else {
            sb.append(SCHEME_AMQP);
        }
        sb.append("://");
        if (this.username != null) {
            sb.append(this.username).append('@');
        }
        sb.append(this.hostname).append(':').append(this.port)
                .append(this.vhost);
        return sb.toString();
    }

    /**
     * Creates a {@link AmqpServerUri} from the specified global configuration
     * settings.
     * 
     * @param globalProps
     *            The contents of the global.conf file on the system. This
     *            configuration file should contain the properties amqpusessl,
     *            amqphost, amqpuser, amqppassword, amqpvhost, and amqpport.
     * @param defaultUri
     *            The URI to use as a fallback in case the properties cannot be
     *            loaded in the global.conf file.
     * @return An {@link AmqpServerUri} from the global configuration if it is
     *         non-empty, otherwise the default.
     * @throws URISyntaxException
     */
    public static AmqpServerUri createFromGlobalConf(Properties globalProps,
            String defaultUri) throws URISyntaxException {
        final String uri;
        if (globalProps.isEmpty()) {
            uri = defaultUri;
        } else {
            String scheme = SCHEME_AMQP;
            int port = DEFAULT_PORT;
            if ("1".equals(globalProps.getProperty("amqpusessl"))) {
                scheme = SCHEME_AMQPS;
                port = DEFAULT_SSL_PORT;
            }
            String host = globalProps.getProperty("amqphost", "localhost");
            String user = globalProps.getProperty("amqpuser", "");
            String pass = globalProps.getProperty("amqppassword", "");
            String vhost = globalProps.getProperty("amqpvhost", "/");
            String strPort = globalProps.getProperty("amqpport");
            if (strPort != null) {
                try {
                    port = Integer.valueOf(strPort);
                } catch (NumberFormatException nfe) {
                    logger.warn("Invalid port number: {}", strPort);
                }
            }
            StringBuilder uriBuilder = new StringBuilder();
            uriBuilder.append(scheme).append("://");
            if (!user.isEmpty()) {
                uriBuilder.append(user);
                if (!pass.isEmpty()) {
                    uriBuilder.append(':').append(pass);
                }
                uriBuilder.append('@');
            }
            uriBuilder.append(host).append(':').append(port).append(vhost);
            uri = uriBuilder.toString();
        }
        return new AmqpServerUri(uri);
    }
}
