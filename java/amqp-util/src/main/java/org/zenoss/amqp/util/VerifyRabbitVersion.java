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
package org.zenoss.amqp.util;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;

/**
 * Command-line program used to verify connectivity to a RabbitMQ server and
 * verify the version number is at an acceptable level.
 */
public class VerifyRabbitVersion {

    private static Logger logger = Logger.getLogger(VerifyRabbitVersion.class
            .getName());
    private static final String CLASSNAME = VerifyRabbitVersion.class.getName();

    public static void main(String[] args) {
        Options options = new Options();
        {
            Option option = new Option("u", "user", true,
                    "RabbitMQ User Name (default: 'guest')");
            option.setRequired(false);
            options.addOption(option);
        }
        {
            Option option = new Option("p", "pass", true,
                    "RabbitMQ Password (default: 'guest')");
            option.setRequired(false);
            options.addOption(option);
        }
        {
            Option option = new Option("h", "host", true,
                    "RabbitMQ Hostname (default: 'localhost')");
            option.setRequired(false);
            options.addOption(option);
        }
        {
            Option option = new Option("P", "port", true, "RabbitMQ Port");
            option.setRequired(false);
            options.addOption(option);
        }
        {
            Option option = new Option("s", "ssl", false,
                    "Use SSL (default: 'false')");
            option.setRequired(false);
            options.addOption(option);
        }
        {
            Option option = new Option("v", "vhost", true,
                    "RabbitMQ VHost (default: '/')");
            option.setRequired(false);
            options.addOption(option);
        }

        Version requiredVersion = null;
        GnuParser parser = new GnuParser();
        CommandLine cl = null;
        try {
            cl = parser.parse(options, args);
            requiredVersion = Version.valueOf(cl.getArgs()[0]);
        } catch (Exception e) {
            HelpFormatter fmt = new HelpFormatter();
            fmt.printHelp(VerifyRabbitVersion.class.getName()
                    + " <version> [...]", options);
            System.exit(1);
        }

        String host = cl.getOptionValue("host", "localhost");
        String user = cl.getOptionValue("user", "guest");
        String pass = cl.getOptionValue("pass", "guest");
        String port = cl.getOptionValue("port");
        boolean isSsl = cl.hasOption("ssl");
        String scheme = isSsl ? AmqpServerUri.SCHEME_AMQPS
                : AmqpServerUri.SCHEME_AMQP;
        String vhost = cl.getOptionValue("vhost", "/");
        StringBuilder uri = new StringBuilder();
        uri.append(scheme).append("://").append(user).append(':').append(pass)
                .append('@').append(host);
        if (port != null) {
            uri.append(':').append(port);
        }
        uri.append(vhost);
        AmqpServerUri amqpUri = null;
        try {
            amqpUri = new AmqpServerUri(uri.toString());
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
        try {
            logger.fine("Connecting to: " + amqpUri);
            Version rabbitVersion = testConnection(amqpUri);
            if (rabbitVersion.compareTo(requiredVersion) < 0) {
                System.err.printf(
                        "RabbitMQ version (%s) < required version (%s)%n",
                        rabbitVersion, requiredVersion);
                System.exit(2);
            }
        } catch (AmqpException e) {
            logger.logp(Level.FINE, CLASSNAME, "main", "exception:", e);
            System.err.println("Failed to connect to RabbitMQ server at: "
                    + amqpUri);
            System.exit(1);
        } catch (Exception e) {
            logger.logp(Level.FINE, CLASSNAME, "main", "exception:", e);
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }

    private static Version testConnection(AmqpServerUri uri)
            throws AmqpException, IOException {
        Connection conn = null;
        try {
            conn = ConnectionFactory.newInstance().newConnection(uri);
            Map<String, Object> props = conn.getServerProperties();
            return Version.valueOf(props.get("version").toString());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e) {
                }
            }
        }

    }
}
