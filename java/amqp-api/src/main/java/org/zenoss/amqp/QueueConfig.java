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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

/**
 * Parser for reading in a queue configuration file (*.qjs). Queue configuration
 * files are stored in JSON format and are used to interoperate between Java and
 * Python code using the AMQP queues, exchanges, and bindings.
 */
public class QueueConfig {
    private static final Logger logger = LoggerFactory
            .getLogger(QueueConfig.class);

    private final Map<String, Message> contentTypeMap = new LinkedHashMap<String, Message>();
    private final Map<String, ExchangeConfiguration> exchangeConfigMap = new LinkedHashMap<String, ExchangeConfiguration>();
    private final Map<String, QueueConfiguration> queueConfigMap = new LinkedHashMap<String, QueueConfiguration>();

    /**
     * Creates a queue configuration from the specified file.
     * 
     * @param file
     *            File to read the queue configuration from.
     * @throws IOException
     */
    public QueueConfig(File file) throws IOException {
        BufferedInputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    /**
     * Creates a queue configuration from the specified input stream.
     * 
     * @param is
     *            Input stream.
     * @throws IOException
     */
    public QueueConfig(InputStream is) throws IOException {
        load(is);
    }

    protected void load(InputStream is) throws IOException {
        JsonFactory jsonFactory = new MappingJsonFactory();
        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(is);
            JsonNode node = parser.readValueAsTree();
            if (!node.isObject()) {
                throw new IOException("Malformed queue configuration");
            }
            JsonNode contentTypesNode = node.get("content_types");
            if (contentTypesNode != null) {
                parseContentTypes(contentTypesNode);
            }
            JsonNode exchangesNode = node.get("exchanges");
            if (exchangesNode != null) {
                parseExchanges(exchangesNode);
            }
            JsonNode queuesNode = node.get("queues");
            if (queuesNode != null) {
                parseQueues(queuesNode);
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private void parseContentTypes(JsonNode contentTypesNode)
            throws IOException {
        if (!contentTypesNode.isObject()) {
            throw new IOException("Content types not a JSON object");
        }
        ObjectNode contentTypes = (ObjectNode) contentTypesNode;
        for (Iterator<Map.Entry<String, JsonNode>> it = contentTypes
                .getFields(); it.hasNext();) {
            Map.Entry<String, JsonNode> entry = it.next();
            String contentTypeId = entry.getKey();
            JsonNode contentType = entry.getValue();
            String javaClass = contentType.get("java_class").getTextValue();
            try {
                Class<?> clazz = Class.forName(javaClass);
                if (Message.class.isAssignableFrom(clazz)) {
                    Method method = clazz.getMethod("getDefaultInstance");
                    Message defaultInstance = (Message) method.invoke(null);
                    this.contentTypeMap.put(contentTypeId, defaultInstance);
                } else {
                    logger.warn("Class is not a protobuf Message: {}",
                            javaClass);
                }
            } catch (ClassNotFoundException e) {
                logger.warn("Invalid content_type", e);
            } catch (SecurityException e) {
                logger.warn("Invalid content_type", e);
            } catch (NoSuchMethodException e) {
                logger.warn("Invalid content_type", e);
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid content_type", e);
            } catch (IllegalAccessException e) {
                logger.warn("Invalid content_type", e);
            } catch (InvocationTargetException e) {
                logger.warn("Invalid content_type", e);
            }
        }
    }

    private void parseExchanges(JsonNode node) throws IOException {
        if (!node.isObject()) {
            throw new IOException("Exchanges not a JSON object");
        }
        ObjectNode exchangesObject = (ObjectNode) node;
        for (Iterator<Map.Entry<String, JsonNode>> it = exchangesObject
                .getFields(); it.hasNext();) {
            final Map.Entry<String, JsonNode> entry = it.next();
            final String exchangeId = entry.getKey();
            final JsonNode exchangeObject = entry.getValue();
            if (!exchangeObject.isObject()) {
                throw new IOException("Exchange value is not a JSON object");
            }

            final String name = exchangeObject.get("name").getTextValue();

            final JsonNode typeNode = exchangeObject.get("type");
            Exchange.Type type = Exchange.Type.FANOUT;
            if (typeNode != null) {
                type = Exchange.Type.fromName(typeNode.getTextValue());
                if (type == null) {
                    logger.warn("Unknown exchange type: {}",
                            typeNode.getTextValue());
                    type = Exchange.Type.FANOUT;
                }
            }

            boolean durable = getJsonBoolean(exchangeObject, "durable", false);
            boolean autoDelete = getJsonBoolean(exchangeObject, "auto_delete",
                    false);

            Exchange exchange = new Exchange(name, type, durable, autoDelete);
            List<Message> messages = new ArrayList<Message>();
            final JsonNode contentTypesNode = exchangeObject
                    .get("content_types");
            if (contentTypesNode != null) {
                for (JsonNode contentType : contentTypesNode) {
                    Message message = this.contentTypeMap.get(contentType
                            .getTextValue());
                    if (message == null) {
                        logger.warn("Unknown content type: {}", contentType);
                    } else {
                        messages.add(message);
                    }
                }
            }
            this.exchangeConfigMap.put(exchangeId, new ExchangeConfiguration(
                    exchange, messages));
        }
    }

    private void parseQueues(JsonNode node) throws IOException {
        if (!node.isObject()) {
            throw new IOException("Queues not a JSON object");
        }
        ObjectNode queuesObject = (ObjectNode) node;
        for (Iterator<Map.Entry<String, JsonNode>> it = queuesObject
                .getFields(); it.hasNext();) {
            Map.Entry<String, JsonNode> entry = it.next();
            String queueId = entry.getKey();
            JsonNode queueNode = entry.getValue();
            if (!queueNode.isObject()) {
                throw new IOException("Queue value is not a JSON object");
            }

            String name = queueNode.get("name").getTextValue();

            boolean durable = getJsonBoolean(queueNode, "durable", false);
            boolean exclusive = getJsonBoolean(queueNode, "exclusive", false);
            boolean autoDelete = getJsonBoolean(queueNode, "auto_delete", false);

            Queue queue = new Queue(name, durable, exclusive, autoDelete);

            Map<String, Message> messages = new LinkedHashMap<String, Message>();
            List<Binding> bindings = new ArrayList<Binding>();
            JsonNode bindingsNode = queueNode.get("bindings");
            if (bindingsNode != null) {
                for (JsonNode bindingNode : bindingsNode) {
                    String exchangeId = bindingNode.get("exchange")
                            .getTextValue();
                    String routingKey = bindingNode.get("routing_key")
                            .getTextValue();
                    ExchangeConfiguration exchangeConfig = this.exchangeConfigMap
                            .get(exchangeId);
                    if (exchangeConfig == null) {
                        logger.warn("Unknown exchange: {}", exchangeId);
                    } else {
                        bindings.add(new Binding(queue, exchangeConfig
                                .getExchange(), routingKey));
                        for (Message message : exchangeConfig.getMessages()) {
                            messages.put(message.getDescriptorForType()
                                    .getFullName(), message);
                        }
                    }

                }
            }
            this.queueConfigMap.put(queueId, new QueueConfiguration(queue,
                    bindings, messages.values()));
        }
    }

    /**
     * Returns the queue configuration with the specified identifier.
     * 
     * @param identifier
     *            Identifier for queue configuration (i.e. $RawZenEvents).
     * @return The queue configuration, or null if not found.
     */
    public QueueConfiguration getQueue(String identifier) {
        QueueConfiguration config = this.queueConfigMap.get(identifier);
        if (config==null) {
            logger.warn("Unknown queue: {}", identifier);
        }
        return config;
    }

    /**
     * Returns the exchange configuration with the specified identifier.
     * 
     * @param identifier
     *            Identifier for exchange configuration (i.e. $RawZenEvents).
     * @return The exchange configuration, or null if not found.
     */
    public ExchangeConfiguration getExchange(String identifier) {
        ExchangeConfiguration config = this.exchangeConfigMap.get(identifier);
        if (config==null) {
            logger.warn("Unknown exchange: {}", identifier);
        }
        return config;
    }

    private static boolean getJsonBoolean(JsonNode parent, String key,
            boolean defaultValue) {
        boolean value = defaultValue;
        JsonNode node = parent.get(key);
        if (node != null) {
            value = node.getBooleanValue();
        }
        return value;
    }
}
