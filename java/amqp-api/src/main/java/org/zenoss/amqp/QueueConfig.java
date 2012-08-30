/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/


package org.zenoss.amqp;

import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.amqp.Exchange.Compression;
import org.zenoss.amqp.Exchange.Type;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for reading in a queue configuration file (*.qjs). Queue configuration
 * files are stored in JSON format and are used to interoperate between Java and
 * Python code using the AMQP queues, exchanges, and bindings.
 */
public class QueueConfig {
    private static final Logger logger = LoggerFactory.getLogger(QueueConfig.class);

    /**
     * Contains parsed configuration for an exchange from QJS file.
     */
    private static class ExchangeNode {
        private String identifier;
        private String name;
        private Type type;
        private boolean durable;
        private boolean autoDelete;
        private String description;
        private Set<String> contentTypeIds = new LinkedHashSet<String>();
        private Map<String, Object> arguments;

        @Override
        public String toString() {
            return "ExchangeNode{" +
                    "identifier='" + identifier + '\'' +
                    ", name='" + name + '\'' +
                    ", type=" + type +
                    ", durable=" + durable +
                    ", autoDelete=" + autoDelete +
                    ", description='" + description + '\'' +
                    ", contentTypeIds=" + contentTypeIds +
                    ", arguments=" + arguments +
                    '}';
        }
    }

    /**
     * Contains parsed configuration for a binding from QJS file.
     */
    private static class BindingNode {
        private String exchangeIdentifier;
        private String routingKey;
        private Map<String, Object> arguments;

        @Override
        public String toString() {
            return "BindingNode{" +
                    "exchangeIdentifier='" + exchangeIdentifier + '\'' +
                    ", routingKey='" + routingKey + '\'' +
                    ", arguments=" + arguments +
                    '}';
        }
    }

    /**
     * Contains parsed configuration for a queue from QJS file.
     */
    private static class QueueNode {
        private String identifier;
        private String name;
        private boolean durable;
        private boolean exclusive;
        private boolean autoDelete;
        private String description;
        private Map<String, Object> arguments;
        private List<BindingNode> bindingNodes = new ArrayList<BindingNode>();

        @Override
        public String toString() {
            return "QueueNode{" +
                    "identifier='" + identifier + '\'' +
                    ", name='" + name + '\'' +
                    ", durable=" + durable +
                    ", exclusive=" + exclusive +
                    ", autoDelete=" + autoDelete +
                    ", description='" + description + '\'' +
                    ", arguments=" + arguments +
                    ", bindingNodes=" + bindingNodes +
                    '}';
        }
    }

    private final Map<String, String> contentTypeIdToJavaClass = new LinkedHashMap<String, String>();
    private final Map<String, ExchangeNode> exchangesById = new LinkedHashMap<String, ExchangeNode>();
    private final Map<String, QueueNode> queuesById = new LinkedHashMap<String, QueueNode>();
    private final MessagingProperties properties = new MessagingProperties();

    /**
     * Creates a queue configuration from the specified file.
     *
     * @param file File to read the queue configuration from.
     * @throws IOException If an error occurs parsing the file.
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
     * @param inputStream Input stream the contents of which should be parsed
     * @throws IOException If an error occurs parsing the input streams.
     */
    public QueueConfig(InputStream inputStream) throws IOException {
        load(inputStream);
    }

    /**
     * Creates a queue configuration from the specified input streams.
     *
     * @param inputStreams Input streams to be parsed.
     * @throws IOException If an error occurs parsing the input streams.
     */
    public QueueConfig(List<InputStream> inputStreams) throws IOException {
        for (InputStream is : inputStreams) {
            load(is);
        }
    }

    public void loadProperties(InputStream is) throws IOException {
        properties.load(is);
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
        for (Iterator<Map.Entry<String, JsonNode>> it = contentTypes.getFields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            String contentTypeId = entry.getKey();
            JsonNode contentType = entry.getValue();
            String javaClass = contentType.get("java_class").getTextValue();
            this.contentTypeIdToJavaClass.put(contentTypeId, javaClass);
        }
    }

    private Class<?> loadClass(String className) throws ClassNotFoundException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl != null) {
            try {
                return cl.loadClass(className);
            } catch (ClassNotFoundException cnfe) {
            }
        }
        return Class.forName(className);
    }

    private Message loadMessageFromContentTypeId(String contentTypeId) {
        String javaClass = this.contentTypeIdToJavaClass.get(contentTypeId);
        if (javaClass == null) {
            throw new IllegalStateException("Failed to find content type: " + contentTypeId);
        }
        try {
            Class<?> clazz = loadClass(javaClass);
            Method method = clazz.getMethod("getDefaultInstance");
            return (Message) method.invoke(null);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load java_class: " + javaClass, e);
        }
    }

    private void parseExchanges(JsonNode node) throws IOException {
        if (!node.isObject()) {
            throw new IOException("Exchanges not a JSON object");
        }
        ObjectNode exchangesObject = (ObjectNode) node;
        for (Iterator<Map.Entry<String, JsonNode>> it = exchangesObject.getFields(); it.hasNext(); ) {
            final Map.Entry<String, JsonNode> entry = it.next();

            final ExchangeNode exchangeNode = new ExchangeNode();
            exchangeNode.identifier = entry.getKey();
            final JsonNode exchangeObject = entry.getValue();
            if (!exchangeObject.isObject()) {
                throw new IOException("Exchange value is not a JSON object");
            }

            exchangeNode.name = exchangeObject.get("name").getTextValue();

            final JsonNode typeNode = exchangeObject.get("type");
            Type type = Exchange.Type.FANOUT;
            if (typeNode != null) {
                type = Exchange.Type.fromName(typeNode.getTextValue());
                if (type == null) {
                    throw new IOException("Unknown exchange type: " + typeNode.getTextValue());
                }
            }
            exchangeNode.type = type;
            exchangeNode.durable = getJsonBoolean(exchangeObject, "durable", false);
            exchangeNode.autoDelete = getJsonBoolean(exchangeObject, "auto_delete", false);
            exchangeNode.arguments = convertObjectNodeToMap(exchangeObject.get("arguments"));
            exchangeNode.description = getJsonString(exchangeObject, "description", null);

            final JsonNode contentTypesNode = exchangeObject.get("content_types");
            if (contentTypesNode != null) {
                for (JsonNode contentType : contentTypesNode) {
                    exchangeNode.contentTypeIds.add(contentType.getTextValue());
                }
            }

            this.exchangesById.put(exchangeNode.identifier, exchangeNode);
        }
    }

    private void parseQueues(JsonNode node) throws IOException {
        if (!node.isObject()) {
            throw new IOException("Queues not a JSON object");
        }
        ObjectNode queuesObject = (ObjectNode) node;
        for (Iterator<Map.Entry<String, JsonNode>> it = queuesObject.getFields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            QueueNode queue = new QueueNode();
            queue.identifier = entry.getKey();
            JsonNode queueNode = entry.getValue();
            if (!queueNode.isObject()) {
                throw new IOException("Queue value is not a JSON object");
            }

            queue.name = queueNode.get("name").getTextValue();
            queue.durable = getJsonBoolean(queueNode, "durable", false);
            queue.exclusive = getJsonBoolean(queueNode, "exclusive", false);
            queue.autoDelete = getJsonBoolean(queueNode, "auto_delete", false);
            queue.description = getJsonString(queueNode, "description", null);
            queue.arguments = convertObjectNodeToMap(queueNode.get("arguments"));

            JsonNode bindingsNode = queueNode.get("bindings");
            if (bindingsNode != null) {
                for (JsonNode bindingNode : bindingsNode) {
                    BindingNode binding = new BindingNode();
                    binding.exchangeIdentifier = bindingNode.get("exchange").getTextValue();
                    binding.routingKey = bindingNode.get("routing_key").getTextValue();
                    binding.arguments = convertObjectNodeToMap(bindingNode.get("arguments"));
                    queue.bindingNodes.add(binding);
                }
            }

            this.queuesById.put(queue.identifier, queue);
        }
    }

    /**
     * Returns the queue configuration with the specified identifier.
     *
     * @param identifier Identifier for queue configuration (i.e. $RawZenEvents).
     * @return The queue configuration, or null if not found.
     */
    public QueueConfiguration getQueue(String identifier) {
        return getQueue(identifier, Collections.<String, String>emptyMap());
    }

    /**
     * Returns the queue configuration with the specified identifier with all replacement values
     * substituted in the configuration with the specified values.
     *
     * @param identifier        Identifier for the queue configuration (i.e. $RawZenEvents).
     * @param replacementValues A map containing all of the replacement variables to be substituted in
     *                          the configuration.
     * @return The queue configuration, or null if not found.
     */
    public QueueConfiguration getQueue(String identifier, Map<String, String> replacementValues) {
        QueueNode queueNode = this.queuesById.get(identifier);
        if (queueNode == null) {
            logger.warn("Unknown queue: {}", identifier);
            return null;
        }

        // Replace name and arguments with replacement values
        String name = substituteReplacements(queueNode.name, replacementValues);
        Map<String, Object> arguments = substituteReplacementsInArguments(queueNode.arguments, replacementValues);

        String ttl = properties.getQueueProperty(identifier, "x-message-ttl", null);
        if (ttl != null) {
            arguments.put("x-message-ttl", Integer.parseInt(ttl));
        }

        String expires = properties.getQueueProperty(identifier, "x-expires", null);
        if (expires != null) {
            arguments.put("x-expires", Integer.parseInt(expires));
        }

        // Create new queue with replacements
        Queue queue = new Queue(name, queueNode.durable, queueNode.exclusive, queueNode.autoDelete, arguments);

        Map<String, Message> messagesById = new LinkedHashMap<String, Message>();
        List<Binding> replacedBindings = new ArrayList<Binding>(queueNode.bindingNodes.size());
        for (BindingNode bindingNode : queueNode.bindingNodes) {
            String routingKey = substituteReplacements(bindingNode.routingKey, replacementValues);
            Map<String, Object> bindingArguments = substituteReplacementsInArguments(bindingNode.arguments,
                    replacementValues);
            ExchangeConfiguration exchange = getExchange(bindingNode.exchangeIdentifier, replacementValues);
            for (Message message : exchange.getMessages()) {
                messagesById.put(message.getDescriptorForType().getFullName(), message);
            }
            Binding binding = new Binding(queue, exchange.getExchange(), routingKey, bindingArguments);
            replacedBindings.add(binding);
        }
        return new QueueConfiguration(queueNode.identifier, queue, replacedBindings, messagesById.values());
    }

    /**
     * Returns the exchange configuration with the specified identifier.
     *
     * @param identifier Identifier for exchange configuration (i.e. $RawZenEvents).
     * @return The exchange configuration, or null if not found.
     */
    public ExchangeConfiguration getExchange(String identifier) {
        return getExchange(identifier, Collections.<String, String>emptyMap());
    }

    /**
     * Returns the exchange configuration with the specified identifier with all replacement values
     * substituted in the configuration with the specified values.
     *
     * @param identifier        Identifier for the exchange configuration (i.e. $RawZenEvents).
     * @param replacementValues A map containing all of the replacement variables to be substituted in
     *                          the configuration.
     * @return The exchange configuration, or null if not found.
     */


    public ExchangeConfiguration getExchange(String identifier, Map<String, String> replacementValues) {
        ExchangeNode exchangeNode = this.exchangesById.get(identifier);
        if (exchangeNode == null) {
            logger.warn("Unknown exchange: {}", identifier);
            return null;
        }

        String name = substituteReplacements(exchangeNode.name, replacementValues);
        Map<String, Object> arguments = substituteReplacementsInArguments(exchangeNode.arguments, replacementValues);

        MessageDeliveryMode deliveryMode = MessageDeliveryMode.fromMode(
                Integer.parseInt(properties.getExchangeProperty(identifier, "delivery_mode", "2")));

        Compression compression;
        try {
            compression = Compression.valueOf(properties.getExchangeProperty(
                    identifier, "compression", "none").toUpperCase());
        } catch (IllegalArgumentException e) {
            // Invalid entry in config file.
            compression = Compression.NONE;
        }

        Exchange exchange = new Exchange(name, exchangeNode.type, exchangeNode.durable,
                exchangeNode.autoDelete, arguments, deliveryMode, compression);
        List<Message> messages = new ArrayList<Message>(exchangeNode.contentTypeIds.size());
        for (String messageId : exchangeNode.contentTypeIds) {
            messages.add(loadMessageFromContentTypeId(messageId));
        }
        return new ExchangeConfiguration(exchangeNode.identifier, exchange, messages);
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

    private static String getJsonString(JsonNode parent, String key, String defaultValue) {
        String value = defaultValue;
        JsonNode node = parent.get(key);
        if (node != null) {
            value = node.getTextValue();
        }
        return value;
    }

    /**
     * Converts an <code>arguments</code> JSON object or nested <code>table</code> argument type
     * to a Map suitable for passing to exchange.declare, queue.bind, queue.declare arguments
     * parameters.
     *
     * @param node The <code>arguments</code> JSON object or nested <code>table</code> argument.
     * @return A map of String->object converted from the JSON objects.
     * @throws IOException If an error occurs.
     */
    private static Map<String, Object> convertObjectNodeToMap(JsonNode node) throws IOException {
        if (node == null || !node.isObject()) {
            return Collections.emptyMap();
        }
        final ObjectNode objectNode = (ObjectNode) node;
        final Map<String, Object> map = new HashMap<String, Object>();
        final Iterator<Map.Entry<String, JsonNode>> it = objectNode.getFields();
        while (it.hasNext()) {
            final Map.Entry<String, JsonNode> entry = it.next();
            final String key = entry.getKey();
            final JsonNode value = entry.getValue();
            if (!value.isObject()) {
                logger.warn("Expected object, got {}", value);
                continue;
            }
            final Object converted = convertObjectNode((ObjectNode) value);
            map.put(key, converted);
        }
        return map;
    }

    /**
     * Converts an <code>array</code> argument type to a Java List suitable for passing as one
     * of the argument values to exchange.declare, queue.bind, queue.declare.
     *
     * @param arrayNode The array node pointing to the value of the array argument.
     * @return A list with all members converted to the appropriate types.
     * @throws IOException If an exception occurs parsing the nodes.
     */
    private static List<Object> convertArrayNodeToList(ArrayNode arrayNode) throws IOException {
        final List<Object> list = new ArrayList<Object>(arrayNode.size());
        for (Iterator<JsonNode> it = arrayNode.getElements(); it.hasNext(); ) {
            final JsonNode node = it.next();
            if (!node.isObject()) {
                throw new IllegalArgumentException("Invalid node: " + node);
            }
            list.add(convertObjectNode((ObjectNode) node));
        }
        return list;
    }

    /**
     * Converts argument values to the appropriate Java type. The supported types are:
     * <p/>
     * <ul>
     * <li>type: "boolean" = Boolean, value: true/false</li>
     * <li>type: "byte" = Byte, value: byte</li>
     * <li>type: "byte[]" = byte[], value: base-64 encoded string</li>
     * <li>type: "short" = Short, value: short</li>
     * <li>type: "int" = Integer, value: int</li>
     * <li>type: "long" = Long, value: long</li>
     * <li>type: "float" = Float, value: float</li>
     * <li>type: "double" = Double, value: double</li>
     * <li>type: "decimal" = BigDecimal, value: "decimal" or decimal</li>
     * <li>type: "string" = String, value: "string"</li>
     * <li>type: "array" = List, value: JSON array of JSON objects</li>
     * <li>type: "timestamp" = Date, value: timestamp (long integer)</li>
     * <li>type: "table" = Map, value: JSON object of JSON objects</li>
     * </ul>
     * <p/>
     * <p>If the type is not specified, type coercion is attempted for the following
     * values:</p>
     * <p/>
     * <ul>
     * <li>JSON boolean</li>
     * <li>JSON int (will convert to Integer if within range, otherwise Long)</li>
     * <li>JSON float (will convert to Float if within range, otherwise Double)</li>
     * <li>JSON array</li>
     * <li>JSON object</li>
     * <li>JSON decimal</li>
     * <li>JSON string</li>
     * </ul>
     *
     * @param node A JSON object with 'type' and 'value' optional attributes.
     * @return The value of the node coerced to the appropriate Java type.
     * @throws IOException If an exception occurs parsing the node.
     */
    private static Object convertObjectNode(ObjectNode node) throws IOException {
        final JsonNode typeNode = node.get("type");
        final JsonNode valueNode = node.get("value");

        // No value
        if (valueNode == null) {
            return null;
        }

        Object converted;
        final String type = (typeNode != null) ? typeNode.getTextValue() : null;
        if ("boolean".equals(type)) {
            converted = valueNode.getBooleanValue();
        } else if ("byte".equals(type)) {
            converted = (byte) valueNode.getIntValue();
        } else if ("byte[]".equals(type)) {
            converted = valueNode.getBinaryValue();
        } else if ("short".equals(type)) {
            converted = (short) valueNode.getIntValue();
        } else if ("int".equals(type)) {
            converted = valueNode.getIntValue();
        } else if ("long".equals(type)) {
            converted = valueNode.getLongValue();
        } else if ("float".equals(type)) {
            converted = (float) valueNode.getDoubleValue();
        } else if ("double".equals(type)) {
            converted = valueNode.getDoubleValue();
        } else if ("decimal".equals(type)) {
            if (valueNode.isTextual()) {
                converted = new BigDecimal(valueNode.getTextValue());
            } else {
                converted = valueNode.getDecimalValue();
            }
        } else if ("string".equals(type)) {
            converted = valueNode.getTextValue();
        } else if ("array".equals(type)) {
            converted = convertArrayNodeToList((ArrayNode) valueNode);
        } else if ("timestamp".equals(type)) {
            converted = new Date(valueNode.getLongValue());
        } else if ("table".equals(type)) {
            converted = convertObjectNodeToMap(valueNode);
        } else if (type == null) {
            // Coerce type
            if (valueNode.isNull()) {
                converted = null;
            } else if (valueNode.isBoolean()) {
                converted = valueNode.getBooleanValue();
            } else if (valueNode.isInt()) {
                converted = valueNode.getIntValue();
            } else if (valueNode.isLong()) {
                converted = valueNode.getLongValue();
            } else if (valueNode.isDouble()) {
                double dVal = valueNode.getDoubleValue();
                if (dVal >= Float.MIN_VALUE && dVal <= Float.MAX_VALUE) {
                    converted = (float) dVal;
                } else {
                    converted = dVal;
                }
            } else if (valueNode.isBinary()) {
                converted = valueNode.getBinaryValue();
            } else if (valueNode.isArray()) {
                converted = convertArrayNodeToList((ArrayNode) valueNode);
            } else if (valueNode.isObject()) {
                converted = convertObjectNodeToMap(valueNode);
            } else if (valueNode.isBigDecimal()) {
                converted = valueNode.getDecimalValue();
            } else if (valueNode.isTextual()) {
                converted = valueNode.getTextValue();
            } else {
                throw new IllegalArgumentException("Unable to coerce type: " + node);
            }
        } else {
            throw new IllegalArgumentException("Invalid type: " + type);
        }
        return converted;
    }

    private static final Pattern REPLACEMENT_PATTERN = Pattern.compile("\\{([^}]+)\\}");

    /**
     * Substitutes replacement strings in the name or value of the arguments with values found in the
     * replacement values map.
     *
     * @param arguments         A map of arguments (may contain strings to be replaced in either the name or value).
     * @param replacementValues A map of values to be replaced in the arguments.
     * @return The arguments with all replacement strings substituted with values found in the replacement
     *         values map.
     */
    private static Map<String, Object> substituteReplacementsInArguments(Map<String, Object> arguments,
                                                                         Map<String, String> replacementValues) {
        Map<String, Object> replaced = new HashMap<String, Object>(arguments.size());
        for (Map.Entry<String, Object> entry : arguments.entrySet()) {
            String argName = substituteReplacements(entry.getKey(), replacementValues);
            Object argValue = entry.getValue();
            if (argValue instanceof String) {
                argValue = substituteReplacements((String) argValue, replacementValues);
            }
            replaced.put(argName, argValue);
        }
        return replaced;
    }

    /**
     * Substitutes replacement strings in the template with values found in the replacement values map. For
     * example, a string containing <code>my {value}</code> when passed a map of "value" -> "name" will return
     * <code>my name</code>.
     *
     * @param template          A template which may contain values to be substituted from the replacement values map.
     * @param replacementValues A map of values to be replaced in the template.
     * @return A string with all of the replacement strings substituted with values from the replacement values map.
     */
    private static String substituteReplacements(String template, Map<String, String> replacementValues) {
        // Bypass costly string replacement if we don't contain replacement character
        if (template.indexOf('{') == -1 || template.indexOf('}') == -1) {
            return template;
        }
        final StringBuffer sb = new StringBuffer(template.length());
        final Matcher matcher = REPLACEMENT_PATTERN.matcher(template);
        while (matcher.find()) {
            final String replacementName = matcher.group(1);
            if (!replacementValues.containsKey(replacementName)) {
                throw new IllegalArgumentException("Missing replacement for: " + replacementName);
            }
            matcher.appendReplacement(sb, replacementValues.get(replacementName));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
