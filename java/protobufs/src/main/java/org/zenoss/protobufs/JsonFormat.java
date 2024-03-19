/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.protobufs;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistry.ExtensionInfo;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Class which can serialize Google protobufs to JSON format.
 */
public class JsonFormat {

    private static final JsonFactory FACTORY = new JsonFactory();

    private JsonFormat() {
    }

    /**
     * Encodes the Google protobuf object to JSON format.
     * 
     * @param message
     *            The protobuf message to serialize.
     * @param output
     *            The output stream to store the serialized message.
     * @throws IOException
     *             If an exception occurs.
     */
    public static void writeTo(Message message, OutputStream output)
            throws IOException {
        JsonGenerator generator = null;
        try {
            generator = FACTORY.createJsonGenerator(output, JsonEncoding.UTF8);
            writeMessage(generator, message);
        } finally {
            if (generator != null) {
                generator.close();
            }
        }
    }

    /**
     * Encodes the Google protobuf object to JSON format.
     * 
     * @param message
     *            THe protobuf message to serialize.
     * @param writer
     *            The writer where the protobuf is stored.
     * @throws IOException
     *             If an exception occurs.
     */
    public static void writeTo(Message message, Writer writer)
            throws IOException {
        JsonGenerator generator = null;
        try {
            generator = FACTORY.createJsonGenerator(writer);
            writeMessage(generator, message);
        } finally {
            if (generator != null) {
                generator.close();
            }
        }
    }

    /**
     * Encodes the Google protobuf object to JSON format.
     * 
     * @param message
     *            THe protobuf message to serialize.
     * @return A JSON string of the encoded message.
     * @throws IOException
     *             If an exception occurs.
     */
    public static String writeAsString(Message message) throws IOException {
        JsonGenerator generator = null;
        StringWriter sw = new StringWriter();
        try {
            generator = FACTORY.createJsonGenerator(sw);
            writeMessage(generator, message);
        } finally {
            if (generator != null) {
                generator.close();
            }
        }
        return sw.toString();
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param is
     *            JSON input stream.
     * @param builder
     *            Message builder.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(InputStream is, Builder builder)
            throws IOException {
        return merge(is, builder, ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param is
     *            JSON input stream.
     * @param builder
     *            Message builder.
     * @param registry
     *            Extension registry.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(InputStream is, Builder builder,
            ExtensionRegistry registry) throws IOException {
        JsonParser jp = null;
        try {
            jp = FACTORY.createJsonParser(is);
            jp.nextToken();
            return readMessage(jp, builder, registry);
        } finally {
            if (jp != null) {
                jp.close();
            }
        }
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param reader
     *            JSON reader.
     * @param builder
     *            Message builder.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(Reader reader, Builder builder)
            throws IOException {
        return merge(reader, builder, ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param reader
     *            JSON reader.
     * @param builder
     *            Message builder.
     * @param registry
     *            Extension registry.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(Reader reader, Builder builder,
            ExtensionRegistry registry) throws IOException {
        JsonParser jp = null;
        try {
            jp = FACTORY.createJsonParser(reader);
            jp.nextToken();
            return readMessage(jp, builder, registry);
        } finally {
            if (jp != null) {
                jp.close();
            }
        }
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param json
     *            JSON string.
     * @param builder
     *            Message builder.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(String json, Builder builder)
            throws IOException {
        return merge(json, builder, ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Creates a protobuf message from the JSON input stream and message
     * builder.
     * 
     * @param json
     *            JSON string.
     * @param builder
     *            Message builder.
     * @param registry
     *            Extension registry.
     * @return The parsed protobuf message.
     * @throws IOException
     *             If an exception occurs reading a protobuf object from the
     *             JSON representation.
     */
    public static Message merge(String json, Builder builder,
            ExtensionRegistry registry) throws IOException {
        JsonParser jp = null;
        try {
            jp = FACTORY.createJsonParser(new StringReader(json));
            jp.nextToken();
            return readMessage(jp, builder, registry);
        } finally {
            if (jp != null) {
                jp.close();
            }
        }
    }

    private static Message readMessage(JsonParser jp, Builder builder,
            ExtensionRegistry registry) throws IOException {
        JsonToken tok = jp.getCurrentToken();
        Descriptor descriptor = builder.getDescriptorForType();
        if (tok != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT, found: " + tok);
        }
        while ((tok = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (tok != JsonToken.FIELD_NAME) {
                throw new IOException("Expected FIELD_NAME, found: " + tok);
            }
            final String fieldName = jp.getCurrentName();
            FieldDescriptor fieldDesc = descriptor.findFieldByName(fieldName);
            if (fieldDesc == null) {
                ExtensionInfo extensionInfo = registry.findExtensionByName(fieldName);
                if (extensionInfo != null) {
                    fieldDesc = extensionInfo.descriptor;
                }
            }

            if (fieldDesc != null) {
                /* Advance to value token */
                tok = jp.nextToken();
                if (fieldDesc.isRepeated()) {
                    if (tok != JsonToken.START_ARRAY) {
                        throw new IOException("Expected START_ARRAY, found: " + tok);
                    }
                    builder.setField(fieldDesc,
                            readRepeatable(jp, builder, registry, fieldDesc));
                } else {
                    builder.setField(fieldDesc,
                            readValue(jp, builder, registry, fieldDesc));
                }
            }
            else {
                /* Skip unknown field type */
                tok = jp.nextToken();
                if (tok == JsonToken.START_ARRAY || tok == JsonToken.START_OBJECT) {
                    jp.skipChildren();
                }
            }
        }
        return builder.build();
    }

    private static List<Object> readRepeatable(JsonParser jp, Builder builder,
            ExtensionRegistry registry, FieldDescriptor desc)
            throws IOException {
        List<Object> l = new ArrayList<Object>();
        while (jp.nextToken() != JsonToken.END_ARRAY) {
            l.add(readValue(jp, builder, registry, desc));
        }
        return l;
    }

    private static Object readValue(JsonParser jp, Builder builder,
            ExtensionRegistry registry, FieldDescriptor desc)
            throws IOException {
        final Object val;
        switch (desc.getJavaType()) {
        case BOOLEAN:
            val = jp.getBooleanValue();
            break;
        case BYTE_STRING:
            val = ByteString.copyFrom(jp.getBinaryValue());
            break;
        case DOUBLE:
            val = jp.getDoubleValue();
            break;
        case ENUM:
            EnumDescriptor enumDesc = desc.getEnumType();
            val = enumDesc.findValueByNumber(jp.getIntValue());
            break;
        case FLOAT:
            val = jp.getFloatValue();
            break;
        case INT:
            val = jp.getIntValue();
            break;
        case LONG:
            val = jp.getLongValue();
            break;
        case MESSAGE:
            val = readMessage(jp, builder.newBuilderForField(desc), registry);
            break;
        case STRING:
            val = jp.getText();
            break;
        default:
            throw new IllegalStateException("Unsupported java type: "
                    + desc.getType());
        }
        return val;
    }

    /**
     * Writes all of the specified messages in delimited format.
     * 
     * @param messages
     *            Messages to write.
     * @param output
     *            Output stream where messages are written.
     * @throws IOException
     *             If an exception occurs.
     */
    public static void writeAllDelimitedTo(
            Collection<? extends Message> messages, OutputStream output)
            throws IOException {
        JsonFormatDelimitedEncoder encoder = null;
        try {
            encoder = new JsonFormatDelimitedEncoder(output);
            for (Message message : messages) {
                encoder.writeDelimitedTo(message);
            }
        } finally {
            if (encoder != null) {
                encoder.close();
            }
        }
    }

    /**
     * Writes all of the specified messages in delimited format.
     * 
     * @param messages
     *            Messages to write.
     * @param writer
     *            Writer where messages are written.
     * @throws IOException
     *             If an exception occurs.
     */
    public static void writeAllDelimitedTo(
            Collection<? extends Message> messages, Writer writer)
            throws IOException {
        JsonFormatDelimitedEncoder encoder = null;
        try {
            encoder = new JsonFormatDelimitedEncoder(writer);
            for (Message message : messages) {
                encoder.writeDelimitedTo(message);
            }
        } finally {
            if (encoder != null) {
                encoder.close();
            }
        }
    }

    /**
     * Writes all of the specified messages in delimited format.
     * 
     * @param messages
     *            Messages to write.
     * @return A JSON encoded string.
     * @throws IOException
     *             If an exception occurs.
     */
    public static String writeAllDelimitedAsString(
            Collection<? extends Message> messages) throws IOException {
        JsonFormatDelimitedEncoder encoder = null;
        StringWriter sw = new StringWriter();
        try {
            encoder = new JsonFormatDelimitedEncoder(sw);
            for (Message message : messages) {
                encoder.writeDelimitedTo(message);
            }
        } finally {
            if (encoder != null) {
                encoder.close();
            }
        }
        return sw.toString();
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param is
     *            The input stream from which to read.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            InputStream is, T msg) throws IOException {
        return mergeAllDelimitedFrom(is, msg,
                ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param is
     *            The input stream from which to read.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @param registry
     *            The extension registry.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    @SuppressWarnings({"unchecked", "UnusedParameters"})
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            InputStream is, T msg, ExtensionRegistry registry)
            throws IOException {
        JsonFormatDelimitedDecoder decoder = null;
        try {
            decoder = new JsonFormatDelimitedDecoder(is, registry);
            List<T> messages = new ArrayList<T>();
            Message decoded;
            while ((decoded = decoder.mergeDelimitedFrom(msg)) != null) {
                messages.add((T) decoded);
            }
            return messages;
        } finally {
            if (decoder != null) {
                decoder.close();
            }
        }
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param reader
     *            The reader from which to read.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            Reader reader, T msg) throws IOException {
        return mergeAllDelimitedFrom(reader, msg,
                ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param reader
     *            The reader from which to read.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @param registry
     *            The extension registry.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    @SuppressWarnings({"unchecked", "UnusedParameters"})
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            Reader reader, T msg, ExtensionRegistry registry)
            throws IOException {
        JsonFormatDelimitedDecoder decoder = null;
        try {
            decoder = new JsonFormatDelimitedDecoder(reader, registry);
            List<T> messages = new ArrayList<T>();
            Message decoded;
            while ((decoded = decoder.mergeDelimitedFrom(msg)) != null) {
                messages.add((T) decoded);
            }
            return messages;
        } finally {
            if (decoder != null) {
                decoder.close();
            }
        }
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param json
     *            The encoded JSON string.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            String json, T msg) throws IOException {
        return mergeAllDelimitedFrom(json, msg,
                ExtensionRegistry.getEmptyRegistry());
    }

    /**
     * Reads all messages from a delimited stream into a list.
     * 
     * @param <T>
     *            The type of message to read.
     * @param json
     *            The encoded JSON string.
     * @param msg
     *            The message type (used to create a builder) to read.
     * @param registry
     *            The extension registry.
     * @return All delimited messages read from the stream.
     * @throws IOException
     *             If an error occurs reading from the stream.
     */
    @SuppressWarnings({"unchecked", "UnusedParameters"})
    public static <T extends Message> List<T> mergeAllDelimitedFrom(
            String json, T msg, ExtensionRegistry registry) throws IOException {
        JsonFormatDelimitedDecoder decoder = null;
        try {
            decoder = new JsonFormatDelimitedDecoder(new StringReader(json), registry);
            List<T> messages = new ArrayList<T>();
            Message decoded;
            while ((decoded = decoder.mergeDelimitedFrom(msg)) != null) {
                messages.add((T) decoded);
            }
            return messages;
        } finally {
            if (decoder != null) {
                decoder.close();
            }
        }
    }

    private static void writeMessage(JsonGenerator generator, Message message)
            throws IOException {
        generator.writeStartObject();
        Map<FieldDescriptor, Object> fields = message.getAllFields();
        for (Map.Entry<FieldDescriptor, Object> entry : fields.entrySet()) {
            FieldDescriptor key = entry.getKey();
            Object val = entry.getValue();
            final String name;
            if (key.isExtension()) {
                name = key.getFullName();
            }
            else {
                name = key.getName();
            }
            generator.writeFieldName(name);
            if (key.isRepeated()) {
                List<?> valList = (List<?>) val;
                generator.writeStartArray();
                for (Object v : valList) {
                    writeValue(generator, key.getJavaType(), v);
                }
                generator.writeEndArray();
            } else {
                writeValue(generator, key.getJavaType(), val);
            }
        }
        generator.writeEndObject();
    }

    private static void writeValue(JsonGenerator generator, JavaType type,
            Object val) throws IOException {
        switch (type) {
        case BOOLEAN:
            generator.writeBoolean((Boolean) val);
            break;
        case BYTE_STRING:
            /* TODO: Improve to use read-only byte buffer */
            generator.writeBinary(((ByteString) val).toByteArray());
            break;
        case DOUBLE:
            generator.writeNumber((Double) val);
            break;
        case ENUM:
            generator.writeNumber(((EnumValueDescriptor) val).getNumber());
            break;
        case FLOAT:
            generator.writeNumber((Float) val);
            break;
        case INT:
            generator.writeNumber((Integer) val);
            break;
        case LONG:
            generator.writeNumber((Long) val);
            break;
        case MESSAGE:
            writeMessage(generator, (Message) val);
            break;
        case STRING:
            generator.writeString((String) val);
            break;
        default:
            throw new IllegalStateException("Unsupported java type: " + type);
        }
    }

    private static class JsonFormatDelimitedEncoder implements Closeable {
        private final JsonGenerator generator;

        public JsonFormatDelimitedEncoder(OutputStream os) throws IOException {
            if (os == null) {
                throw new NullPointerException();
            }
            this.generator = FACTORY.createJsonGenerator(os, JsonEncoding.UTF8);
            this.generator.writeStartArray();
        }

        public JsonFormatDelimitedEncoder(Writer writer) throws IOException {
            if (writer == null) {
                throw new NullPointerException();
            }
            this.generator = FACTORY.createJsonGenerator(writer);
            this.generator.writeStartArray();
        }

        public void writeDelimitedTo(Message message) throws IOException {
            writeMessage(this.generator, message);
        }

        @Override
        public void close() throws IOException {
            if (this.generator != null) {
                this.generator.writeEndArray();
                this.generator.close();
            }
        }
    }

    private static class JsonFormatDelimitedDecoder implements Closeable {
        private final JsonParser parser;
        private final ExtensionRegistry registry;

        public JsonFormatDelimitedDecoder(InputStream is,
                ExtensionRegistry registry) throws IOException {
            if (is == null) {
                throw new NullPointerException();
            }
            this.parser = FACTORY.createJsonParser(is);
            this.registry = registry;
            JsonToken tok = this.parser.nextToken();
            if (tok != JsonToken.START_ARRAY) {
                throw new IOException("Expected START_ARRAY");
            }
        }

        public JsonFormatDelimitedDecoder(Reader reader,
                ExtensionRegistry registry) throws IOException {
            if (reader == null) {
                throw new NullPointerException();
            }
            this.parser = FACTORY.createJsonParser(reader);
            this.registry = registry;
            JsonToken tok = this.parser.nextToken();
            if (tok != JsonToken.START_ARRAY) {
                throw new IOException("Expected START_ARRAY");
            }
        }

        @SuppressWarnings("unchecked")
        public <T extends Message> T mergeDelimitedFrom(Message message)
                throws IOException {
            JsonToken tok = this.parser.nextToken();
            if (tok == null || tok == JsonToken.END_ARRAY) {
                return null;
            }
            return (T) readMessage(this.parser, message.newBuilderForType(),
                    this.registry);
        }

        @Override
        public void close() throws IOException {
            if (this.parser != null) {
                this.parser.close();
            }
        }
    }
}
