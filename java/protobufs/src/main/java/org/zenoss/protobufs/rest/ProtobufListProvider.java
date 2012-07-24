/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.protobufs.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zenoss.protobufs.JsonFormat;
import org.zenoss.protobufs.ProtobufConstants;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Converts a List of protobuf objects to a singly encoded, length-delimited
 * protobuf object.
 * 
 * @param <T>
 *            Message type.
 */
@Provider
@Produces({ ProtobufConstants.CONTENT_TYPE_PROTOBUF, MediaType.APPLICATION_JSON })
@Consumes({ ProtobufConstants.CONTENT_TYPE_PROTOBUF, MediaType.APPLICATION_JSON })
public class ProtobufListProvider<T extends Message> implements
        MessageBodyWriter<List<T>>, MessageBodyReader<List<T>> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory
            .getLogger(ProtobufListProvider.class);

    private ProtobufMessageRegistry messageRegistry;

    /**
     * Specifies the message registry to be used for decoding messages. Usually
     * injected automatically via DI framework.
     * 
     * @param messageRegistry
     *            Message registry to use to decode messages.
     */
    public void setMessageRegistry(ProtobufMessageRegistry messageRegistry) {
        this.messageRegistry = messageRegistry;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType) {
        return List.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(List<T> messages, Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType) {
        final int size;
        if (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            size = -1;
        } else {
            int totalSize = 0;
            for (Message message : messages) {
                final int serializedSize = message.getSerializedSize();
                totalSize += CodedOutputStream
                        .computeRawVarint32Size(serializedSize);
                totalSize += serializedSize;
            }
            size = totalSize;
        }
        return size;
    }

    @Override
    public void writeTo(List<T> messages, Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream entityStream) throws IOException,
            WebApplicationException {
        if (!messages.isEmpty()) {
            String fullName = messages.get(0).getDescriptorForType()
                    .getFullName();
            httpHeaders.add(ProtobufConstants.HEADER_PROTOBUF_FULLNAME,
                    fullName);
        }
        if (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            JsonFormat.writeAllDelimitedTo(messages, entityStream);
        } else {
            for (Message message : messages) {
                message.writeDelimitedTo(entityStream);
            }
        }
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType,
            Annotation[] annotations, MediaType mediaType) {
        if (type == List.class) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Type[] actualArguments = pt.getActualTypeArguments();
            if (actualArguments.length > 0) {
                Type protobufType = actualArguments[0];
                if (protobufType instanceof Class<?>) {
                    Class<?> protobufClass = (Class<?>) protobufType;
                    if (Message.class.isAssignableFrom(protobufClass)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> readFrom(Class<List<T>> type, Type genericType,
            Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException {
        String fullName = httpHeaders
                .getFirst(ProtobufConstants.HEADER_PROTOBUF_FULLNAME);
        if (fullName == null) {
            throw new IOException("Missing header: " + fullName);
        }
        Message message = messageRegistry.getMessageByFullName(fullName);
        if (message == null) {
            throw new IOException("Unsupported message: " + fullName);
        }
        final List<T> messages;
        if (mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            messages = JsonFormat.mergeAllDelimitedFrom(entityStream,
                    (T) message);
        } else {
            messages = new ArrayList<T>();
            Builder builder = message.newBuilderForType();
            while (builder.mergeDelimitedFrom(entityStream)) {
                messages.add((T) builder.build());
                builder = message.newBuilderForType();
            }
        }
        return messages;
    }
}
