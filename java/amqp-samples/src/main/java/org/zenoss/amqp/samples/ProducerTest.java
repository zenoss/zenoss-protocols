/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp.samples;

import java.net.URISyntaxException;
import java.util.UUID;

import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Binding;
import org.zenoss.amqp.Channel;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;
import org.zenoss.amqp.Exchange;
import org.zenoss.amqp.Exchange.Type;
import org.zenoss.amqp.ProtobufConverter;
import org.zenoss.amqp.Publisher;
import org.zenoss.amqp.Queue;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.EventSummary;
import org.zenoss.protobufs.zep.Zep.Signal;

public class ProducerTest {

    public static final Exchange EXCHANGE_EVENTS = new Exchange("events",
            Type.FANOUT, false, true);
    public static final Queue QUEUE_EVENTS = new Queue("events", false, false,
            true);
    public static final Binding BINDING_ZENOSS_EVENTS = new Binding(
            QUEUE_EVENTS, EXCHANGE_EVENTS, "zenoss.events");

    public static void main(String[] args) throws URISyntaxException,
            AmqpException {
        if (args.length < 1) {
            System.err.println("Usage: ProducerTest <AmqpServerUri>");
            System.exit(1);
        }
        AmqpServerUri uri = new AmqpServerUri(args[0]);

        Connection conn = null;
        Channel channel = null;
        try {
            conn = ConnectionFactory.newInstance().newConnection(uri);
            channel = conn.openChannel();
            channel.declareExchange(EXCHANGE_EVENTS);
            channel.declareQueue(QUEUE_EVENTS);
            channel.bindQueue(BINDING_ZENOSS_EVENTS);
            Event.Builder eventBuilder = Event.newBuilder();
            eventBuilder.setUuid(UUID.randomUUID().toString());
            eventBuilder.setCreatedTime(System.currentTimeMillis());
            Event event = eventBuilder.build();
            EventSummary.Builder summaryBuilder = EventSummary.newBuilder();
            summaryBuilder.addOccurrence(event);
            EventSummary summary = summaryBuilder.build();

            Publisher<com.google.protobuf.Message> publisher = channel
                    .createPublisher(EXCHANGE_EVENTS,
                            new ProtobufConverter(Event.getDefaultInstance(),
                                    Signal.getDefaultInstance()));
            publisher.publish(event, BINDING_ZENOSS_EVENTS.getRoutingKey());

            Signal.Builder signalBuilder = Signal.newBuilder();
            signalBuilder.setEvent(summary);
            signalBuilder.setMessage("My Alert Message!");
            Signal signal = signalBuilder.build();
            publisher.publish(signal, BINDING_ZENOSS_EVENTS.getRoutingKey());
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
