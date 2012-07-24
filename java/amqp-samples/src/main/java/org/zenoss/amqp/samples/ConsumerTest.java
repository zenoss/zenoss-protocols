/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.amqp.samples;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.zenoss.amqp.AmqpException;
import org.zenoss.amqp.AmqpServerUri;
import org.zenoss.amqp.Channel;
import org.zenoss.amqp.Connection;
import org.zenoss.amqp.ConnectionFactory;
import org.zenoss.amqp.Consumer;
import org.zenoss.amqp.Message;
import org.zenoss.amqp.ProtobufConverter;
import org.zenoss.protobufs.zep.Zep.Event;
import org.zenoss.protobufs.zep.Zep.Signal;

public class ConsumerTest {

    public static void main(String[] args) throws AmqpException, InterruptedException, URISyntaxException {
        if (args.length < 1) {
            System.err.println("Usage: ConsumerTest <AmqpServerUri>");
            System.exit(1);
        }
        AmqpServerUri uri = new AmqpServerUri(args[0]);

        long delay = 60;
        Connection conn = null;
        Channel channel = null;
        try {
            conn = ConnectionFactory.newInstance().newConnection(uri);
            channel = conn.openChannel();
            channel.declareQueue(ProducerTest.QUEUE_EVENTS);
            Consumer<com.google.protobuf.Message> consumer = channel
                    .createConsumer(ProducerTest.QUEUE_EVENTS,
                            new ProtobufConverter(Event.getDefaultInstance(),
                                    Signal.getDefaultInstance()));
            Message<com.google.protobuf.Message> msg;
            while ((msg = consumer.nextMessage(delay, TimeUnit.SECONDS)) != null) {
                if (msg.getBody() instanceof Event) {
                    System.out.println("Received event: " + msg.getBody());
                } else if (msg.getBody() instanceof Signal) {
                    System.out.println("Received signal: " + msg.getBody());
                }
                consumer.ackMessage(msg);
            }
            System.out.printf(
                    "No messages received in last %ds, closing connection%n",
                    delay);
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
