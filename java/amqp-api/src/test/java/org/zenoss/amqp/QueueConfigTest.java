package org.zenoss.amqp;

import org.junit.Test;
import org.zenoss.amqp.Exchange.Type;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Unit tests for QueueConfig.
 */
public class QueueConfigTest {

    private QueueConfig loadQueueConfig(String resourcePath) throws IOException {
        InputStream is = null;
        try {
            is = QueueConfigTest.class.getResourceAsStream(resourcePath);
            return new QueueConfig(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    @Test
    public void testReadArguments() throws IOException {
        QueueConfig queueConfig = loadQueueConfig("/sample.qjs");
        ExchangeConfiguration exchangeConfiguration = queueConfig.getExchange("$HeadersExchange");
        assertEquals(0, exchangeConfiguration.getMessages().size());
        assertEquals("zenoss.test.headers", exchangeConfiguration.getExchange().getName());
        assertEquals(Exchange.Type.HEADERS, exchangeConfiguration.getExchange().getType());
        assertTrue(exchangeConfiguration.getExchange().isDurable());
        assertFalse(exchangeConfiguration.getExchange().isAutoDelete());
        Map<String,Object> arguments = exchangeConfiguration.getExchange().getArguments();
        assertEquals("val1", arguments.get("arg1"));
        assertEquals("val1_2", arguments.get("arg1_2"));
        assertEquals(Boolean.FALSE, arguments.get("arg2"));
        assertEquals(Boolean.TRUE, arguments.get("arg2_2"));
        assertEquals((byte) 5, arguments.get("arg3"));
        assertArrayEquals(new byte[] { (byte) 0xfa, (byte) 0xfb, (byte) 0xfc, (byte) 0xfd },
                (byte[])arguments.get("arg4"));
        assertEquals((short) 5, arguments.get("arg5"));
        assertEquals(100, arguments.get("arg6"));
        assertEquals(200, arguments.get("arg6_1"));
        assertEquals(1000L, arguments.get("arg7"));
        assertEquals(2147483648L, arguments.get("arg8"));
        assertEquals(1.0f, arguments.get("arg9"));
        assertEquals(1.0d, arguments.get("arg10"));
        assertEquals(Double.MAX_VALUE, arguments.get("arg10_1"));
        assertEquals(new BigDecimal("3.14"), arguments.get("arg11"));
        assertEquals(Arrays.asList((byte)1, "string1"), arguments.get("arg12"));
        assertEquals(Arrays.asList((byte)1, "string1"), arguments.get("arg12_1"));
        assertEquals(new Date(1311869517L), arguments.get("arg13"));
        Map<String,Object> m = new HashMap<String, Object>();
        m.put("table_key1", Boolean.TRUE);
        m.put("table_key2", "string2");
        assertEquals(m, arguments.get("arg14"));
        assertEquals(m, arguments.get("arg14_1"));
        assertNull(arguments.get("arg15"));
    }

    @Test
    public void testExchangeReplacements() throws IOException {
        QueueConfig queueConfig = loadQueueConfig("/sample.qjs");

        Map<String,String> replacements = new HashMap<String, String>();
        replacements.put("exchange_uuid", UUID.randomUUID().toString());
        replacements.put("exchange_name", "myname");
        replacements.put("exchange_value", UUID.randomUUID().toString());

        ExchangeConfiguration exchangeConfiguration = queueConfig.getExchange("$ReplacementExchange", replacements);

        Exchange exchange = exchangeConfiguration.getExchange();
        assertEquals("zenoss.exchanges." + replacements.get("exchange_uuid"), exchange.getName());
        assertEquals(1, exchange.getArguments().size());
        assertEquals("my argument " + replacements.get("exchange_value"), exchange.getArguments().get("arg_" +
                replacements.get("exchange_name")));
        assertTrue(exchange.isDurable());
        assertFalse(exchange.isAutoDelete());
        assertTrue(exchangeConfiguration.getMessages().isEmpty());
    }

    @Test
    public void testQueueReplacements() throws IOException {
        QueueConfig queueConfig = loadQueueConfig("/sample.qjs");

        Map<String,String> replacements = new HashMap<String, String>();
        replacements.put("queue_uuid", UUID.randomUUID().toString());
        replacements.put("arg5", UUID.randomUUID().toString());
        replacements.put("arg6", UUID.randomUUID().toString());
        replacements.put("queue_name", UUID.randomUUID().toString());
        replacements.put("arg7", UUID.randomUUID().toString());
        replacements.put("arg8", UUID.randomUUID().toString());
        replacements.put("key", UUID.randomUUID().toString());
        replacements.put("name", UUID.randomUUID().toString());
        replacements.put("arg1", UUID.randomUUID().toString());
        replacements.put("arg2", UUID.randomUUID().toString());
        replacements.put("exchange_uuid", UUID.randomUUID().toString());
        replacements.put("exchange_name", UUID.randomUUID().toString());
        replacements.put("exchange_value", UUID.randomUUID().toString());

        QueueConfiguration queueConfiguration = queueConfig.getQueue("$ReplacementQueue", replacements);
        Queue queue = queueConfiguration.getQueue();
        assertEquals("zenoss.queues." + replacements.get("queue_uuid"), queue.getName());
        assertTrue(queue.isDurable());
        assertFalse(queue.isExclusive());
        assertFalse(queue.isAutoDelete());
        assertEquals(2, queue.getArguments().size());
        assertEquals(String.format("my %s and %s", replacements.get("arg5"), replacements.get("arg6")),
                queue.getArguments().get("arg1"));
        assertEquals(String.format("my %s and %s", replacements.get("arg7"), replacements.get("arg8")),
                queue.getArguments().get("queue_arg_" + replacements.get("queue_name")));
        assertEquals(1, queueConfiguration.getBindings().size());
        Binding binding = queueConfiguration.getBindings().get(0);
        assertEquals("zenoss.events." + replacements.get("key"), binding.getRoutingKey());
        assertEquals(1, binding.getArguments().size());
        assertEquals(String.format("my binding argument %s and %s", replacements.get("arg1"), replacements.get("arg2")),
                binding.getArguments().get("binding_arg" + replacements.get("name")));

        Exchange exchange = binding.getExchange();

        assertEquals("zenoss.exchanges." + replacements.get("exchange_uuid"), exchange.getName());
        assertEquals(Type.TOPIC, exchange.getType());
        assertTrue(exchange.isDurable());
        assertFalse(exchange.isAutoDelete());
        assertEquals(1, exchange.getArguments().size());
        assertEquals("my argument " + replacements.get("exchange_value"),
                exchange.getArguments().get("arg_" + replacements.get("exchange_name")));
    }
}
