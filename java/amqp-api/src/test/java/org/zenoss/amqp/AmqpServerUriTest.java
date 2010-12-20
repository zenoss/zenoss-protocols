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
package org.zenoss.amqp;

import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class AmqpServerUriTest {
	@Test
	public void testConstructor() throws URISyntaxException {
		// Test with invalid scheme
		try {
			new AmqpServerUri("http://foo");
			fail("Expected to fail with invalid scheme");
		} catch (IllegalArgumentException e) {
			// Expected
		}

		// Test that amqp scheme is non-ssl
		assertFalse(new AmqpServerUri("amqp://localhost/vhost").isSsl());

		// Test that amqps scheme is ssl
		assertTrue(new AmqpServerUri("amqps://localhost/vhost").isSsl());

		// Test hostname
		assertEquals("myhost1.hostname.com", new AmqpServerUri(
				"amqp://myhost1.hostname.com/vhost").getHostname());

		// Test port
		assertEquals(AmqpServerUri.DEFAULT_PORT, new AmqpServerUri(
				"amqp://myhost/exch").getPort());
		assertEquals(8211,
				new AmqpServerUri("amqp://myhost:8211/vhost").getPort());

		// Test username
		assertEquals("root",
				new AmqpServerUri("amqp://root@myhost/vhost").getUsername());

		// Test password
		assertNull(new AmqpServerUri("amqp://root@myhost/vhost").getPassword());
		assertArrayEquals("mypass".toCharArray(), new AmqpServerUri(
				"amqp://root:mypass@myhost/vhost").getPassword());

		// Test vhost
		assertEquals("/",
				new AmqpServerUri("amqp://myhost1.hostname.com").getVhost());
		assertEquals("/",
				new AmqpServerUri("amqp://myhost1.hostname.com/").getVhost());
		assertEquals("/vhost_name", new AmqpServerUri(
				"amqp://myhost1.hostname.com/vhost_name").getVhost());

		// Test exception on too many path components
		try {
			new AmqpServerUri("amqp://hostname/vhost/exchange/something_else");
			fail("Expected exception on too many path components");
		} catch (IllegalArgumentException e) {
			// Expected
		}

		Map<String, String> properties = new HashMap<String, String>();
		properties.put("insist", "True");
		properties.put("queue", "myqueue");
		properties.put("queue_durable", "False");
		properties.put("queue_exclusive", "True");
		properties.put("queue_autodelete", "True");
		properties.put("queue_passive", "False");
		properties.put("exchange_type", "topic");
		properties.put("exchange_durable", "True");
		properties.put("exchange_autodelete", "False");
		properties.put("exchange_passive", "False");
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			if (sb.length() > 0) {
				sb.append(';');
			}
			sb.append(entry.getKey()).append('=').append(entry.getValue());
		}
		assertEquals(properties, new AmqpServerUri("amqp://localhost/vhost?"
				+ sb.toString()).getProperties());
		// Test ability to ignore empty properties
		assertEquals(properties, new AmqpServerUri("amqp://localhost/vhost?"
				+ sb.toString() + ";=;=value;").getProperties());
	}
}
