/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2012, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/
package org.zenoss.amqp.impl;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class CompressionTest {

    public static String makeStringOfLength(int length) {
        Random r = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) r.nextInt(128);
            // Skip control characters
            while (Character.isISOControl(c)) {
                c = (char) r.nextInt(128);
            }
            sb.append(c);
        }
        return sb.toString();
    }

    @Test
    public void testCompression() throws IOException {
        List<String> testStrings = Arrays.asList("abc", makeStringOfLength(8192));
        for (String testString : testStrings) {
            byte[] rawBytes = testString.getBytes("UTF-8");
            byte[] compressed = PublisherImpl.deflateCompress(rawBytes);
            assertFalse(Arrays.equals(rawBytes, compressed));
            assertArrayEquals(rawBytes, ConsumerImpl.deflateDecompress(compressed));
        }
    }
}
