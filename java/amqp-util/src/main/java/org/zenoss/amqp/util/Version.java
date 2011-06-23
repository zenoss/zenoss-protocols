/*
 * Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp.util;

/**
 * Version class which represents the version number of a RabbitMQ server.
 */
public class Version implements Comparable<Version> {

    private final int major, minor, micro;

    /**
     * Creates a new version with the specified major, minor, and micro
     * components.
     * 
     * @param major
     *            The major version.
     * @param minor
     *            The minor version.
     * @param micro
     *            The micro version.
     */
    public Version(int major, int minor, int micro) {
        if (major < 0 || minor < 0 || micro < 0) {
            throw new IllegalArgumentException();
        }
        this.major = major;
        this.minor = minor;
        this.micro = micro;
    }

    /**
     * Parses a version from the specified string
     * 
     * @param version
     *            The version string.
     * @return The parsed version.
     * @throws IllegalArgumentException
     *             If the version string is invalid.
     */
    public static Version valueOf(String version) {
        String[] components = version.split("\\.");
        if (components.length > 3) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
        int major = 0, minor = 0, micro = 0;
        try {
            for (int i = 0; i < components.length; i++) {
                final int intVal = Integer.parseInt(components[i]);
                if (i == 0) {
                    major = intVal;
                } else if (i == 1) {
                    minor = intVal;
                }
                if (i == 2) {
                    micro = intVal;
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
        return new Version(major, minor, micro);
    }

    @Override
    public int compareTo(Version o) {
        int diff = this.major - o.major;
        if (diff == 0) {
            diff = this.minor - o.minor;
            if (diff == 0) {
                diff = this.micro - o.micro;
            }
        }
        return diff;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = 37 * hash + this.major;
        hash = 37 * hash + this.minor;
        hash = 37 * hash + this.micro;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Version)) {
            return false;
        }
        Version other = (Version) obj;
        return this.major == other.major && this.minor == other.minor
                && this.micro == other.micro;
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d", this.major, this.minor, this.micro);
    }
}
