/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.amqp;

import java.util.concurrent.Callable;

// TODO: Move ThreadRenamingCallable to a common utility project.

/**
 * Wraps a callable object and sets the thread name while the thread is executing.
 *
 * @param <T> The return type of the callable.
 */
public abstract class ThreadRenamingCallable<T> implements Callable<T> {
    private final String name;

    /**
     * Create a callable which will use the specified thread name when it executes.
     *
     * @param name The thread name.
     */
    public ThreadRenamingCallable(String name) {
        this.name = name;
    }

    /**
     * Method which must be implemented by subclasses to perform the actual computation.
     *
     * @return The result of the callable.
     * @throws Exception If an exception occurs.
     */
    protected abstract T doCall() throws Exception;

    @Override
    public final T call() throws Exception {
        final String previousThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(this.name);
        } catch (SecurityException e) {
            /* Ignored - can't set thread name */
        }
        try {
            return doCall();
        } finally {
            try {
                Thread.currentThread().setName(previousThreadName);
            } catch (SecurityException e) {
                /* Ignored - can't set thread name */
            }
        }
    }

    /**
     * Wraps an existing callable object in a ThreadRenamingCallable.
     *
     * @param name The name to give the thread.
     * @param callable The callable to wrap.
     * @param <T> The return type of the callable.
     * @return A wrapped callable object which will set the name of the thread during execution.
     */
    public static <T> Callable<T> wrap(final String name, final Callable<T> callable) {
        return new ThreadRenamingCallable<T>(name) {
            @Override
            protected T doCall() throws Exception {
                return callable.call();
            }
        };
    }
}
