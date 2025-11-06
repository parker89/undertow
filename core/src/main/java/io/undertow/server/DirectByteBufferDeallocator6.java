/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.undertow.server;

import io.undertow.UndertowLogger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link DirectByteBufferDeallocator6} Utility class used to free direct buffer memory.
 */
public final class DirectByteBufferDeallocator6 {

    private static final int DEALLOCATION_DELAY_MILLIS = 100;
    private static final boolean SUPPORTED;

    private static final Method cleaner;

    private static final Method cleanerClean;

    private static final ConcurrentLinkedQueue<QueuedByteBuffer>[] queues;
    private static final Lock[] queueLocks;
    private static final int queueCount;
    private static final long CLEANUP_INTERVAL_MILLIS = 1000; // Clean every 1 second

    private static final Unsafe UNSAFE;

    static {
        // Initialize sharded queues for better concurrency
        queueCount = Runtime.getRuntime().availableProcessors() * 2;
        @SuppressWarnings("unchecked")
        ConcurrentLinkedQueue<QueuedByteBuffer>[] tmpQueues = new ConcurrentLinkedQueue[queueCount];
        queues = tmpQueues;
        queueLocks = new ReentrantLock[queueCount];
        for (int i = 0; i < queueCount; i++) {
            queues[i] = new ConcurrentLinkedQueue<>();
            queueLocks[i] = new ReentrantLock();
        }
        String versionString = System.getProperty("java.specification.version");
        if (versionString.equals("0.9")) {
            // android hardcoded
            versionString = "11";
        } else if (versionString.startsWith("1.")) {
            versionString = versionString.substring(2);
        }
        int version = Integer.parseInt(versionString);

        Method tmpCleaner = null;
        Method tmpCleanerClean = null;
        boolean supported;
        Unsafe tmpUnsafe = null;
        if (version < 9) {
            try {
                tmpCleaner = getAccesibleMethod("java.nio.DirectByteBuffer", "cleaner");
                tmpCleanerClean = getAccesibleMethod("sun.misc.Cleaner", "clean");
                supported = true;
            } catch (Throwable t) {
                UndertowLogger.ROOT_LOGGER.directBufferDeallocatorInitializationFailed(t);
                supported = false;
            }
        } else {
            try {
                tmpUnsafe = getUnsafe();
                tmpCleanerClean = getDeclaredMethod(tmpUnsafe, "invokeCleaner", ByteBuffer.class);
                supported = true;
            } catch (Throwable t) {
                UndertowLogger.ROOT_LOGGER.directBufferDeallocatorInitializationFailed(t);
                supported = false;
            }
        }
        SUPPORTED = supported;
        cleaner = tmpCleaner;
        cleanerClean = tmpCleanerClean;
        UNSAFE = tmpUnsafe;

        // Start background cleanup thread
        if (SUPPORTED) {
            Thread cleanupThread = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(CLEANUP_INTERVAL_MILLIS);
                        cleanAll();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        UndertowLogger.ROOT_LOGGER.debug("DirectByteBufferDeallocator6 cleanup thread interrupted");
                        break;
                    } catch (Throwable t) {
                        UndertowLogger.ROOT_LOGGER.debug("Error in DirectByteBufferDeallocator6 cleanup thread", t);
                    }
                }
            }, "DirectByteBufferDeallocator6-cleanup");
            cleanupThread.setDaemon(true);
            cleanupThread.start();
        }
    }

    private DirectByteBufferDeallocator6() {
        // Utility Class
    }

    private static int getQueueIndex() {
        long threadId = Thread.currentThread().getId();
        return (int) (threadId % queueCount);
    }

    /**
     * Attempts to deallocate the underlying direct memory.
     * This is a no-op for buffers where {@link ByteBuffer#isDirect()} returns false.
     *
     * @param buffer to deallocate
     */
    public static void free(ByteBuffer buffer) {
        if (SUPPORTED && buffer != null && buffer.isDirect()) {
            try {
                // Get the sharded queue for this thread
                int queueIdx = getQueueIndex();
                final ConcurrentLinkedQueue<QueuedByteBuffer> queue = queues[queueIdx];
                final Lock lock = queueLocks[queueIdx];

                // Try to clean old buffers if we can acquire the lock
                // If another thread is already cleaning, skip it to avoid contention
                if (lock.tryLock()) {
                    try {
                        cleanOldBuffers(queue);
                    } finally {
                        lock.unlock();
                    }
                }

                // Put the buffer to be cleaned in the queue
                // The goal here is to create a delay to make sure
                // that the buffer is not immediately deallocated
                // as there is a small window of time in which the
                // buffer is still accessible via local variables;
                // if a direct buffer is cleaned and then written to
                // or read from, the behavior of the sdk is unpredictable
                queue.add(new QueuedByteBuffer(buffer));
            } catch (Throwable t) {
                UndertowLogger.ROOT_LOGGER.directBufferDeallocationFailed(t);
            }
        }
    }

    /**
     * Attempts to clean all queues by trying to acquire each queue's lock and cleaning old buffers.
     * This method does not block - if a lock cannot be acquired, that queue is skipped.
     * Useful for periodic cleanup or shutdown scenarios.
     */
    public static void cleanAll() {
        if (!SUPPORTED) {
            return;
        }
        for (int i = 0; i < queueCount; i++) {
            final Lock lock = queueLocks[i];
            final ConcurrentLinkedQueue<QueuedByteBuffer> queue = queues[i];
            if (lock.tryLock()) {
                try {
                    cleanOldBuffers(queue);
                } catch (Throwable t) {
                    UndertowLogger.ROOT_LOGGER.debug("Failed to clean buffer queue", t);
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * Cleans buffers from the queue that have been waiting at least DEALLOCATION_DELAY_MILLIS.
     *
     * @param queue the queue to clean buffers from
     */
    private static void cleanOldBuffers(ConcurrentLinkedQueue<QueuedByteBuffer> queue)
            throws InvocationTargetException, IllegalAccessException {
        final long targetTimeMillis = System.currentTimeMillis() - DEALLOCATION_DELAY_MILLIS;
        QueuedByteBuffer queuedByteBuffer = queue.peek();
        while (queuedByteBuffer != null) {
            if (queuedByteBuffer.getTimeStamp() > targetTimeMillis) {
                break;
            }
            queue.remove();
            cleanBuffer(queuedByteBuffer.getByteBuffer());
            queuedByteBuffer = queue.peek();
        }
    }

    private static void cleanBuffer(ByteBuffer buffer) throws InvocationTargetException, IllegalAccessException {
        if (buffer != null) {
            if (UNSAFE != null && cleanerClean != null) {
                // use the JDK9 method
                cleanerClean.invoke(UNSAFE, buffer);
            } else if (cleaner != null && cleanerClean != null) {
                Object cleanerObj = cleaner.invoke(buffer);
                cleanerClean.invoke(cleanerObj);
            }
        }
    }

    private static Unsafe getUnsafe() {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(new PrivilegedAction<Unsafe>() {
                @Override
                public Unsafe run() {
                    return getUnsafe0();
                }
            });
        }
        return getUnsafe0();
    }

    private static Unsafe getUnsafe0() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(null);
        } catch (Throwable t) {
            throw new RuntimeException("JDK did not allow accessing unsafe", t);
        }
    }

    private static Method getAccesibleMethod(String className, String methodName) {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(new PrivilegedAction<Method>() {
                @Override
                public Method run() {
                    return getAccesibleMethod0(className, methodName);
                }
            });
        }
        return getAccesibleMethod0(className, methodName);
    }

    private static Method getAccesibleMethod0(String className, String methodName) {
        try {
            Method method = Class.forName(className).getMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (Throwable t) {
            throw new RuntimeException("JDK did not allow accessing method", t);
        }
    }

    private static Method getDeclaredMethod(Unsafe tmpUnsafe, String methodName, Class<?>... parameterTypes) {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(new PrivilegedAction<Method>() {
                @Override
                public Method run() {
                    return getDeclaredMethod0(tmpUnsafe, methodName, parameterTypes);
                }
            });
        }
        return getDeclaredMethod0(tmpUnsafe, methodName, parameterTypes);
    }

    private static Method getDeclaredMethod0(Unsafe tmpUnsafe, String methodName, Class<?>... parameterTypes) {
        try {
            Method method = tmpUnsafe.getClass().getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method;
        } catch (Throwable t) {
            throw new RuntimeException("JDK did not allow accessing method", t);
        }
    }

    private static final class QueuedByteBuffer {
        private final long timeStamp;
        private final ByteBuffer byteBuffer;

        QueuedByteBuffer(ByteBuffer byteBuffer) {
            this.timeStamp = System.currentTimeMillis();
            this.byteBuffer = byteBuffer;
        }

        long getTimeStamp() {
            return timeStamp;
        }

        ByteBuffer getByteBuffer() {
            return byteBuffer;
        }
    }
}
