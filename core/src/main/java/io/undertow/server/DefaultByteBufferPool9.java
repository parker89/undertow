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

import io.undertow.UndertowMessages;
import io.undertow.connector.ByteBufferPool;
import io.undertow.connector.PooledByteBuffer;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A byte buffer pool that supports reference counted pools.
 * Uses LinkedBlockingDeque with LIFO (stack) semantics for better cache locality.
 * Most recently freed buffers are reused first, improving CPU cache hit rates.
 *
 * @author Stuart Douglas
 */
// DefaultByteBufferPool9 uses LinkedBlockingDeque with LIFO behavior (pollFirst/addFirst)
public final class DefaultByteBufferPool9 implements ByteBufferPool {

    private final LinkedBlockingDeque<ByteBuffer>[] queues;
    private final int queueCount;
    private final int perQueueMax;

    private final boolean direct;
    private final int bufferSize;

    private final AtomicIntegerArray currentQueueLengths;

    private volatile boolean closed;

    private final DefaultByteBufferPool9 arrayBackedPool;

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     */
    public DefaultByteBufferPool9(boolean direct, int bufferSize) {
        this(direct, bufferSize, -1);
    }

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     * @param maximumPoolSize      The maximum pool size, in number of buffers
     */
    public DefaultByteBufferPool9(boolean direct, int bufferSize, int maximumPoolSize) {
        this(direct, bufferSize, maximumPoolSize, Runtime.getRuntime().availableProcessors() * 8);
    }

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     * @param maximumPoolSize      The maximum pool size, in number of buffers
     * @param queueCount           Number of queues to use for reduced contention
     */
    @SuppressWarnings("unchecked")
    public DefaultByteBufferPool9(boolean direct, int bufferSize, int maximumPoolSize, int queueCount) {
        this.direct = direct;
        this.bufferSize = bufferSize;
        this.queueCount = Math.max(1, queueCount);

        this.perQueueMax = maximumPoolSize >= 0 ? maximumPoolSize / this.queueCount : Integer.MAX_VALUE;

        this.queues = new LinkedBlockingDeque[this.queueCount];
        this.currentQueueLengths = new AtomicIntegerArray(this.queueCount);
        for (int i = 0; i < this.queueCount; i++) {
            // Create bounded or unbounded deque based on perQueueMax
            if (perQueueMax < Integer.MAX_VALUE) {
                this.queues[i] = new LinkedBlockingDeque<>(perQueueMax + 1);
            } else {
                this.queues[i] = new LinkedBlockingDeque<>();
            }
        }

        if (direct) {
            arrayBackedPool = new DefaultByteBufferPool9(false, bufferSize, maximumPoolSize, this.queueCount);
        } else {
            arrayBackedPool = this;
        }
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public boolean isDirect() {
        return direct;
    }

    private int getQueueIndex() {
        long threadId = Thread.currentThread().getId();
        return (int) (threadId % queueCount);
    }

    @Override
    public PooledByteBuffer allocate() {
        if (closed) {
            throw UndertowMessages.MESSAGES.poolIsClosed();
        }

        // Try to get a buffer from the sharded queue (LIFO - use pollFirst for stack behavior)
        int queueIdx = getQueueIndex();
        ByteBuffer buffer = queues[queueIdx].pollFirst();
        if (buffer != null) {
            currentQueueLengths.decrementAndGet(queueIdx);
        }

        // If no buffer available, allocate a new one
        if (buffer == null) {
            if (direct) {
                buffer = ByteBuffer.allocateDirect(bufferSize);
            } else {
                buffer = ByteBuffer.allocate(bufferSize);
            }
        }

        buffer.clear();
        return new DefaultPooledBuffer(this, buffer);
    }

    @Override
    public ByteBufferPool getArrayBackedPool() {
        return arrayBackedPool;
    }

    private void freeInternal(ByteBuffer buffer) {
        if (closed) {
            DirectByteBufferDeallocator6.free(buffer);
            return;
        }
        queueIfUnderMax(buffer);
    }

    private void queueIfUnderMax(ByteBuffer buffer) {
        int size;
        int queueIdx = getQueueIndex();
        do {
            size = currentQueueLengths.get(queueIdx);
            if (size > perQueueMax) {
                DirectByteBufferDeallocator6.free(buffer);
                return;
            }
        } while (!currentQueueLengths.compareAndSet(queueIdx, size, size + 1));

        // LIFO - use offerFirst for stack behavior (better cache locality)
        // Use offerFirst() instead of addFirst() to avoid blocking if deque is bounded
        if (!queues[queueIdx].offerFirst(buffer)) {
            // Deque is full, decrement the count and free the buffer
            currentQueueLengths.decrementAndGet(queueIdx);
            DirectByteBufferDeallocator6.free(buffer);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        for (int i = 0; i < queueCount; i++) {
            queues[i].clear();
            currentQueueLengths.set(i, 0);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    private static class DefaultPooledBuffer implements PooledByteBuffer {

        private final DefaultByteBufferPool9 pool;

        private ByteBuffer buffer;

        private volatile int referenceCount = 1;
        private static final AtomicIntegerFieldUpdater<DefaultPooledBuffer> referenceCountUpdater =
                AtomicIntegerFieldUpdater.newUpdater(DefaultPooledBuffer.class, "referenceCount");

        DefaultPooledBuffer(DefaultByteBufferPool9 pool, ByteBuffer buffer) {
            this.pool = pool;
            this.buffer = buffer;
        }

        @Override
        public ByteBuffer getBuffer() {
            final ByteBuffer tmp = this.buffer;
            // UNDERTOW-2072
            if (referenceCount == 0 || tmp == null) {
                throw UndertowMessages.MESSAGES.bufferAlreadyFreed();
            }
            return tmp;
        }

        @Override
        public void close() {
            final ByteBuffer tmp = this.buffer;
            if (referenceCountUpdater.compareAndSet(this, 1, 0)) {
                this.buffer = null;
                if (tmp != null) {
                    pool.freeInternal(tmp);
                }
            }
        }

        @Override
        public boolean isOpen() {
            return referenceCount > 0;
        }

        @Override
        public String toString() {
            return "DefaultPooledBuffer{" + "buffer=" + buffer + ", referenceCount=" + referenceCount + '}';
        }
    }
}
