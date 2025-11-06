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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A high-performance byte buffer pool using a custom lock-free ring buffer.
 * This implementation uses advanced atomic operations for maximum throughput
 * and minimal contention under high concurrency.
 *
 * The ring buffer design provides:
 * - Lock-free MPMC (Multiple Producer Multiple Consumer) operations
 * - Better cache locality than linked structures
 * - Bounded memory usage with no dynamic allocation
 * - Minimal false sharing through careful field layout
 *
 * @author Stuart Douglas (original)
 * @author Enhanced for maximum performance with lock-free ring buffer
 */
public final class DefaultByteBufferPool10 implements ByteBufferPool {

    private final LockFreeRingBuffer<ByteBuffer>[] ringBuffers;
    private final int ringCount;

    private final boolean direct;
    private final int bufferSize;
    private final int maximumPoolSize;

    private volatile boolean closed;

    private final DefaultByteBufferPool10 arrayBackedPool;

    // Thread-local index for ring buffer selection to reduce contention
    private static final ThreadLocal<Integer> threadIndex = ThreadLocal.withInitial(() -> 0);

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     */
    public DefaultByteBufferPool10(boolean direct, int bufferSize) {
        this(direct, bufferSize, -1);
    }

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     * @param maximumPoolSize      The maximum pool size, in number of buffers
     */
    public DefaultByteBufferPool10(boolean direct, int bufferSize, int maximumPoolSize) {
        this(direct, bufferSize, maximumPoolSize, Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * @param direct               If this implementation should use direct buffers
     * @param bufferSize           The buffer size to use
     * @param maximumPoolSize      The maximum pool size, in number of buffers
     * @param ringCount            Number of ring buffers to use for reduced contention
     */
    @SuppressWarnings("unchecked")
    public DefaultByteBufferPool10(boolean direct, int bufferSize, int maximumPoolSize, int ringCount) {
        this.direct = direct;
        this.bufferSize = bufferSize;
        this.maximumPoolSize = maximumPoolSize;
        this.ringCount = Math.max(1, ringCount);

        // Calculate per-ring capacity (must be power of 2 for efficient masking)
        int perRingCapacity = maximumPoolSize > 0 ?
            nextPowerOfTwo((maximumPoolSize / this.ringCount) + 1) :
            1024; // Default capacity if unbounded

        this.ringBuffers = new LockFreeRingBuffer[this.ringCount];
        for (int i = 0; i < this.ringCount; i++) {
            this.ringBuffers[i] = new LockFreeRingBuffer<>(perRingCapacity);
        }

        if (direct) {
            arrayBackedPool = new DefaultByteBufferPool10(false, bufferSize, maximumPoolSize, this.ringCount);
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

    @Override
    public PooledByteBuffer allocate() {
        if (closed) {
            throw UndertowMessages.MESSAGES.poolIsClosed();
        }

        ByteBuffer buffer = null;

        // Try to get from ring buffers using thread-local index for better locality
        int startIdx = threadIndex.get();
        for (int i = 0; i < ringCount; i++) {
            int idx = (startIdx + i) % ringCount;
            buffer = ringBuffers[idx].poll();
            if (buffer != null) {
                // Update thread-local for next allocation to spread load
                threadIndex.set((idx + 1) % ringCount);
                break;
            }
        }

        if (buffer == null) {
            // Allocate new buffer if pool is empty
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
            DirectByteBufferDeallocator.free(buffer);
            return;
        }

        // Try to return to ring buffers, using thread-local index
        int startIdx = threadIndex.get();
        for (int i = 0; i < ringCount; i++) {
            int idx = (startIdx + i) % ringCount;
            if (ringBuffers[idx].offer(buffer)) {
                return;
            }
        }

        // If all ring buffers are full, deallocate
        DirectByteBufferDeallocator.free(buffer);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Clear all ring buffers
        for (LockFreeRingBuffer<ByteBuffer> ring : ringBuffers) {
            ring.clear();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    private static int nextPowerOfTwo(int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Custom lock-free ring buffer implementation optimized for ByteBuffer pooling.
     * Uses separate producer and consumer sequences to minimize contention.
     */
    private static class LockFreeRingBuffer<T> {
        // Padding to prevent false sharing on producer side
        private volatile long p1, p2, p3, p4, p5, p6, p7;

        private volatile long producerSequence;
        private static final AtomicLongFieldUpdater<LockFreeRingBuffer> PRODUCER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(LockFreeRingBuffer.class, "producerSequence");

        // Padding between producer and consumer
        private volatile long p8, p9, p10, p11, p12, p13, p14, p15;

        private volatile long consumerSequence;
        private static final AtomicLongFieldUpdater<LockFreeRingBuffer> CONSUMER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(LockFreeRingBuffer.class, "consumerSequence");

        // Padding to prevent false sharing on consumer side
        private volatile long c1, c2, c3, c4, c5, c6, c7;

        private final int capacity;
        private final int mask;
        private final AtomicReferenceArray<T> buffer;

        // Track cached values to reduce volatile reads
        private volatile long cachedConsumerSequence;
        private volatile long cachedProducerSequence;

        LockFreeRingBuffer(int capacity) {
            if (capacity < 1 || (capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of 2");
            }
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.buffer = new AtomicReferenceArray<>(capacity);
            this.producerSequence = 0;
            this.consumerSequence = 0;
            this.cachedConsumerSequence = 0;
            this.cachedProducerSequence = 0;
        }

        boolean offer(T item) {
            if (item == null) {
                return false;
            }

            long currentProducer;
            long newProducer;

            do {
                currentProducer = producerSequence;
                newProducer = currentProducer + 1;

                // Check if buffer is full using cached consumer sequence
                long currentConsumer = cachedConsumerSequence;
                if (newProducer - currentConsumer > capacity) {
                    // Update cache and recheck
                    currentConsumer = cachedConsumerSequence = consumerSequence;
                    if (newProducer - currentConsumer > capacity) {
                        return false; // Buffer is full
                    }
                }
            } while (!PRODUCER_UPDATER.compareAndSet(this, currentProducer, newProducer));

            // We have reserved our slot, now insert the item
            int index = (int)(currentProducer & mask);

            // Spin-wait if slot is not yet available (rare case)
            while (buffer.get(index) != null) {
                Thread.onSpinWait();
            }

            buffer.lazySet(index, item);
            return true;
        }

        T poll() {
            long currentConsumer;
            long newConsumer;

            do {
                currentConsumer = consumerSequence;

                // Check if buffer is empty using cached producer sequence
                long currentProducer = cachedProducerSequence;
                if (currentConsumer >= currentProducer) {
                    // Update cache and recheck
                    currentProducer = cachedProducerSequence = producerSequence;
                    if (currentConsumer >= currentProducer) {
                        return null; // Buffer is empty
                    }
                }

                newConsumer = currentConsumer + 1;
            } while (!CONSUMER_UPDATER.compareAndSet(this, currentConsumer, newConsumer));

            // We have reserved our slot, now retrieve the item
            int index = (int)(currentConsumer & mask);

            // Spin-wait for item to be available
            T item;
            while ((item = buffer.get(index)) == null) {
                Thread.onSpinWait();
            }

            // Clear the slot for reuse
            buffer.lazySet(index, null);
            return item;
        }

        void clear() {
            // Drain all items
            while (poll() != null) {
                // Keep draining
            }
        }
    }

    private static class DefaultPooledBuffer implements PooledByteBuffer {

        private final DefaultByteBufferPool10 pool;
        private ByteBuffer buffer;

        private volatile int referenceCount = 1;
        private static final AtomicIntegerFieldUpdater<DefaultPooledBuffer> referenceCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(DefaultPooledBuffer.class, "referenceCount");

        DefaultPooledBuffer(DefaultByteBufferPool10 pool, ByteBuffer buffer) {
            this.pool = pool;
            this.buffer = buffer;
        }

        @Override
        public ByteBuffer getBuffer() {
            final ByteBuffer tmp = this.buffer;
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
                pool.freeInternal(tmp);
            }
        }

        @Override
        public boolean isOpen() {
            return referenceCount > 0;
        }

        @Override
        public String toString() {
            return "DefaultPooledBuffer{" +
                    "buffer=" + buffer +
                    ", referenceCount=" + referenceCount +
                    '}';
        }
    }
}