package io.undertow.benchmarks;

import io.undertow.connector.ByteBufferPool;
import io.undertow.connector.PooledByteBuffer;
import io.undertow.server.DefaultByteBufferPool;
import io.undertow.server.DefaultByteBufferPool2;
import io.undertow.server.DefaultByteBufferPool3;
import io.undertow.server.DefaultByteBufferPool4;
import io.undertow.server.DefaultByteBufferPool5;
import io.undertow.server.DefaultByteBufferPool6;
import io.undertow.server.DefaultByteBufferPool7;
import io.undertow.server.DefaultByteBufferPool8;
import io.undertow.server.DefaultByteBufferPool9;
import io.undertow.server.DefaultByteBufferPool10;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;

import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.BenchmarkException;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing different ByteBufferPool implementations under high contention.
 *
 * Tests different queue implementations (ConcurrentLinkedQueue, ConcurrentLinkedDeque,
 * LinkedBlockingQueue, LinkedBlockingDeque) with both FIFO and LIFO semantics.
 *
 * Creates many tasks (numTasks) but limits concurrent execution to maxConcurrency
 * using a semaphore. This simulates realistic contention where many tasks compete
 * for limited concurrency slots.
 *
 * Allocation modes:
 * - batch: Allocate all 5 buffers, use them, then close all (tests bulk operations)
 * - interleaved: Allocate one, use it, close it, repeat (tests frequent pool contention)
 */
// Throughput mode - measures operations per second
//@BenchmarkMode(Mode.Throughput)
//@OutputTimeUnit(TimeUnit.SECONDS)

// Average time mode - measures time per operation
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

@State(Scope.Benchmark)
@Fork(1)  // Single fork for consistency
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Threads(1)  // JMH thread count (we'll use virtual threads internally)
public class VirtualThreadPoolContentionBenchmark {

    private ByteBufferPool pool;
    private ExecutorService executor;
    private Semaphore concurrencyLimiter;
    private CountDownLatch completionLatch;
    private Runnable bufferTask;

    //@Param({"DefaultByteBufferPool", "DefaultByteBufferPool2", "DefaultByteBufferPool3", "DefaultByteBufferPool4"})
    @Param({
//            "DefaultByteBufferPool",
//            "DefaultByteBufferPool6",
//            "DefaultByteBufferPool7",
//            "DefaultByteBufferPool8",
//            "DefaultByteBufferPool9",
            "DefaultByteBufferPool10",
    })
    private String poolType;

    @Param({"16384"})  // Buffer sizes to test
    private int bufferSize;

    //@Param({"8", "16", "32", "64"})  // Number of concurrent virtual threads allowed
    //@Param({"1", "2", "4", "8"})  // Number of concurrent virtual threads allowed
    //@Param({"64", "128", "256", "512", "1024", "2048", "4096"})  // Number of concurrent virtual threads allowed

    //@Param({"8192", "16384", "32768"})  // Number of concurrent tasks allowed
    @Param({"256"})  // Number of concurrent tasks allowed
    private int maxConcurrency;

    @Param({"1000000"})  // Total number of tasks
    private int numTasks;

    @Param({
    //        "virtual",
            "platform"
    })  // Thread type to use
    private String threadType;

    @Param({
            "0",
            //"6"
    })  // Thread local cache size (0 = disabled)
    private int threadLocalCacheSize;

    @Param({"1000"})  // Maximum pool size
    private int maxPoolSize;

    @Param({
            "batch",
            //"interleaved"
    })  // Allocation pattern: batch=allocate all then free all, interleaved=allocate one, free one, repeat
    private String allocationMode;

    private static final int BUFFERS_PER_TASK = 5;

    @Setup(Level.Trial)
    public void setupTrial() {
        // Skip invalid combination: virtual threads with thread-local cache
        if ("virtual".equals(threadType) && threadLocalCacheSize > 0) {
            throw new BenchmarkException(new RuntimeException(
                "Skipping benchmark: virtual threads with thread-local cache size > 0 " +
                "causes excessive memory usage (each virtual thread gets its own cache)"
            ));
        }

        // Skip invalid combination: DefaultByteBufferPool6/7/8/9/10 with thread-local cache
        if (("DefaultByteBufferPool6".equals(poolType) ||
             "DefaultByteBufferPool7".equals(poolType) ||
             "DefaultByteBufferPool8".equals(poolType) ||
             "DefaultByteBufferPool9".equals(poolType) ||
             "DefaultByteBufferPool10".equals(poolType)) && threadLocalCacheSize > 0) {
            throw new BenchmarkException(new RuntimeException(
                "Skipping benchmark: " + poolType + " does not support thread-local cache"
            ));
        }

        // Create the appropriate pool based on parameter
        if ("DefaultByteBufferPool".equals(poolType)) {
            // DefaultByteBufferPool(direct, bufferSize, maxPoolSize, threadLocalCacheSize)
            pool = new DefaultByteBufferPool(true, bufferSize, maxPoolSize, threadLocalCacheSize);
        } else if ("DefaultByteBufferPool2".equals(poolType)) {
            // DefaultByteBufferPool2(direct, bufferSize, maxPoolSize, threadLocalCacheSize)
            pool = new DefaultByteBufferPool2(true, bufferSize, maxPoolSize, threadLocalCacheSize);
        } else if ("DefaultByteBufferPool3".equals(poolType)) {
            // DefaultByteBufferPool3(direct, bufferSize, maxPoolSize, threadLocalCacheSize)
            pool = new DefaultByteBufferPool3(true, bufferSize, maxPoolSize, threadLocalCacheSize);
        } else if ("DefaultByteBufferPool4".equals(poolType)) {
            // DefaultByteBufferPool3(direct, bufferSize, maxPoolSize, threadLocalCacheSize)
            pool = new DefaultByteBufferPool4(true, bufferSize, maxPoolSize, threadLocalCacheSize);
        } else if ("DefaultByteBufferPool5".equals(poolType)) {
            // DefaultByteBufferPool3(direct, bufferSize, maxPoolSize, threadLocalCacheSize)
            pool = new DefaultByteBufferPool5(true, bufferSize, maxPoolSize, threadLocalCacheSize);
        } else if ("DefaultByteBufferPool6".equals(poolType)) {
            // DefaultByteBufferPool6(direct, bufferSize, maxPoolSize) - uses ConcurrentLinkedQueue (FIFO)
            pool = new DefaultByteBufferPool6(true, bufferSize, maxPoolSize);
        } else if ("DefaultByteBufferPool7".equals(poolType)) {
            // DefaultByteBufferPool7(direct, bufferSize, maxPoolSize) - uses ConcurrentLinkedDeque (LIFO)
            pool = new DefaultByteBufferPool7(true, bufferSize, maxPoolSize);
        } else if ("DefaultByteBufferPool8".equals(poolType)) {
            // DefaultByteBufferPool8(direct, bufferSize, maxPoolSize) - uses LinkedBlockingQueue (FIFO)
            pool = new DefaultByteBufferPool8(true, bufferSize, maxPoolSize);
        } else if ("DefaultByteBufferPool9".equals(poolType)) {
            // DefaultByteBufferPool9(direct, bufferSize, maxPoolSize) - uses LinkedBlockingDeque (LIFO)
            pool = new DefaultByteBufferPool9(true, bufferSize, maxPoolSize);
        } else if ("DefaultByteBufferPool10".equals(poolType)) {
            // DefaultByteBufferPool10(direct, bufferSize, maxPoolSize) - uses custom lock-free ring buffer
            pool = new DefaultByteBufferPool10(true, bufferSize, maxPoolSize);
        } else {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        // Setup for each benchmark invocation
        concurrencyLimiter = new Semaphore(maxConcurrency, true);  // Fair semaphore
        completionLatch = new CountDownLatch(numTasks);

        // Create executor based on thread type
        if ("virtual".equals(threadType)) {
            executor = Executors.newVirtualThreadPerTaskExecutor();
        } else if ("platform".equals(threadType)) {
            // Use fixed thread pool with exactly maxConcurrency platform threads
            executor = Executors.newFixedThreadPool(maxConcurrency);
        } else {
            throw new IllegalArgumentException("Unknown thread type: " + threadType);
        }

        // Create reusable task to minimize object allocation (1 instance vs numTasks instances)
        bufferTask = new BufferTask();
    }

    @Benchmark
    public int contentionTest() throws InterruptedException {
        // Submit all tasks using reusable Runnable (minimize object allocation)
        // Use execute() instead of submit() to avoid Future wrapper overhead
        for (int i = 0; i < numTasks; i++) {
            executor.execute(bufferTask);
        }

        // Prevent new tasks from being submitted
        executor.shutdown();

        // Wait for all tasks to complete
        completionLatch.await();

        // Verify all tasks completed (prevents DCE and validates correctness)
        long remaining = completionLatch.getCount();
        if (remaining != 0) {
            throw new RuntimeException("Expected all tasks to complete, but " + remaining + " tasks are still pending");
        }
        return numTasks;
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() throws InterruptedException {
        if (executor != null) {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool = null;
    }

    /**
     * Reusable task for buffer allocation/deallocation.
     * Instance inner class to access benchmark fields (pool, concurrencyLimiter, completionLatch).
     */
    private class BufferTask implements Runnable {
        @Override
        public void run() {
            try {
                // Acquire permit (blocks if maxConcurrency threads are already running)
                concurrencyLimiter.acquire();

                try {
                    if ("batch".equals(allocationMode)) {
                        // Batch mode: Allocate all buffers, use them, then close all
                        PooledByteBuffer[] buffers = new PooledByteBuffer[BUFFERS_PER_TASK];

                        // Allocate all buffers
                        for (int j = 0; j < BUFFERS_PER_TASK; j++) {
                            buffers[j] = pool.allocate();
                            if (buffers[j] == null) {
                                throw new RuntimeException("Failed to allocate buffer");
                            }
                        }

                        // Do minimal work with buffers (just write a byte to each)
                        for (int j = 0; j < BUFFERS_PER_TASK; j++) {
                            buffers[j].getBuffer().put((byte) j);
                        }

                        // Close all buffers (return to pool)
                        for (int j = 0; j < BUFFERS_PER_TASK; j++) {
                            buffers[j].close();
                        }
                    } else if ("interleaved".equals(allocationMode)) {
                        // Interleaved mode: Allocate one, use it, close it, repeat
                        for (int j = 0; j < BUFFERS_PER_TASK; j++) {
                            PooledByteBuffer buffer = pool.allocate();
                            if (buffer == null) {
                                throw new RuntimeException("Failed to allocate buffer");
                            }
                            // Do minimal work with buffer
                            buffer.getBuffer().put((byte) j);
                            // Immediately close and return to pool
                            buffer.close();
                        }
                    } else {
                        throw new IllegalArgumentException("Unknown allocation mode: " + allocationMode);
                    }
                } finally {
                    // Always release the permit
                    concurrencyLimiter.release();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completionLatch.countDown();
            }
        }
    }

    /**
     * Simple arithmetic task for baseline comparison.
     * Just adds numbers without any buffer allocation.
     */
    private class ArithmeticTask implements Runnable {
        @Override
        public void run() {
            try {
                // Acquire permit (blocks if maxConcurrency threads are already running)
                concurrencyLimiter.acquire();

                try {
                    // Just do some simple arithmetic
                    int sum = 0;
                    for (int j = 0; j < BUFFERS_PER_TASK; j++) {
                        sum += j * 2;
                    }
                    // Prevent DCE by using the result
                    if (sum < 0) {
                        throw new RuntimeException("Unexpected sum: " + sum);
                    }
                } finally {
                    // Always release the permit
                    concurrencyLimiter.release();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completionLatch.countDown();
            }
        }
    }

    // Main method to run the benchmark
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + VirtualThreadPoolContentionBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(5)
                .addProfiler(JavaFlightRecorderProfiler.class, "")
                .build();

        new Runner(opt).run();
    }
}