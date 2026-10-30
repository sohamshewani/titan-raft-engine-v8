package com.titan.raft.concurrency;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class EpochScheduler {

    private static final Unsafe UNSAFE;
    private static final long GLOBAL_EPOCH_OFFSET;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            GLOBAL_EPOCH_OFFSET = UNSAFE.objectFieldOffset(
                EpochScheduler.class.getDeclaredField("globalEpoch")
            );
        } catch (Exception e) {
            throw new AssertionError("Failed to initialize Unsafe for EpochScheduler", e);
        }
    }

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int MAX_PARTICIPANTS = 1024;
    private static final long UPDATE_INTERVAL_MS = 10L;

    private volatile long globalEpoch = 1L;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread backgroundSchedulerThread;

    private final ParticipantRegistry[] registries;
    private final EpochRetiredList[] retiredLists;

    public EpochScheduler() {
        this.registries = new ParticipantRegistry[NUM_THREADS];
        this.retiredLists = new EpochRetiredList[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            this.registries[i] = new ParticipantRegistry(MAX_PARTICIPANTS);
            this.retiredLists[i] = new EpochRetiredList();
        }

        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "titan-raft-epoch-scheduler");
            t.setDaemon(true);
            return t;
        };
        this.backgroundSchedulerThread = factory.newThread(this::runEpochAdvanceLoop);
        this.backgroundSchedulerThread.start();
    }

    public long acquireEpoch() {
        int threadId = (int) (Thread.currentThread().getId() % NUM_THREADS);
        registries[threadId].enter(globalEpoch);
        return globalEpoch;
    }

    public void releaseEpoch() {
        int threadId = (int) (Thread.currentThread().getId() % NUM_THREADS);
        registries[threadId].leave();
    }

    public void defer(Runnable task) {
        if (task == null) {
            return;
        }
        int threadId = (int) (Thread.currentThread().getId() % NUM_THREADS);
        long currentEpoch = globalEpoch;
        retiredLists[threadId].push(currentEpoch, task);
    }

    private void runEpochAdvanceLoop() {
        while (running.get()) {
            LockSupport.parkNanos(UPDATE_INTERVAL_MS * 1_000_000L);
            if (!running.get()) {
                break;
            }

            long targetEpoch = globalEpoch;
            boolean allPassed = true;

            for (int i = 0; i < NUM_THREADS; i++) {
                if (!registries[i].isSafeToAdvance(targetEpoch)) {
                    allPassed = false;
                    break;
                }
            }

            if (allPassed) {
                long nextEpoch = targetEpoch + 1L;
                UNSAFE.compareAndSwapLong(this, GLOBAL_EPOCH_OFFSET, targetEpoch, nextEpoch);

                long safeCleanupThreshold = targetEpoch - 1L;
                for (int i = 0; i < NUM_THREADS; i++) {
                    retiredLists[i].reclaimUpTo(safeCleanupThreshold);
                }
            }
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            LockSupport.unpark(backgroundSchedulerThread);
            try {
                backgroundSchedulerThread.join(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            for (int i = 0; i < NUM_THREADS; i++) {
                retiredLists[i]._forceDrainAll();
            }
        }
    }

    private static final class ParticipantRegistry {
        private final long[] epochs;
        private final int capacity;

        ParticipantRegistry(int capacity) {
            this.capacity = capacity;
            this.epochs = new long[capacity * 8]; // padded to avoid false sharing
        }

        void enter(long epoch) {
            int slot = (int) (Thread.currentThread().getId() % capacity) * 8;
            epochs[slot] = epoch;
        }

        void leave() {
            int slot = (int) (Thread.currentThread().getId() % capacity) * 8;
            epochs[slot] = 0L;
        }

        boolean isSafeToAdvance(long currentGlobalEpoch) {
            for (int i = 0; i < capacity; i++) {
                long e = epochs[i * 8];
                if (e != 0L && e < currentGlobalEpoch) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class EpochRetiredList {
        private static final class Node {
            final long epoch;
            final Runnable task;
            Node next;

            Node(long epoch, Runnable task) {
                this.epoch = epoch;
                this.task = task;
            }
        }

        private Node head;
        private Node tail;

        synchronized void push(long epoch, Runnable task) {
            Node node = new Node(epoch, task);
            if (head == null) {
                head = node;
                tail = node;
            } else {
                tail.next = node;
                tail = node;
            }
        }

        synchronized void reclaimUpTo(long safeEpoch) {
            while (head != null && head.epoch <= safeEpoch) {
                Node current = head;
                head = head.next;
                if (head == null) {
                    tail = null;
                }
                try {
                    current.task.run();
                } catch (Throwable t) {
                    // Suppress or log silently to prevent epoch loop termination
                }
            }
        }

        synchronized void _forceDrainAll() {
            while (head != null) {
                Node current = head;
                head = head.next;
                try {
                    current.task.run();
                } catch (Throwable t) {
                    // Drain
                }
            }
            tail = null;
        }
    }
}