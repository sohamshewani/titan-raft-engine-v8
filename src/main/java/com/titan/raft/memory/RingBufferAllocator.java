package com.titan.raft.memory;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class RingBufferAllocator {
    private static final Unsafe UNSAFE;
    private static final int CACHE_LINE_SIZE = 64;
    private static final int ALIGNMENT = 8;
    
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Unsafe", e);
        }
    }

    private final long baseAddress;
    private final long capacityBytes;
    private final int bufferSize;
    private final int elementSize;
    
    private final AtomicLong head = new AtomicLong(0L);
    private final AtomicLong tail = new AtomicLong(0L);
    
    private static final AtomicIntegerFieldUpdater<RingBufferAllocator> CLOSED_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(RingBufferAllocator.class, "closed");
    
    @SuppressWarnings("unused")
    private volatile int closed = 0;

    public RingBufferAllocator(int bufferSize, int elementSize) {
        if (bufferSize <= 0 || (bufferSize & (bufferSize - 1)) != 0) {
            throw new IllegalArgumentException("bufferSize must be a positive power of two");
        }
        if (elementSize <= 0) {
            throw new IllegalArgumentException("elementSize must be positive");
        }
        
        this.bufferSize = bufferSize;
        int alignedElementSize = (elementSize + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);
        this.elementSize = alignedElementSize;
        
        long rawSize = (long) bufferSize * alignedElementSize;
        long padding = CACHE_LINE_SIZE * 2;
        this.capacityBytes = rawSize + padding;
        
        this.baseAddress = UNSAFE.allocateMemory(this.capacityBytes);
        UNSAFE.setMemory(this.baseAddress, this.capacityBytes, (byte) 0);
    }

    public long allocate() {
        if (closed != 0) {
            throw new IllegalStateException("RingBufferAllocator is closed");
        }
        
        long currentHead;
        long currentTail;
        
        do {
            currentHead = head.get();
            currentTail = tail.get();
            
            if (currentHead - currentTail >= bufferSize) {
                return 0L; // Buffer full (backpressure)
            }
        } while (!head.compareAndSet(currentHead, currentHead + 1));
        
        long index = currentHead & (bufferSize - 1);
        return baseAddress + (index * elementSize);
    }

    public void release(long address) {
        if (address < baseAddress || address >= baseAddress + ((long) bufferSize * elementSize)) {
            throw new IllegalArgumentException("Address out of bounds for this allocator");
        }
        
        long currentTail;
        do {
            currentTail = tail.get();
        } while (!tail.compareAndSet(currentTail, currentTail + 1));
    }

    public ByteBuffer wrapDirect(long address, int length) {
        if (address < baseAddress || address + length > baseAddress + capacityBytes) {
            throw new IllegalArgumentException("Address bounds exceed allocated arena");
        }
        try {
            ByteBuffer buffer = (ByteBuffer) UNSAFE.allocateInstance(java.nio.DirectByteBuffer.class);
            
            Field addressField = java.nio.Buffer.class.getDeclaredField("address");
            Field capacityField = java.nio.Buffer.class.getDeclaredField("capacity");
            Field limitField = java.nio.Buffer.class.getDeclaredField("limit");
            Field positionField = java.nio.Buffer.class.getDeclaredField("position");
            
            addressField.setAccessible(true);
            capacityField.setAccessible(true);
            limitField.setAccessible(true);
            positionField.setAccessible(true);
            
            addressField.setLong(buffer, address);
            capacityField.setInt(buffer, length);
            limitField.setInt(buffer, length);
            positionField.setInt(buffer, 0);
            
            return buffer;
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct DirectByteBuffer via reflection", e);
        }
    }

    public void close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            UNSAFE.freeMemory(baseAddress);
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getElementSize() {
        return elementSize;
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public long getBaseAddress() {
        return baseAddress;
    }
}