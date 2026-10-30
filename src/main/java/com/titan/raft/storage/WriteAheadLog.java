package com.titan.raft.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Production-grade Write-Ahead Log (WAL) implementation utilizing memory-mapped files
 * (FileChannel + MappedByteBuffer), segment-based rotation, checksum validation, and
 * direct off-heap serialization pathways for titan-raft-engine.
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final int MAGIC_HEADER = 0x54495441; // "TITA"
    private static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 4; // magic(4) + type(4) + term(8) + index(8) + length(4)
    private static final int CRC_SIZE = 4;
    private static final int DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024; // 64 MB

    private final File directory;
    private final long segmentSize;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    
    private final List<LogSegment> segments = new ArrayList<>();
    private LogSegment activeSegment;
    
    private final AtomicLong minLogIndex = new AtomicLong(1);
    private final AtomicLong maxLogIndex = new AtomicLong(0);

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final byte type;
        private final ByteBuffer payload;

        public LogEntry(long term, long index, byte type, ByteBuffer payload) {
            this.term = term;
            this.index = index;
            this.type = type;
            this.payload = payload;
        }

        public long getTerm() { return term; }
        public long getIndex() { return index; }
        public byte getType() { return type; }
        public ByteBuffer getPayload() { return payload.asReadOnlyBuffer(); }
    }

    private static final class LogSegment implements AutoCloseable {
        private final File file;
        private final long baseIndex;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private MappedByteBuffer mappedBuffer;
        private long writePosition;

        private LogSegment(File file, long baseIndex, long capacity, boolean readOnly) throws IOException {
            this.file = file;
            this.baseIndex = baseIndex;
            this.raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
            this.channel = raf.getChannel();
            if (!readOnly) {
                this.channel.truncate(capacity);
            }
            long size = channel.size();
            this.mappedBuffer = channel.map(
                readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                0, size
            );
            this.writePosition = 0;
        }

        public boolean hasCapacity(int requiredBytes) {
            return (writePosition + requiredBytes) <= mappedBuffer.capacity();
        }

        public void write(long term, long index, byte type, ByteBuffer payload) {
            int payloadLen = payload.remaining();
            int recordSize = HEADER_SIZE + payloadLen + CRC_SIZE;

            mappedBuffer.position((int) writePosition);
            
            // Mark start position for CRC calculation
            int crcStartIndex = mappedBuffer.position();

            mappedBuffer.putInt(MAGIC_HEADER);
            mappedBuffer.put(type);
            // Pad 3 bytes if needed or keep structure strict
            mappedBuffer.put(new byte[3]); 
            mappedBuffer.putLong(term);
            mappedBuffer.putLong(index);
            mappedBuffer.putInt(payloadLen);

            int payloadStartMark = mappedBuffer.position();
            mappedBuffer.put(payload);

            int crcEndIndex = mappedBuffer.position();

            // Compute CRC32 over header + payload
            ByteBuffer duplicate = mappedBuffer.duplicate();
            duplicate.position(crcStartIndex);
            duplicate.limit(crcEndIndex);

            CRC32 crc32 = new CRC32();
            crc32.update(duplicate);
            int checksum = (int) crc32.getValue();

            mappedBuffer.putInt(checksum);
            writePosition += recordSize;
        }

        public void force() {
            if (mappedBuffer != null) {
                mappedBuffer.force();
            }
        }

        @Override
        public void close() throws IOException {
            if (mappedBuffer != null) {
                mappedBuffer = null;
            }
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (raf != null) {
                raf.close();
            }
        }
    }

    public WriteAheadLog(File directory) throws IOException {
        this(directory, DEFAULT_SEGMENT_SIZE);
    }

    public WriteAheadLog(File directory, long segmentSize) throws IOException {
        this.directory = directory;
        this.segmentSize = segmentSize;
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Failed to create WAL directory: " + directory.getAbsolutePath());
        }
        recoverAndOpen();
    }

    private void recoverAndOpen() throws IOException {
        File[] files = directory.listFiles((dir, name) -> name.endsWith(".wal"));
        List<File> segmentFiles = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                segmentFiles.add(f);
            }
        }

        segmentFiles.sort((f1, f2) -> {
            long idx1 = parseBaseIndex(f1.getName());
            long idx2 = parseBaseIndex(f2.getName());
            return Long.compare(idx1, idx2);
        });

        if (segmentFiles.isEmpty()) {
            File firstFile = new File(directory, formatFileName(1));
            LogSegment segment = new LogSegment(firstFile, 1, segmentSize, false);
            segments.add(segment);
            activeSegment = segment;
            minLogIndex.set(1);
            maxLogIndex.set(0);
        } else {
            for (int i = 0; i < segmentFiles.size(); i++) {
                File f = segmentFiles.get(i);
                long baseIdx = parseBaseIndex(f.getName());
                boolean isLast = (i == segmentFiles.size() - 1);
                
                LogSegment segment = new LogSegment(f, baseIdx, segmentSize, false);
                long validEndPosition = recoverSegment(segment, baseIdx);
                segment.writePosition = validEndPosition;
                segments.add(segment);

                if (isLast) {
                    activeSegment = segment;
                }
            }
            if (!segments.isEmpty()) {
                minLogIndex.set(segments.get(0).baseIndex);
                maxLogIndex.set(calculateMaxIndex());
            }
        }
    }

    private long recoverSegment(LogSegment segment, long baseIndex) {
        MappedByteBuffer buf = segment.mappedBuffer;
        buf.position(0);
        long validPosition = 0;
        long maxIdxFound = baseIndex - 1;

        while (buf.remaining() >= HEADER_SIZE + CRC_SIZE) {
            int mark = buf.position();
            int magic = buf.getInt();
            if (magic != MAGIC_HEADER) {
                // Hit unwritten or corrupted block boundary
                buf.position(mark);
                break;
            }

            byte type = buf.get();
            buf.position(buf.position() + 3); // skip padding
            long term = buf.getLong();
            long index = buf.getLong();
            int payloadLen = buf.getInt();

            int totalRecordSize = HEADER_SIZE + payloadLen + CRC_SIZE;
            if (buf.remaining() < payloadLen + CRC_SIZE) {
                // Partial record due to crash
                buf.position(mark);
                break;
            }

            int payloadStart = buf.position();
            buf.position(payloadStart + payloadLen);
            int recordedChecksum = buf.getInt();

            // Verify CRC
            ByteBuffer duplicate = buf.duplicate();
            duplicate.position(mark);
            duplicate.limit(payloadStart + payloadLen);

            CRC32 crc32 = new CRC32();
            crc32.update(duplicate);
            int computedChecksum = (int) crc32.getValue();

            if (computedChecksum != recordedChecksum) {
                // Checksum failure: truncate corrupt trailing block
                buf.position(mark);
                break;
            }

            validPosition = buf.position();
            maxIdxFound = Math.max(maxIdxFound, index);
        }

        // Zero out corrupt/trailing region for cleanliness
        int limit = buf.limit();
        buf.position((int) validPosition);
        while (buf.position() < limit) {
            buf.put((byte) 0);
        }
        
        return validPosition;
    }

    private long calculateMaxIndex() throws IOException {
        long maxIdx = 0;
        for (LogSegment seg : segments) {
            MappedByteBuffer buf = seg.mappedBuffer;
            buf.position(0);
            while (buf.remaining() >= HEADER_SIZE + CRC_SIZE) {
                int mark = buf.position();
                int magic = buf.getInt();
                if (magic != MAGIC_HEADER) {
                    break;
                }
                buf.position(buf.position() + 3); // padding
                long term = buf.getLong();
                long index = buf.getLong();
                int payloadLen = buf.getInt();
                if (buf.remaining() < payloadLen + CRC_SIZE) {
                    break;
                }
                buf.position(buf.position() + payloadLen + CRC_SIZE);
                maxIdx = Math.max(maxIdx, index);
            }
        }
        return maxIdx;
    }

    public void append(long term, long index, byte type, ByteBuffer payload) throws IOException {
        ensureNotClosed();
        writeLock.lock();
        try {
            int payloadLen = payload.remaining();
            int recordSize = HEADER_SIZE + payloadLen + CRC_SIZE;

            if (!activeSegment.hasCapacity(recordSize)) {
                activeSegment.force();
                rollover(index);
            }

            activeSegment.write(term, index, type, payload);
            maxLogIndex.set(Math.max(maxLogIndex.get(), index));
        } finally {
            writeLock.unlock();
        }
    }

    private void rollover(long nextIndex) throws IOException {
        activeSegment.force();
        File newFile = new File(directory, formatFileName(nextIndex));
        LogSegment newSegment = new LogSegment(newFile, nextIndex, segmentSize, false);
        segments.add(newSegment);
        activeSegment = newSegment;
    }

    public void sync() throws IOException {
        ensureNotClosed();
        writeLock.lock();
        try {
            if (activeSegment != null) {
                activeSegment.force();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public LogEntry read(long index) throws IOException {
        ensureNotClosed();
        writeLock.lock();
        try {
            LogSegment targetSegment = findSegment(index);
            if (targetSegment == null) {
                return null;
            }

            MappedByteBuffer buf = targetSegment.mappedBuffer;
            buf.position(0);

            while (buf.remaining() >= HEADER_SIZE + CRC_SIZE) {
                int mark = buf.position();
                int magic = buf.getInt();
                if (magic != MAGIC_HEADER) {
                    break;
                }
                byte type = buf.get();
                buf.position(buf.position() + 3);
                long term = buf.getLong();
                long entryIndex = buf.getLong();
                int payloadLen = buf.getInt();

                if (buf.remaining() < payloadLen + CRC_SIZE) {
                    break;
                }

                int payloadStart = buf.position();
                ByteBuffer payloadSlice = buf.duplicate();
                payloadSlice.position(payloadStart);
                payloadSlice.limit(payloadStart + payloadLen);

                buf.position(payloadStart + payloadLen + CRC_SIZE);

                if (entryIndex == index) {
                    ByteBuffer copiedPayload = ByteBuffer.allocateDirect(payloadLen);
                    copiedPayload.put(payloadSlice);
                    copiedPayload.flip();
                    return new LogEntry(term, index, type, copiedPayload);
                }
            }
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    public void truncateSuffix(long index) throws IOException {
        ensureNotClosed();
        writeLock.lock();
        try {
            // Remove segments whose baseIndex > index
            List<LogSegment> toRemove = new ArrayList<>();
            for (LogSegment seg : segments) {
                if (seg.baseIndex > index) {
                    toRemove.add(seg);
                }
            }

            for (LogSegment seg : toRemove) {
                segments.remove(seg);
                seg.close();
                if (!seg.file.delete()) {
                    throw new IOException("Failed to delete truncated segment file: " + seg.file.getAbsolutePath());
                }
            }

            if (!segments.isEmpty()) {
                activeSegment = segments.get(segments.size() - 1);
            }

            // Truncate within active segment
            if (activeSegment != null) {
                MappedByteBuffer buf = activeSegment.mappedBuffer;
                buf.position(0);
                long validPosition = 0;

                while (buf.remaining() >= HEADER_SIZE + CRC_SIZE) {
                    int mark = buf.position();
                    int magic = buf.getInt();
                    if (magic != MAGIC_HEADER) {
                        break;
                    }
                    buf.position(buf.position() + 3);
                    long term = buf.getLong();
                    long entryIndex = buf.getLong();
                    int payloadLen = buf.getInt();

                    if (buf.remaining() < payloadLen + CRC_SIZE || entryIndex > index) {
                        buf.position(mark);
                        break;
                    }

                    buf.position(buf.position() + payloadLen + CRC_SIZE);
                    validPosition = buf.position();
                }

                activeSegment.writePosition = validPosition;
                // Zero out truncated space
                int limit = buf.limit();
                buf.position((int) validPosition);
                while (buf.position() < limit) {
                    buf.put((byte) 0);
                }
                activeSegment.force();
            }

            maxLogIndex.set(Math.min(maxLogIndex.get(), index));
        } finally {
            writeLock.unlock();
        }
    }

    public void truncatePrefix(long index) throws IOException {
        ensureNotClosed();
        writeLock.lock();
        try {
            List<LogSegment> toRemove = new ArrayList<>();
            for (int i = 0; i < segments.size() - 1; i++) {
                LogSegment seg = segments.get(i);
                LogSegment nextSeg = segments.get(i + 1);
                if (nextSeg.baseIndex <= index) {
                    toRemove.add(seg);
                } else {
                    break;
                }
            }

            for (LogSegment seg : toRemove) {
                segments.remove(seg);
                seg.close();
                if (!seg.file.delete()) {
                    throw new IOException("Failed to delete compacted segment file: " + seg.file.getAbsolutePath());
                }
            }

            if (!segments.isEmpty()) {
                minLogIndex.set(segments.get(0).baseIndex);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private LogSegment findSegment(long index) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            LogSegment seg = segments.get(i);
            if (seg.baseIndex <= index) {
                return seg;
            }
        }
        return null;
    }

    private String formatFileName(long baseIndex) {
        return String.format("%020d.wal", baseIndex);
    }

    private long parseBaseIndex(String fileName) {
        String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));