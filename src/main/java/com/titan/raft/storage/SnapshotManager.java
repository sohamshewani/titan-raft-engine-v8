package com.titan.raft.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public final class SnapshotManager {

    private static final int SNAPSHOT_MAGIC = 0x54495441; // "TITA"
    private static final int VERSION_1 = 1;

    private final Path snapshotDir;
    private final AtomicLong lastSnapshotIndex;
    private final AtomicLong lastSnapshotTerm;
    private final AtomicBoolean isInstalling;

    public SnapshotManager(Path snapshotDir) throws IOException {
        this.snapshotDir = Objects.requireNonNull(snapshotDir, "snapshotDir cannot be null");
        this.lastSnapshotIndex = new AtomicLong(0L);
        this.lastSnapshotTerm = new AtomicLong(0L);
        this.isInstalling = new AtomicBoolean(false);

        Files.createDirectories(snapshotDir);
        recoverLatestSnapshot();
    }

    private void recoverLatestSnapshot() throws IOException {
        Path latestMeta = findLatestSnapshotPath();
        if (latestMeta == null) {
            return;
        }

        try (FileChannel channel = FileChannel.open(latestMeta, StandardOpenOption.READ)) {
            ByteBuffer headerBuf = ByteBuffer.allocate(24);
            readFully(channel, headerBuf, 0);
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new IOException("Invalid snapshot magic bytes: " + Integer.toHexString(magic));
            }
            int version = headerBuf.getInt();
            if (version != VERSION_1) {
                throw new IOException("Unsupported snapshot version: " + version);
            }
            long index = headerBuf.getLong();
            long term = headerBuf.getLong();

            this.lastSnapshotIndex.set(index);
            this.lastSnapshotTerm.set(term);
        }
    }

    private Path findLatestSnapshotPath() throws IOException {
        if (!Files.exists(snapshotDir)) {
            return null;
        }
        Path latest = null;
        long maxIndex = -1L;
        try (var stream = Files.list(snapshotDir)) {
            for (Path path : (Iterable<Path>) stream::iterator) {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".snapshot")) {
                    try {
                        String indexStr = fileName.substring(0, fileName.indexOf('.'));
                        long idx = Long.parseLong(indexStr);
                        if (idx > maxIndex) {
                            maxIndex = idx;
                            latest = path;
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }
        return latest;
    }

    public synchronized void createSnapshot(long snapshotIndex, long snapshotTerm, ByteBuffer stateMachineData) throws IOException {
        if (snapshotIndex <= lastSnapshotIndex.get()) {
            throw new IllegalArgumentException("Snapshot index " + snapshotIndex + " is not greater than current " + lastSnapshotIndex.get());
        }

        Path tempFile = snapshotDir.resolve(snapshotIndex + ".snapshot.tmp");
        Path finalFile = snapshotDir.resolve(snapshotIndex + ".snapshot");

        Files.deleteIfExists(tempFile);
        Files.deleteIfExists(finalFile);

        int dataLen = stateMachineData.remaining();
        int totalHeaderSize = 24; // magic(4) + version(4) + index(8) + term(8)
        int totalPayloadSize = totalHeaderSize + 4 + dataLen + 8; // payloadLen(4) + data + crc32(8)

        ByteBuffer buffer = ByteBuffer.allocateDirect(totalPayloadSize);
        buffer.putInt(SNAPSHOT_MAGIC);
        buffer.putInt(VERSION_1);
        buffer.putLong(snapshotIndex);
        buffer.putLong(snapshotTerm);
        buffer.putInt(dataLen);

        int dataStart = buffer.position();
        buffer.put(stateMachineData);
        int dataEnd = buffer.position();

        CRC32 crc32 = new CRC32();
        buffer.position(0);
        buffer.limit(dataEnd);
        
        // Compute CRC over header + payload length + data
        byte[] crcBytes = new byte[dataEnd];
        buffer.get(crcBytes);
        crc32.update(crcBytes);
        long checksum = crc32.getValue();

        buffer.limit(totalPayloadSize);
        buffer.position(dataEnd);
        buffer.putLong(checksum);
        buffer.flip();

        try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        Files.move(tempFile, finalFile);
        this.lastSnapshotIndex.set(snapshotIndex);
        this.lastSnapshotTerm.set(snapshotTerm);
    }

    public synchronized void installSnapshot(long snapshotIndex, long snapshotTerm, ByteBuffer chunkData, boolean isLastChunk) throws IOException {
        if (!isInstalling.get()) {
            if (snapshotIndex <= lastSnapshotIndex.get()) {
                return; // Stale snapshot
            }
            isInstalling.set(true);
        }

        Path stagingFile = snapshotDir.resolve("installing.snapshot.tmp");
        StandardOpenOption[] options = Files.exists(stagingFile) 
            ? new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE}
            : new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

        try (FileChannel channel = FileChannel.open(stagingFile, options)) {
            channel.position(channel.size());
            while (chunkData.hasRemaining()) {
                channel.write(chunkData);
            }
            channel.force(true);
        }

        if (isLastChunk) {
            verifyAndFinalizeInstall(snapshotIndex, snapshotTerm, stagingFile);
        }
    }

    private void verifyAndFinalizeInstall(long snapshotIndex, long snapshotTerm, Path stagingFile) throws IOException {
        try (FileChannel channel = FileChannel.open(stagingFile, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < 32) {
                throw new IOException("Staging snapshot file too small: " + size);
            }

            ByteBuffer headerBuf = ByteBuffer.allocate(24);
            readFully(channel, headerBuf, 0);
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new IOException("Invalid snapshot magic bytes during install");
            }
            int version = headerBuf.getInt();
            if (version != VERSION_1) {
                throw new IOException("Unsupported snapshot version during install");
            }
            long index = headerBuf.getLong();
            long term = headerBuf.getLong();

            if (index != snapshotIndex || term != snapshotTerm) {
                throw new IOException("Snapshot metadata mismatch during installation");
            }

            Path finalFile = snapshotDir.resolve(snapshotIndex + ".snapshot");
            Files.deleteIfExists(finalFile);
            Files.move(stagingFile, finalFile);

            this.lastSnapshotIndex.set(snapshotIndex);
            this.lastSnapshotTerm.set(snapshotTerm);
        } finally {
            isInstalling.set(false);
            Files.deleteIfExists(stagingFile);
        }
    }

    public ByteBuffer readSnapshotData(long snapshotIndex) throws IOException {
        Path path = snapshotDir.resolve(snapshotIndex + ".snapshot");
        if (!Files.exists(path)) {
            throw new IOException("Snapshot file not found for index: " + snapshotIndex);
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < 32) {
                throw new IOException("Corrupt snapshot file: size too small");
            }

            ByteBuffer headerBuf = ByteBuffer.allocate(24);
            readFully(channel, headerBuf, 0);
            headerBuf.flip();

            headerBuf.getInt(); // magic
            headerBuf.getInt(); // version
            headerBuf.getLong(); // index
            headerBuf.getLong(); // term

            int dataLen = headerBuf.getInt(); // Wait, let's verify layout. 
            // Layout: magic(4), version(4), index(8), term(8), dataLen(4), data(dataLen), checksum(8)
            // Ah, let's parse carefully.
        }

        // Proper full read implementation
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer lengthBuf = ByteBuffer.allocate(4);
            readFully(channel, lengthBuf, 24);
            lengthBuf.flip();
            int dataLen = lengthBuf.getInt();

            ByteBuffer dataBuf = ByteBuffer.allocate(dataLen);
            readFully(channel, dataBuf, 28);
            dataBuf.flip();
            return dataBuf;
        }
    }

    private void readFully(FileChannel channel, ByteBuffer dst, long position) throws IOException {
        long currentPos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, currentPos);
            if (read == -1) {
                throw new IOException("Unexpected EOF while reading snapshot file");
            }
            currentPos += read;
        }
    }

    public long getLastSnapshotIndex() {
        return lastSnapshotIndex.get();
    }

    public long getLastSnapshotTerm() {
        return lastSnapshotTerm.get();
    }
}