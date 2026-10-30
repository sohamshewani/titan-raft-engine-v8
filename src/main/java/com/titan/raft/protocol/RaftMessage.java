package com.titan.raft.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * High-performance, zero-allocation Raft message wrapper optimized for direct byte buffers
 * and off-heap memory arenas within the Titan Raft Engine.
 * 
 * Complies with strict lock-free event loop and memory layout specifications.
 */
public final class RaftMessage {

    public enum MessageType {
        APPEND_ENTRIES_REQUEST(1),
        APPEND_ENTRIES_RESPONSE(2),
        REQUEST_VOTE_REQUEST(3),
        REQUEST_VOTE_RESPONSE(4),
        PRE_VOTE_REQUEST(5),
        PRE_VOTE_RESPONSE(6),
        INSTALL_SNAPSHOT(7);

        private final int id;

        MessageType(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static MessageType fromId(int id) {
            for (MessageType type : values()) {
                if (type.id == id) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown MessageType ID: " + id);
        }
    }

    private final ByteBuffer backingBuffer;
    private final MessageType type;
    private final long term;
    private final long sourceId;
    private final long targetId;
    private final long index;
    private final long commitIndex;
    private final boolean successOrGranted;
    private final int payloadLength;
    private final int payloadOffset;

    private RaftMessage(
            ByteBuffer backingBuffer,
            MessageType type,
            long term,
            long sourceId,
            long targetId,
            long index,
            long commitIndex,
            boolean successOrGranted,
            int payloadLength,
            int payloadOffset) {
        this.backingBuffer = backingBuffer;
        this.type = type;
        this.term = term;
        this.sourceId = sourceId;
        this.targetId = targetId;
        this.index = index;
        this.commitIndex = commitIndex;
        this.successOrGranted = successOrGranted;
        this.payloadLength = payloadLength;
        this.payloadOffset = payloadOffset;
    }

    public static RaftMessage wrap(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "Backing buffer cannot be null");
        ByteBuffer slice = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        
        int magic = slice.getInt();
        if (magic != 0x5449544E) { // "TITN"
            throw new IllegalArgumentException("Invalid RaftMessage magic header: " + Integer.toHexString(magic));
        }

        int typeId = slice.getInt();
        MessageType type = MessageType.fromId(typeId);
        long term = slice.getLong();
        long sourceId = slice.getLong();
        long targetId = slice.getLong();
        long index = slice.getLong();
        long commitIndex = slice.getLong();
        byte flags = slice.get();
        boolean successOrGranted = (flags & 0x01) != 0;
        int payloadLength = slice.getInt();
        int payloadOffset = slice.position();

        return new RaftMessage(
                slice,
                type,
                term,
                sourceId,
                targetId,
                index,
                commitIndex,
                successOrGranted,
                payloadLength,
                payloadOffset
        );
    }

    public static void write(
            ByteBuffer destination,
            MessageType type,
            long term,
            long sourceId,
            long targetId,
            long index,
            long commitIndex,
            boolean successOrGranted,
            ByteBuffer payload) {
        Objects.requireNonNull(destination, "Destination buffer cannot be null");
        Objects.requireNonNull(type, "MessageType cannot be null");

        int payloadLen = payload != null ? payload.remaining() : 0;
        int requiredCapacity = 4 + 4 + 8 + 8 + 8 + 8 + 8 + 1 + 4 + payloadLen;

        if (destination.remaining() < requiredCapacity) {
            throw new BufferOverflowException(
                    "Insufficient buffer capacity. Required: " + requiredCapacity + ", Remaining: " + destination.remaining());
        }

        destination.order(ByteOrder.LITTLE_ENDIAN);
        destination.putInt(0x5449544E); // "TITN"
        destination.putInt(type.getId());
        destination.putLong(term);
        destination.putLong(sourceId);
        destination.putLong(targetId);
        destination.putLong(index);
        destination.putLong(commitIndex);
        
        byte flags = 0;
        if (successOrGranted) {
            flags = (byte) (flags | 0x01);
        }
        destination.put(flags);
        destination.putInt(payloadLen);

        if (payload != null && payloadLen > 0) {
            ByteBuffer payloadDup = payload.duplicate();
            destination.put(payloadDup);
        }
    }

    public MessageType getType() {
        return type;
    }

    public long getTerm() {
        return term;
    }

    public long getSourceId() {
        return sourceId;
    }

    public long getTargetId() {
        return targetId;
    }

    public long getIndex() {
        return index;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public boolean isSuccessOrGranted() {
        return successOrGranted;
    }

    public int getPayloadLength() {
        return payloadLength;
    }

    public ByteBuffer getPayload() {
        if (payloadLength <= 0) {
            return null;
        }
        ByteBuffer payloadSlice = backingBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        payloadSlice.position(payloadOffset);
        payloadSlice.limit(payloadOffset + payloadLength);
        return payloadSlice.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    public ByteBuffer getBackingBuffer() {
        return backingBuffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RaftMessage that = (RaftMessage) o;
        return term == that.term &&
                sourceId == that.sourceId &&
                targetId == that.targetId &&
                index == that.index &&
                commitIndex == that.commitIndex &&
                successOrGranted == that.successOrGranted &&
                payloadLength == that.payloadLength &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, term, sourceId, targetId, index, commitIndex, successOrGranted, payloadLength);
    }

    @Override
    public String toString() {
        return "RaftMessage{" +
                "type=" + type +
                ", term=" + term +
                ", sourceId=" + sourceId +
                ", targetId=" + targetId +
                ", index=" + index +
                ", commitIndex=" + commitIndex +
                ", successOrGranted=" + successOrGranted +
                ", payloadLength=" + payloadLength +
                '}';
    }

    public static class BufferOverflowException extends RuntimeException {
        public BufferOverflowException(String message) {
            super(message);
        }
    }
}