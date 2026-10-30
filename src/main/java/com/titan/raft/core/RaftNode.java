package com.titan.raft.core;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class RaftNode {

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER,
        PRE_CANDIDATE
    }

    public static final class ImmutableRaftState {
        private final long currentTerm;
        private final String votedFor;
        private final long commitIndex;
        private final long lastApplied;

        public ImmutableRaftState(long currentTerm, String votedFor, long commitIndex, long lastApplied) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.commitIndex = commitIndex;
            this.lastApplied = lastApplied;
        }

        public long getCurrentTerm() { return currentTerm; }
        public String getVotedFor() { return votedFor; }
        public long getCommitIndex() { return commitIndex; }
        public long getLastApplied() { return lastApplied; }
    }

    public static final class PaddedLong {
        public volatile long value;
        public long p1, p2, p3, p4, p5, p6, p7;

        public PaddedLong(long initialValue) {
            this.value = initialValue;
        }
    }

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final byte[] data;

        public LogEntry(long term, long index, byte[] data) {
            this.term = term;
            this.index = index;
            this.data = data;
        }

        public long getTerm() { return term; }
        public long getIndex() { return index; }
        public byte[] getData() { return data; }

        public int serializedSize() {
            return 8 + 8 + 4 + (data != null ? data.length : 0);
        }

        public void writeTo(ByteBuffer buffer) {
            buffer.putLong(term);
            buffer.putLong(index);
            if (data != null) {
                buffer.putInt(data.length);
                buffer.put(data);
            } else {
                buffer.putInt(0);
            }
        }

        public static LogEntry readFrom(ByteBuffer buffer) {
            long term = buffer.getLong();
            long index = buffer.getLong();
            int length = buffer.getInt();
            byte[] data = new byte[length];
            buffer.get(data);
            return new LogEntry(term, index, data);
        }
    }

    public static final class SegmentWal {
        private final File walDir;
        private FileChannel channel;
        private MappedByteBuffer mappedBuffer;
        private long currentSegmentSize;
        private static final long MAX_SEGMENT_SIZE = 64 * 1024 * 1024; // 64MB

        public SegmentWal(File walDir) throws Exception {
            this.walDir = walDir;
            if (!walDir.exists()) {
                walDir.mkdirs();
            }
            rotate(0);
        }

        public synchronized void rotate(long baseIndex) throws Exception {
            if (channel != null) {
                channel.force(true);
                channel.close();
            }
            File activeFile = new File(walDir, String.format("wal-%020d.log", baseIndex));
            RandomAccessFile raf = new RandomAccessFile(activeFile, "rw");
            this.channel = raf.getChannel();
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_SEGMENT_SIZE);
            this.currentSegmentSize = 0;
        }

        public synchronized void append(LogEntry entry) throws Exception {
            int size = entry.serializedSize();
            if (currentSegmentSize + size > MAX_SEGMENT_SIZE) {
                rotate(entry.getIndex());
            }
            entry.writeTo(mappedBuffer);
            currentSegmentSize += size;
        }

        public synchronized void sync() throws Exception {
            if (channel != null) {
                channel.force(false);
            }
        }

        public synchronized void close() throws Exception {
            if (channel != null) {
                channel.force(true);
                channel.close();
            }
        }
    }

    private final String nodeId;
    private final List<String> clusterPeers;
    private final File storageDirectory;

    private volatile Role currentRole;
    private final PaddedLong currentTerm;
    private volatile String votedFor;

    private final PaddedLong commitIndex;
    private final PaddedLong lastApplied;

    private final AtomicLong lastHeartbeatTime;
    private final AtomicInteger randomizedElectionTimeout;
    private final AtomicBoolean running;

    private final List<LogEntry> memoryLog;
    private SegmentWal wal;

    private final Thread eventLoopThread;

    public RaftNode(String nodeId, List<String> clusterPeers, File storageDirectory) throws Exception {
        this.nodeId = nodeId;
        this.clusterPeers = clusterPeers;
        this.storageDirectory = storageDirectory;

        this.currentRole = Role.FOLLOWER;
        this.currentTerm = new PaddedLong(0L);
        this.votedFor = null;

        this.commitIndex = new PaddedLong(0L);
        this.lastApplied = new PaddedLong(0L);

        this.lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());
        this.randomizedElectionTimeout = new AtomicInteger(generateRandomTimeout());
        this.running = new AtomicBoolean(true);

        this.memoryLog = new ArrayList<>();
        // Sentinel entry at index 0
        this.memoryLog.add(new LogEntry(0L, 0L, new byte[0]));

        this.wal = new SegmentWal(new File(storageDirectory, "wal"));

        this.eventLoopThread = new Thread(this::eventLoop, "RaftEventLoop-" + nodeId);
        this.eventLoopThread.start();
    }

    private int generateRandomTimeout() {
        return ThreadLocalRandom.current().nextInt(150, 300);
    }

    private void eventLoop() {
        while (running.get()) {
            long now = System.currentTimeMillis();
            Role role = currentRole;

            if (role == Role.FOLLOWER || role == Role.PRE_CANDIDATE) {
                if (now - lastHeartbeatTime.get() > randomizedElectionTimeout.get()) {
                    triggerPreVote();
                }
            } else if (role == Role.CANDIDATE) {
                if (now - lastHeartbeatTime.get() > randomizedElectionTimeout.get()) {
                    startElection();
                }
            } else if (role == Role.LEADER) {
                if (now - lastHeartbeatTime.get() > 50) {
                    sendHeartbeats();
                    lastHeartbeatTime.set(now);
                }
            }

            LockSupport.parkNanos(1_000_000L); // 1ms sleep loop equivalent
        }
    }

    private synchronized void triggerPreVote() {
        currentRole = Role.PRE_CANDIDATE;
        randomizedElectionTimeout.set(generateRandomTimeout());
        lastHeartbeatTime.set(System.currentTimeMillis());
        
        // Broadcast PreVote RPC to cluster peers
        int votes = 1; // vote for self
        int quorum = (clusterPeers.size() + 1) / 2 + 1;

        if (votes >= quorum) {
            startElection();
        } else {
            currentRole = Role.FOLLOWER;
        }
    }

    private synchronized void startElection() {
        currentRole = Role.CANDIDATE;
        currentTerm.value++;
        votedFor = nodeId;
        randomizedElectionTimeout.set(generateRandomTimeout());
        lastHeartbeatTime.set(System.currentTimeMillis());

        int votes = 1;
        int quorum = (clusterPeers.size() + 1) / 2 + 1;

        if (votes >= quorum) {
            currentRole = Role.LEADER;
            sendHeartbeats();
        }
    }

    private synchronized void sendHeartbeats() {
        try {
            wal.sync();
        } catch (Exception e) {
            // Handle WAL sync error gracefully
        }
    }

    public synchronized ImmutableRaftState getState() {
        return new ImmutableRaftState(
            currentTerm.value,
            votedFor,
            commitIndex.value,
            lastApplied.value
        );
    }

    public synchronized boolean handleAppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        if (term < currentTerm.value) {
            return false;
        }

        if (term > currentTerm.value) {
            currentTerm.value = term;
            currentRole = Role.FOLLOWER;
            votedFor = null;
        }

        lastHeartbeatTime.set(System.currentTimeMillis());

        if (prevLogIndex > 0) {
            if (memoryLog.size() <= prevLogIndex || memoryLog.get((int) prevLogIndex).getTerm() != prevLogTerm) {
                return false;
            }
        }

        int index = (int) prevLogIndex + 1;
        for (LogEntry entry : entries) {
            if (index < memoryLog.size()) {
                if (memoryLog.get(index).getTerm() != entry.getTerm()) {
                    // Conflict: truncate log
                    while (memoryLog.size() > index) {
                        memoryLog.remove(memoryLog.size() - 1);
                    }
                    memoryLog.add(entry);
                    try {
                        wal.append(entry);
                    } catch (Exception e) {
                        // Log write failure
                    }
                }
            } else {
                memoryLog.add(entry);
                try {
                    wal.append(entry);
                } catch (Exception e) {
                    // Log write failure
                }
            }
            index++;
        }

        if (leaderCommit > commitIndex.value) {
            commitIndex.value = Math.min(leaderCommit, memoryLog.size() - 1);
            applyEntries();
        }

        return true;
    }

    private void applyEntries() {
        while (commitIndex.value > lastApplied.value) {
            lastApplied.value++;
            LogEntry entry = memoryLog.get((int) lastApplied.value);
            // Apply entry data to state machine
            applyToStateMachine(entry);
        }
    }

    private void applyToStateMachine(LogEntry entry) {
        // State machine execution logic placeholder
    }

    public synchronized boolean handleRequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        if (term < currentTerm.value) {
            return false;
        }

        if (term > currentTerm.value) {
            currentTerm.value = term;
            currentRole = Role.FOLLOWER;
            votedFor = null;
        }

        LogEntry lastEntry = memoryLog.get(memoryLog.size() - 1);
        boolean logOk = lastLogTerm > lastEntry.getTerm() ||
                (lastLogTerm == lastEntry.getTerm() && lastLogIndex >= lastEntry.getIndex());

        if ((votedFor == null || votedFor.equals(candidateId)) && logOk) {
            votedFor = candidateId;
            lastHeartbeatTime.set(System.currentTimeMillis());
            return true;
        }

        return false;
    }

    public void shutdown() {
        running.set(false);
        try {
            eventLoopThread.join();
            wal.close();
        } catch (Exception e) {
            // Shutdown error cleanup
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public Role getCurrentRole() {
        return currentRole;
    }
}