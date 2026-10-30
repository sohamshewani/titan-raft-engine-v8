package com.titan.raft.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.concurrent.Execution;
import org.junit.jupiter.api.concurrent.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Unit, Invariant, and Concurrent Stress-Test Suite for Titan Raft Engine.
 * Implements a fully compliant, self-contained Raft consensus engine mock and tests it rigorously
 * across boundary conditions, network partitions, multi-threaded concurrency, and failure recoveries.
 */
public class TitanRaftEngineTestSuite {

    // =========================================================================
    // PRODUCTION CORE MODEL: TITAN RAFT ENGINE IMPLEMENTATION
    // =========================================================================

    public enum NodeRole {
        FOLLOWER, CANDIDATE, LEADER
    }

    public static class LogEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long index;
        private final long term;
        private final byte[] command;

        public LogEntry(long index, long term, byte[] command) {
            this.index = index;
            this.term = term;
            this.command = command;
        }

        public long getIndex() { return index; }
        public long getTerm() { return term; }
        public byte[] getCommand() { return command; }
    }

    public static class AppendEntriesArgs implements Serializable {
        private static final long serialVersionUID = 1L;
        public long term;
        public String leaderId;
        public long prevLogIndex;
        public long prevLogTerm;
        public List<LogEntry> entries;
        public long leaderCommit;
    }

    public static class AppendEntriesReply implements Serializable {
        private static final long serialVersionUID = 1L;
        public long term;
        public boolean success;
        public long matchIndex;
    }

    public static class RequestVoteArgs implements Serializable {
        private static final long serialVersionUID = 1L;
        public long term;
        public String candidateId;
        public long lastLogIndex;
        public long lastLogTerm;
    }

    public static class RequestVoteReply implements Serializable {
        private static final long serialVersionUID = 1L;
        public long term;
        public boolean voteGranted;
    }

    public interface ClusterTransport {
        AppendEntriesReply sendAppendEntries(String targetNode, AppendEntriesArgs args) throws IOException;
        RequestVoteReply sendRequestVote(String targetNode, RequestVoteArgs args) throws IOException;
    }

    public interface StateMachine {
        void apply(byte[] command);
        byte[] snapshot();
        void restore(byte[] snapshot);
    }

    public static class RaftNode {
        private final String id;
        private final List<String> peerIds;
        private final ClusterTransport transport;
        private final StateMachine stateMachine;
        private final Path storagePath;
        private final ReentrantLock lock = new ReentrantLock();

        // Persistent state on all servers
        private long currentTerm = 0;
        private String votedFor = null;
        private final List<LogEntry> log = new ArrayList<>();

        // Volatile state on all servers
        private long commitIndex = 0;
        private long lastApplied = 0;
        private NodeRole role = NodeRole.FOLLOWER;
        private String leaderId = null;

        // Volatile state on leaders
        private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
        private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

        // Timestamps & concurrency flags
        private final AtomicLong lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        private final Logger logger;

        public RaftNode(String id, List<String> peerIds, ClusterTransport transport, StateMachine stateMachine, Path storagePath) {
            this.id = id;
            this.peerIds = peerIds;
            this.transport = transport;
            this.stateMachine = stateMachine;
            this.storagePath = storagePath;
            this.logger = Logger.getLogger("RaftNode-" + id);
            this.logger.setLevel(Level.WARNING);

            // Dummy initial log entry at index 0 for indexing safety
            log.add(new LogEntry(0, 0, new byte[0]));
            loadState();

            // Background loops
            scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 50, 50, TimeUnit.MILLISECONDS);
            scheduler.scheduleAtFixedRate(this::sendHeartbeats, 50, 50, TimeUnit.MILLISECONDS);
        }

        public void stop() {
            running.set(false);
            scheduler.shutdownNow();
        }

        public String getId() { return id; }
        public NodeRole getRole() { lock.lock(); try { return role; } finally { lock.unlock(); } }
        public long getCurrentTerm() { lock.lock(); try { return currentTerm; } finally { lock.unlock(); } }
        public long getCommitIndex() { lock.lock(); try { return commitIndex; } finally { lock.unlock(); } }
        public long getLastApplied() { lock.lock(); try { return lastApplied; } finally { lock.unlock(); } }
        public String getLeaderId() { lock.lock(); try { return leaderId; } finally { lock.unlock(); } }

        public int getLogSize() {
            lock.lock();
            try {
                return log.size();
            } finally {
                lock.unlock();
            }
        }

        public CompletableFuture<Boolean> clientRequest(byte[] command) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            lock.lock();
            try {
                if (role != NodeRole.LEADER) {
                    future.complete(false);
                    return future;
                }
                long index = log.get(log.size() - 1).index + 1;
                log.add(new LogEntry(index, currentTerm, command));
                persistState();
                matchIndex.put(id, index);
                nextIndex.put(id, index + 1);
                
                // Immediately trigger replication attempt
                CompletableFuture.runAsync(this::sendHeartbeats);
                future.complete(true); // Accepted into leader's log
            } finally {
                lock.unlock();
            }
            return future;
        }

        private void checkElectionTimeout() {
            if (!running.get()) return;
            lock.lock();
            try {
                if (role == NodeRole.LEADER) return;
                long now = System.currentTimeMillis();
                long timeout = 150 + (Math.abs(id.hashCode()) % 150); // randomized timeout
                if (now - lastHeartbeatTime.get() > timeout) {
                    startElection();
                }
            } finally {
                lock.unlock();
            }
        }

        private void startElection() {
            role = NodeRole.CANDIDATE;
            currentTerm++;
            votedFor = id;
            lastHeartbeatTime.set(System.currentTimeMillis());
            persistState();

            long termToVote = currentTerm;
            long lastLogIdx = log.get(log.size() - 1).index;
            long lastLogTrm = log.get(log.size() - 1).term;

            List<String> peers = new ArrayList<>(peerIds);
            lock.unlock(); // Release lock during network calls

            AtomicInteger votes = new AtomicInteger(1);
            ExecutorService voteExec = Executors.newFixedThreadPool(Math.max(1, peers.size()));

            for (String peer : peers) {
                voteExec.submit(() -> {
                    RequestVoteArgs args = new RequestVoteArgs();
                    args.term = termToVote;
                    args.candidateId = id;
                    args.lastLogIndex = lastLogIdx;
                    args.lastLogTerm = lastLogTrm;
                    try {
                        RequestVoteReply reply = transport.sendRequestVote(peer, args);
                        lock.lock();
                        try {
                            if (role == NodeRole.CANDIDATE && currentTerm == termToVote) {
                                if (reply.term > currentTerm) {
                                    becomeFollower(reply.term);
                                    return;
                                }
                                if (reply.voteGranted) {
                                    if (votes.incrementAndGet() > (peerIds.size() + 1) / 2) {
                                        becomeLeader();
                                    }
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception ignored) {
                    }
                });
            }
            voteExec.shutdown();
            lock.lock(); // Reacquire lock for method exit
        }

        private void becomeFollower(long term) {
            currentTerm = term;
            role = NodeRole.FOLLOWER;
            votedFor = null;
            leaderId = null;
            persistState();
        }

        private void becomeLeader() {
            role = NodeRole.LEADER;
            leaderId = id;
            long lastIdx = log.get(log.size() - 1).index;
            for (String peer : peerIds) {
                nextIndex.put(peer, lastIdx + 1);
                matchIndex.put(peer, 0L);
            }
            matchIndex.put(id, lastIdx);
            nextIndex.put(id, lastIdx + 1);
            sendHeartbeats();
        }

        private void sendHeartbeats() {
            if (!running.get()) return;
            lock.lock();
            try {
                if (role != NodeRole.LEADER) return;
                long term = currentTerm;
                String lid = id;
                long commitIdx = commitIndex;

                Map<String, Long> peersMap = new HashMap<>(nextIndex);
                lock.unlock(); // Release lock during network dispatch

                ExecutorService hbExec = Executors.newFixedThreadPool(Math.max(1, peerIds.size()));
                for (String peer : peerIds) {
                    hbExec.submit(() -> {
                        lock.lock();
                        if (role != NodeRole.LEADER) {
                            lock.unlock();
                            return;
                        }
                        long pNext = peersMap.getOrDefault(peer, log.get(log.size() - 1).index + 1);
                        long prevIndex = pNext - 1;
                        long prevTerm = 0;
                        for (int i = log.size() - 1; i >= 0; i--) {
                            if (log.get(i).index == prevIndex) {
                                prevTerm = log.get(i).term;
                                break;
                            }
                        }
                        List<LogEntry> entriesToSend = new ArrayList<>();
                        for (LogEntry entry : log) {
                            if (entry.index >= pNext) {
                                entriesToSend.add(entry);
                            }
                        }
                        AppendEntriesArgs args = new AppendEntriesArgs();
                        args.term = term;
                        args.leaderId = lid;
                        args.prevLogIndex = prevIndex;
                        args.prevLogTerm = prevTerm;
                        args.entries = entriesToSend;
                        args.leaderCommit = commitIdx;
                        lock.unlock();

                        try {
                            AppendEntriesReply reply = transport.sendAppendEntries(peer, args);
                            lock.lock();
                            try {
                                if (role == NodeRole.LEADER && currentTerm == term) {
                                    if (reply.term > currentTerm) {
                                        becomeFollower(reply.term);
                                        return;
                                    }
                                    if (reply.success) {
                                        long maxMatch = prevIndex + entriesToSend.size();
                                        matchIndex.put(peer, Math.max(matchIndex.getOrDefault(peer, 0L), maxMatch));
                                        nextIndex.put(peer, maxMatch + 1);
                                        checkAndUpdateCommitIndex();
                                    } else {
                                        nextIndex.put(peer, Math.max(1L, nextIndex.getOrDefault(peer, 2L) - 1));
                                    }
                                }