package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import dslabs.shardkv.PaxosDecision;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Log
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;

    // Your code here...


    @Data
    @AllArgsConstructor
    public static class Ballot implements Comparable<Ballot>, Serializable {
        private int bnum;
        private Address proposer;

        @Override
        public int compareTo(Ballot other) {
            if (this.bnum < other.bnum) {
                return -1;
            } else if (this.bnum > other.bnum) {
                return 1;
            } else {
                return this.proposer.compareTo(other.proposer);
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class LogEntry implements Serializable {
        private Ballot ballot;
        private PaxosLogSlotStatus status;
        private AMOCommand amoCommand;
    }

    private AMOApplication<Application> app;
    private boolean isActive;
    private int slotIn;
    private int slotOut;
    private Map<Integer, LogEntry> log;
    private Set<Address> followers;
    private Map<Integer, Set<Address>> votes;
    private Ballot ballot;
    private Address heartbeat;

    private Map<Address, Integer> cleared;
    private int slotClear;
    private Address parentAddress;


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // Your code here...
        this.app = new AMOApplication<>(app);
        this.parentAddress = null;
        isActive = true;
        slotIn = 1;
        slotOut = 1;
        log = new HashMap<>();
        followers = new HashSet<>();
        votes = new HashMap<>();
        ballot = new Ballot(0, this.address());
        heartbeat = null;
        cleared = new HashMap<>();
        slotClear = 1;
    }

    public PaxosServer(Address address, Address[] servers, Address parentAddress) {
        super(address); // 'address' is the address of this node
        this.servers = servers;
        this.parentAddress = parentAddress;
        this.app = null;
        isActive = true;
        slotIn = 1;
        slotOut = 1;
        log = new HashMap<>();
        followers = new HashSet<>();
        votes = new HashMap<>();
        ballot = new Ballot(0, this.address());
        heartbeat = null;
        cleared = new HashMap<>();
        slotClear = 1;
    }

    @Override
    public void init() {
        // Your code here...
        sendToPaxosCluster(new Prepare(ballot));
        followers.add(this.address());
        cleared.put(this.address(), slotOut);
        set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);
        set(new PrepareTimer(ballot), PrepareTimer.PREPARE_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the server's local log.
     *
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     *
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        if (!log.containsKey(logSlotNum)) {
            if (logSlotNum < slotClear) {
                return PaxosLogSlotStatus.CLEARED;
            } else {
                return PaxosLogSlotStatus.EMPTY;
            }
        } else {
            return log.get(logSlotNum).status;
        }
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     *
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     *
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     *
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        // Your code here...
        if (!log.containsKey(logSlotNum)) {
            return null;
        } else {
            if (log.get(logSlotNum).amoCommand == null) {
                return null;
            } else {
                return log.get(logSlotNum).amoCommand.command();
            }
        }
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        // Your code here...
        return slotClear;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        // Your code here...
        return slotIn - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/

    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // Your code here...

        if (m.amoCommand().sequenceNum() == -1) {
            if (app != null) {
                AMOResult res = new AMOResult(app.executeReadOnly(m.amoCommand().command()), -1);
                send(new PaxosReply(res), m.amoCommand().address());
            } else {
                handleMessage(new PaxosDecision(m.amoCommand().command()), this.parentAddress);
            }
            return;
        }
        AMOCommand clientC = m.amoCommand();
        for (int i = slotClear; i < slotOut; i++) {
            AMOCommand logC = log.get(i).amoCommand();
            if (logC != null && (logC.sequenceNum() == -2 || (logC.sequenceNum() == clientC.sequenceNum() &&
                    logC.address().equals(clientC.address())))) {
                // if no Objects.equal, then two reconfig commands which will have the same seq# -2,
                // and might have same address will be sent back as Decision, even if they are different commands
                if (Objects.equal(logC.command(), clientC.command())) {
                    if (app != null) {
                        AMOResult res = app.execute(logC);
                        send(new PaxosReply(res), logC.address());
                    } else {
                        handleMessage(new PaxosDecision(logC.command()), this.parentAddress);
                    }
                    return;
                }
            }
        }
        if (isActive && followers.size() >= ((servers.length / 2) + 1)) {
            for (int i = slotOut; i < slotIn; i++) {  // check if the request has been proposed in a slot
                AMOCommand logC = log.get(i).amoCommand();
                if (logC != null && (logC.sequenceNum() == -2 || (logC.sequenceNum() == clientC.sequenceNum() &&
                        logC.address().equals(clientC.address())))) {
                    if (Objects.equal(logC.command(), clientC.command())) {
                        return;
                    }
                }
            }
            // new request that is not in the log nor has it already been proposed
            log.put(slotIn, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand()));
            HashSet<Address> haveVoted = new HashSet<>();
            haveVoted.add(this.address());
            votes.put(slotIn, haveVoted);
            // for one server PAXOS
            if (haveVoted.size() == ((servers.length / 2) + 1)) {
                log.get(slotIn).status = PaxosLogSlotStatus.CHOSEN;
                executeLog();
            } else {
                sendToPaxosCluster(new Accept(ballot, slotIn, m.amoCommand()));
            }
            slotIn++;
        }
    }

    // Your code here...

    private void handlePrepare(Prepare m, Address sender) {
        if (m.ballot().compareTo(ballot) > 0) {
            heartbeat = sender;
            submitToLeader(m.ballot());
        }
        send(new PrepareResponse(ballot, log), sender);
    }

    private void handlePrepareResponse(PrepareResponse m, Address sender) {
        if (isActive) {
            if (m.ballot().compareTo(ballot) == 0) {
                mergeLog(m.log());
                executeLog();
                followers.add(sender);
                if (followers.size() >= ((servers.length / 2) + 1)) {
                    updateProposalsAndFillHoles();
                }
                if (followers.size() == ((servers.length / 2) + 1)) {
                    for (int slot = slotOut; slot < slotIn; slot++) {
                        if (log.get(slot).status != PaxosLogSlotStatus.CHOSEN) {
                            sendToPaxosCluster(new Accept(ballot, slot, log.get(slot).amoCommand));
                        }
                    }
                    set(new AcceptTimer(ballot), AcceptTimer.ACCEPT_RETRY_MILLIS);
                }
            } else if (m.ballot().compareTo(ballot) > 0) {
                heartbeat = null;
                submitToLeader(m.ballot());
            }
        }
    }

    private void handleAccept(Accept m, Address sender) {
        if (m.ballot().compareTo(this.ballot) >= 0) {
            if (m.ballot().compareTo(this.ballot) > 0) {
                submitToLeader(m.ballot());
            }
            heartbeat = sender;
            if (log.containsKey(m.slot())
                    && log.get(m.slot()).status == PaxosLogSlotStatus.CHOSEN) {
                send(new AcceptResponse(ballot, m.slot(), PaxosLogSlotStatus.CHOSEN), sender);
            } else if (m.slot() >= slotClear) {
                log.put(m.slot(), new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand()));
                if (slotIn <= m.slot()) {
                    slotIn = m.slot() + 1;
                }
                send(new AcceptResponse(ballot, m.slot(), PaxosLogSlotStatus.ACCEPTED), sender);
            }
        } else {
            send(new AcceptResponse(ballot, m.slot(), null), sender);
        }
    }

    private void handleAcceptResponse(AcceptResponse m, Address sender) {
        if (isActive && m.ballot().compareTo(ballot) == 0) {
            if (m.status() != null && m.slot() >= slotClear && log.get(m.slot()).status != PaxosLogSlotStatus.CHOSEN) {
                Set<Address> haveVoted = votes.get(m.slot());
                haveVoted.add(sender);
                if ((haveVoted.size() == ((servers.length / 2) + 1) || m.status() == PaxosLogSlotStatus.CHOSEN)) {
                    log.get(m.slot()).status = PaxosLogSlotStatus.CHOSEN;
                    executeLog();
                }
            }
        } else if (isActive && m.ballot().compareTo(ballot) > 0) {
            heartbeat = null;
            submitToLeader(m.ballot());
        }
    }

    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (m.ballot().compareTo(ballot) >= 0) {
            heartbeat = sender;
            mergeLog(m.log());
            executeLog();
            if (m.slotClear() > slotClear) {
                for (int i = slotClear; i < m.slotClear(); i++) {
                    log.remove(i);
                    votes.remove(i);
                }
                slotClear = m.slotClear();
            }
            if (m.ballot().compareTo(ballot) > 0) {
                submitToLeader(m.ballot());
            }
            sendToPaxosCluster(new HeartbeatResponse(slotOut));
        }
    }

    private void handleHeartbeatResponse(HeartbeatResponse m, Address sender) {
        if (isActive) {
            if (!cleared.containsKey(sender)) {
                cleared.put(sender, m.slotOut());
            } else if (m.slotOut() > cleared.get(sender)) {
                cleared.put(sender, m.slotOut());
            }
            if (cleared.size() == servers.length) {
                int safeOut = Collections.min(cleared.values());
                if (safeOut > slotClear) {
                    for (int i = slotClear; i < safeOut; i++) {
                        log.remove(i);
                        votes.remove(i);
                    }
                    slotClear = safeOut;
                }
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // Your code here...

    private void onPrepareTimer(PrepareTimer t) {
        if (isActive && t.ballot.compareTo(ballot) == 0 && followers.size() < ((servers.length / 2) + 1)) {
            ballot = new Ballot(ballot.bnum + 1, this.address());
            sendToPaxosCluster(new Prepare(ballot));
            set(new PrepareTimer(ballot), PrepareTimer.PREPARE_RETRY_MILLIS);
        }
    }
    private void onAcceptTimer(AcceptTimer t) {
        if (isActive && t.ballot.compareTo(ballot) == 0) {
            for (int slot : log.keySet()) {
                if (log.get(slot).status != PaxosLogSlotStatus.CHOSEN) {
                    sendToPaxosCluster(new Accept(ballot, slot, log.get(slot).amoCommand));
                }
            }
            set(t, AcceptTimer.ACCEPT_RETRY_MILLIS);
        }
    }
    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (isActive) {
            sendToPaxosCluster(new Heartbeat(ballot, log, slotClear));
        }
        set(t, HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);
    }

    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        if (!isActive && t.ballot().compareTo(ballot) == 0) {
            if (!ballot.proposer.equals(heartbeat)) {
                // seek election
                isActive = true;
                heartbeat = null;
                votes = new HashMap<>();
                followers = new HashSet<>();
                ballot = new Ballot(ballot.bnum + 1, this.address());
                sendToPaxosCluster(new Prepare(ballot));
                followers.add(this.address());
                set(new PrepareTimer(ballot), PrepareTimer.PREPARE_RETRY_MILLIS);
            } else { // heartbeat != null
                heartbeat = null;
                set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
            }
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // Your code here...

    private void submitToLeader(Ballot leader) {
        ballot = leader;
        isActive = false;
        followers = new HashSet<>();
        votes = new HashMap<>();
        set(new HeartbeatCheckTimer(ballot), HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
    }

    private void mergeLog(Map<Integer, LogEntry> otherLog) {
        for (int slot : otherLog.keySet()) {
            if (slot >= slotOut && slot >= slotClear) {
                LogEntry entry = otherLog.get(slot);
                if (!log.containsKey(slot)) {
                    log.put(slot, entry);
                    if (isActive && log.get(slot).status != PaxosLogSlotStatus.CHOSEN) {
                        Set<Address> haveVoted = new HashSet<>();
                        haveVoted.add(this.address());
                        votes.put(slot, haveVoted);
                    }
                } else if (entry.status == PaxosLogSlotStatus.CHOSEN) {
                    log.put(slot, entry);
                } else if (log.get(slot).status != PaxosLogSlotStatus.CHOSEN &&
                        entry.ballot.compareTo(log.get(slot).ballot) > 0) {
                    log.put(slot, entry);
                }
                if (slotIn <= slot) {
                    slotIn = slot + 1;
                }
            }
        }
    }

    private void updateProposalsAndFillHoles() {
        for (int i = slotOut; i < slotIn; i++) {
            if (!log.containsKey(i)) {
                log.put(i, new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, null));
            } else {
                log.get(i).ballot = this.ballot;
            }
            if (!votes.containsKey(i) && log.get(i).status != PaxosLogSlotStatus.CHOSEN) {
                Set<Address> haveVoted = new HashSet<>();
                haveVoted.add(this.address());
                votes.put(i, haveVoted);
            }
        }
    }

    private void sendToPaxosCluster(Message m) {
        for (Address server : servers) {
            if (!server.equals(this.address())) {
                send(m, server);
            }
        }
    }

    private void executeLog() {
        while (log.containsKey(slotOut) && log.get(slotOut).status == PaxosLogSlotStatus.CHOSEN) {
            if (log.get(slotOut).amoCommand != null) {
                if (app != null) {
                    AMOResult res = app.execute(log.get(slotOut).amoCommand);
                    send(new PaxosReply(res), log.get(slotOut).amoCommand.address());
                } else {
                    handleMessage(new PaxosDecision(log.get(slotOut).amoCommand.command()), this.parentAddress);
                }
            }
            slotOut++;
        }
        cleared.put(this.address(), slotOut);
    }
}