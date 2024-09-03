package dslabs.paxos;

// Your code here...

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import dslabs.paxos.PaxosServer.Ballot;
import dslabs.paxos.PaxosServer.LogEntry;
import java.util.Map;
import lombok.Data;

@Data
class Prepare implements Message {
    private final Ballot ballot;
}

@Data
class PrepareResponse implements Message {
    private final Ballot ballot;
    private final Map<Integer, LogEntry> log;
}

@Data
class Accept implements Message {
    private final Ballot ballot;
    private final int slot;
    private final AMOCommand amoCommand;
}

@Data
class AcceptResponse implements Message {
    private final Ballot ballot;
    private final int slot;
    private final PaxosLogSlotStatus status;
}

@Data
class Heartbeat implements Message {
    private final Ballot ballot;
    private final Map<Integer, LogEntry> log;
    private final int slotClear;
}

@Data
class HeartbeatResponse implements Message {
    private final int slotOut;
}
