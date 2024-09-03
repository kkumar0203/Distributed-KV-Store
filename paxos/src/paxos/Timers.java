package dslabs.paxos;

import dslabs.framework.Timer;
import dslabs.paxos.PaxosServer.Ballot;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // Your code here...
    final int sequenceNum;
}

// Your code here...

@Data
final class PrepareTimer implements Timer {
    static final int PREPARE_RETRY_MILLIS = 100;
    final Ballot ballot;
}

@Data
final class AcceptTimer implements Timer {
    static final int ACCEPT_RETRY_MILLIS = 100;
    final Ballot ballot;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_RETRY_MILLIS = 25;
}

@Data
final class HeartbeatCheckTimer implements Timer {
    static final int HEARTBEAT_CHECK_RETRY_MILLIS = 100;
    final Ballot ballot;
}
