package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    // Your code here...
    private PaxosRequest request;
    private PaxosReply reply;
    private int sequenceNum;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
        request = null;
        reply = null;
        sequenceNum = 0;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // Your code here...
        reply = null;
        sequenceNum++;
        request = new PaxosRequest(new AMOCommand(operation, sequenceNum, this.address()));
        for (Address server : servers) {
            this.send(request, server);
        }
        this.set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // Your code here...
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // Your code here...
        while (reply == null) {
            this.wait();
        }
        return reply.amoResult().result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // Your code here...
        if (m.amoResult().sequenceNum() == sequenceNum) {
            reply = m;
            this.notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // Your code here...
        if (sequenceNum == t.sequenceNum() && reply == null) {
            for (Address server : servers) {
                this.send(request, server);
            }
            set(t, ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }
}
