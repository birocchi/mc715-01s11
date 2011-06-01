package org.apache.zookeeper.recipes.tpcp;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Transaction viewed by a participant
 *
 *
 */
class ParticipantTransaction extends BaseTransaction {
    private ITransactionHandler handler;

    public ParticipantTransaction(String zNodeTransaction, GroupMember participant, ITransactionHandler handler) throws InterruptedException {
        super(zNodeTransaction, participant);
        this.handler = handler;
    }

    private boolean canParticipate() {
        boolean canParticipate = false;
        List<String> pList = getParticipants();

        if (pList == null)
            canParticipate = true;
        else {
            String myID = me.getName();

            for (String p : pList)
                if (p.equals(myID)) {
                    canParticipate = true;
                    break;
                }
        }

        if (canParticipate) {
            // TODO: this can be improved with a list of own transactions on TransactionGroup and verify if a transaction is in that list
            // during watcher process - so we don't need to load znode's data

            // we shouldn't participate on our own transaction (those we are coordinator)
            canParticipate = !me.getName().equals(getCoordinatorID());
        }

        return canParticipate;
    }

    private void analyseCoordinatorDecision(String decision) {
        TransactionState coordinatorState = TransactionState.parse(decision);

        if (coordinatorState == TransactionState.COMMITTED) {
            state = TransactionState.COMMITTED;
        }
        else if (coordinatorState == TransactionState.ABORTED) {
            // abort if decision is not commit
            state = TransactionState.ABORTED;
        }
        else {
            // coordinator haven't decided yet, we have to way further
            return;
        }

        // we have finished
        finish();
    }

    private void waitForCoordinatorDecision() throws InterruptedException {
        String coordinatorNodePath = getPath(getCoordinatorID());
        String coordData;

        // gets coordinator decision
        // TODO improve left alone watcher (when we know coordinator have decided)
        coordData = readParticipantNode(coordinatorNodePath, true);

        if (coordData != null) {
            analyseCoordinatorDecision(coordData);
        }
        //else
        // coordinator haven't created his node yet, we should wait

    }

    @Override
    protected void nodeEvent(WatchedEvent event) {
        // the only event we are watching is data change on coordinator's node
        EventType etype = event.getType();

        // finally coordinator created his node or we decided
        if (etype == EventType.NodeDataChanged || etype == EventType.NodeCreated) {
            try {
                waitForCoordinatorDecision();
            }
            catch (InterruptedException e) {
                // TODO do something - this is critical!
            }
        }
        else if (etype == EventType.NodeDeleted) {
            // damn, coordinator died, we should abort
            analyseCoordinatorDecision(TransactionState.ABORTED.toString());
        }
    }

    /**
     * Participate on transaction
     * @throws InterruptedException
     */
    private void participate() throws InterruptedException {
        if (!canParticipate())
            return;

        createParticipantNode();

        // if user commits
        if (handler.execute(BaseTransaction.getTransactionID(zNodePath), getQuery())) {
            // pre-commits a transaction
            try {
                commit(true);

                waitForCoordinatorDecision();
            }
            catch(Exception e) {
                // TODO handle
            }

            // TODO set timeout for coordinator (avoid coordinator "online crash") - this will be trick
            // because we'll have to consider last participant's process time - think carefully
        }
        else {
            try {
                abort();

                // we have finished
                finish();
            }
            catch(Exception e) {
                // TODO handle
            }

            // when we abort, we can just ignore everything else
        }
    }

    private void finish() {
        long id = BaseTransaction.getTransactionID(zNodePath);

        decisionReached();

        handler.result(id, state == TransactionState.COMMITTED);
        handler = null;
    }

    @Override
    public void run() {
        try {
            participate();
        }
        catch (InterruptedException e) {
            // TODO handle this
        }
    }
}
