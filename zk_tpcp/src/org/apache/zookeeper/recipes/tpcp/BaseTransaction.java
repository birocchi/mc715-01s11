package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

/**
 * Base class for defining common transaction operations
 *
 */
abstract class BaseTransaction implements ITransaction, Runnable {
    private static final List<String> emptyParticipantList = new ArrayList<String>(0);
    protected static final String transactionNodePrefix = "t";

    private BaseWatcher defaultWatcher;
    private TransactionData data;

    protected String zNodePath;
    protected ZooKeeper zkClient;
    protected GroupMember me;
    protected volatile TransactionState state;

    /**
     * Coordinator constructor.
     * @param query
     * @param coordinator
     * @param participants
     * @param zkClient zookeeper client
     * @throws InterruptedException 
     */
    public BaseTransaction(Serializable query, GroupMember coordinator, 
                           List<GroupMember> participants) throws InterruptedException {
        initialize();

        this.me = coordinator;
        this.zkClient = coordinator.getGroup().getZkClient();
        createZnode(participants, query, coordinator.getName());
    }

    /**
     * Creates a transaction object from a znode (participant constructor)
     * @param zNodePath path to transaction znode
     * @param zkClient zookeeper client
     * @throws InterruptedException 
     */
    public BaseTransaction(String zNodePath, GroupMember participant) throws InterruptedException {
        initialize();

        this.me = participant;
        this.zkClient = me.getGroup().getZkClient();
        this.zNodePath = zNodePath;
        readZnode();
    }

    private void initialize() {
        this.state = TransactionState.PRESET;
        this.defaultWatcher = new BaseWatcher();
    }

    private void createZnode(List<GroupMember> participants, Serializable query, String coordinatorID) throws InterruptedException {
        // compose transaction path
        zNodePath = me.getGroup().getGroupPath() + "/" + TransactionGroup.transactionZnode + "/" + 
            BaseTransaction.transactionNodePrefix;

        ArrayList<String> memberList;

        if (participants == null) {
            memberList = null;
        }
        else {
            memberList = new ArrayList<String>(participants.size());

            for(GroupMember gm : participants) {
                memberList.add(gm.getName());
            }
        }

        data = new TransactionData(query, coordinatorID, memberList);

        try {
            // we need a persistent node so it can have children
            zNodePath = zkClient.create(zNodePath, data.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            // now we create out znode
            createParticipantNode();

            this.state = TransactionState.SET;
        }
        catch(KeeperException zkEx) {
            if(Code.NONODE.equals(zkEx.code())){
                System.out.println("Could not create the znode '" + zNodePath + "', the node doesn't have a parent in the Zookeeper.");
            }
            else if (Code.NODEEXISTS.equals(zkEx.code())){
                System.out.println("Could not create the znode '" + zNodePath + "', the node already exists.");
            }
            else {
                System.out.println("Error while trying to create the znode '" + zNodePath + "'.");
            }
        }
    }

    private void readZnode() throws InterruptedException {
        try {
            Stat s = zkClient.exists(zNodePath, false);

            if (s == null) {
                // it seems transaction was aborted before we could even vote, we can just abort it locally and continue
                // create dummy data to force participant imp. to abort
                this.data = new TransactionData(null, null, emptyParticipantList);
            }
            else {
                // TODO revise this code portion : couldn't I use just getData and avoid getting znode stat?
                byte[] zdata = zkClient.getData(zNodePath, false, s);

                this.data = TransactionData.readByteArray(zdata);
            }

            this.state = TransactionState.SET;
        }
        catch(KeeperException zkEx) {
            if(Code.NONODE.equals(zkEx.code())){
                System.out.println("Could not get the data of the znode '" + zNodePath + "', the node doesn't exist.");
            }
            else {
                System.out.println("Error while processing the znode '" + zNodePath + "'.");
            }
        }
    }

    /**
     * Writes in my znode data string
     * @param zNodePath
     * @param data
     * @throws InterruptedException
     */
    private void setMyData(String data) throws InterruptedException {
        try {
            String myZnode = getPath(me.getName());
            zkClient.setData(myZnode, data.getBytes(), -1);
        } 
        catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get allowed participants. Null for anyone allowed.
     * @return null or a list of allowed participants
     */
    protected List<String> getParticipants() {
        return this.data.getParticipants();
    }

    /**
     * Get the transaction query defined by the coordinator.
     * @return query object
     */
    protected Serializable getQuery() {
        return this.data.getQuery();
    }

    /**
     * Get transaction coordinator's ID.
     * @return
     */
    protected String getCoordinatorID() {
        return this.data.getCoordinatorID();
    }

    /**
     * Commits an ongoing transaction.
     * Committing a transaction does not mean that it will be committed on all peers.
     * There may be some which will try to abort it, so committing a transaction is just
     * setting a flag meaning you want to have it committed, thus it will be committed
     * if everyone agrees on this. 
     * @param preCommit true change state to COMMITTED, false to PRE_COMMITED.
     * @throws Exception when another action (rollback or commit) was previously taken.
     */
    protected void commit(boolean preCommit) throws Exception {
        if (state == TransactionState.SET) {
            if (preCommit)
                state = TransactionState.PRE_COMMITTED;
            else
                state = TransactionState.COMMITTED;

            setMyData(state.toString());
        }
        else
            throw new Exception("Cannot commit. Previous action was taken.");
    }

    /**
     * Aborts an ongoing transaction.
     * If anyone aborts the transaction, it will be aborted disregarding the rest votes.
     * @throws Exception when another action (rollback or commit) was previously taken.
     */
    protected void abort() throws Exception {
        if (state == TransactionState.SET) {
            state = TransactionState.ABORTED;
            setMyData(state.toString());
        }
        else {
            throw new Exception("Cannot abort. Previous action was taken.");
        }
    }

    /**
     * This method is called for events watched with getDefaultWatcher 
     * @param event
     */
    protected abstract void nodeEvent(WatchedEvent event);

    /**
     * Get default watcher.
     * @return
     */
    protected Watcher getDefaultWatcher() {
        return defaultWatcher;
    }

    /**
     * Creates a transaction child node with id equals me.getName()
     * @throws InterruptedException
     */
    protected void createParticipantNode() throws InterruptedException {
        try {
            String participantNodePath = getPath(me.getName());

            // creates our znode with SET state (meaning we are ready)
            zkClient.create(participantNodePath, TransactionState.SET.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        catch(KeeperException zkEx) {
            if(Code.NONODE.equals(zkEx.code())){
                System.out.println("Could not create the participant znode, the node doesn't have a parent in the Zookeeper.");
            }
            else if (Code.NODEEXISTS.equals(zkEx.code())){
                System.out.println("Could not create the participant znode, the node already exists.");
            }
            else {
                System.out.println("Error while trying to create the znode '" + zNodePath + "'.");
            }
        }
    }

    /**
     * Reads a transaction child node data and sets a watch if asked to
     * CAUTION: node may not exists (see returned value)
     * @param childNodePath full path of child node
     * @param watch true to set the default watcher - a watch is set on exists and getData calls.
     * @return read data - null if node does not exists
     * @throws InterruptedException 
     */
    protected String readParticipantNode(String childNodePath, boolean watch) throws InterruptedException {
        String data = null;

        try {
            Stat s = zkClient.exists(childNodePath, false);

            if (s != null) {
                byte[] zdata;

                if (watch) {
                    zdata = zkClient.getData(childNodePath, defaultWatcher, s);
                }
                else {
                    zdata = zkClient.getData(childNodePath, false, s);
                }

                data = new String(zdata);
            }
        }
        catch (KeeperException zkEx) {
            if (Code.NONODE.equals(zkEx.code())) {
                System.out.println("Could not get the data of the znode '" + zNodePath + "', the node doesn't exist.");
            }
            else {
                System.out.println("Error while processing the znode '" + zNodePath + "'.");
            }
        }

        return data;
    }

    /**
     * Return the full path of a transaction participant node
     * @param participantID participantID (nothing more)
     * @return full path to participant node
     */
    protected String getPath(String participantID) {
        return zNodePath + "/" + participantID;
    }

    /**
     * This method should be called when decision on whether commit or abort was reached.
     * CAUTION: this should be your last super call!!!
     */
    protected final void decisionReached() {
        // avoid exception on double call
        if (me == null)
            return;

        // we have finished
        defaultWatcher.die();

        defaultWatcher = null;
        data = null;
        zNodePath = null;
        zkClient = null;
        me = null;

        // wake everyone waiting for this result
        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * ITransaction implementation
     * @throws InterruptedException 
     */
    public boolean getResult() throws InterruptedException {
        synchronized (this) {
            // if decision has not yet been made, wait for it
            if (state != TransactionState.COMMITTED && state != TransactionState.ABORTED)
                wait();
        }

        // if we get here, a decision has been made

        return state == TransactionState.COMMITTED;
    }

    /**
     * Parse transactionID from it's znode name
     * @param transactionZnode a znode representing a transaction
     * @return a long representing transactions ID
     */
    static Long getTransactionID(String transactionZnode) {
        String tid;
        int index;

        // plus 1 to skip initial 't' char
        index = transactionZnode.lastIndexOf('/') + 1;
        tid = transactionZnode.substring(index+1);

        return new Long(tid);
    }

    /**
     * Sort a transaction list.
     * @param transactionList a list of transaction znodes.
     */
    static void sortTransactionList(List<String> transactionList) {
        // as simple as this ;)
        Collections.sort(transactionList);
    }

    /////////////////////////////
    //// WATCHER  ///////////////
    /////////////////////////////

    /**
     * Watcher for data change
     */
    private class BaseWatcher implements Watcher  {
        private volatile boolean die = false;

        public void die() {
            die = true;
        }

        @Override
        public void process(WatchedEvent event) {
            if (die || event.getType() == EventType.None) {
                return;
            }

            nodeEvent(event);
        }
    }
}
