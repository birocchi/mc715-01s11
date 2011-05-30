package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Base class for defining common transaction operations
 *
 */
abstract class BaseTransaction implements ITransaction, Runnable
{
	private static final List<String> emptyParticipantList = new ArrayList<String>(0);
	private static final String transactionNodePrefix = "t";
	
	protected String zNodePath;
	protected ZooKeeper zkClient;
	protected GroupMember me;
	
	private TransactionData data;
	protected TransactionState state;
	
	/**
	 * Coordinator constructor.
	 * @param query
	 * @param coordinator
	 * @param participants
	 * @param zkClient zookeeper client
	 * @throws InterruptedException 
	 */
	public BaseTransaction(Serializable query, GroupMember coordinator, 
			List<GroupMember> participants) throws InterruptedException
	{
		this.state = TransactionState.PRESET;
		
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
	public BaseTransaction(String zNodePath, GroupMember participant) throws InterruptedException
	{
		this.state = TransactionState.PRESET;
		
		this.me = participant;
		this.zkClient = me.getGroup().getZkClient();
		readZnode();
	}
	
	private void createZnode(List<GroupMember> participants, Serializable query, String coordinatorID) throws InterruptedException
	{
		// compose transaction path
		zNodePath = me.getGroup().getGroupPath() + "/" + TransactionGroup.transactionZnode + "/" + 
			BaseTransaction.transactionNodePrefix;
		
		ArrayList<String> memberList;
		
		if (participants == null)
			memberList = null;
		else
		{
			memberList = new ArrayList<String>(participants.size());
			
			for(GroupMember gm : participants)
				memberList.add(gm.getName());
		}
		
		data = new TransactionData(query, coordinatorID, memberList);		
		
		try
		{			
			// we need a persistent node so it can have children
			zkClient.create(zNodePath, data.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			
			this.state = TransactionState.SET;
		}
		catch(KeeperException zkEx)
		{
			// TODO handle this
		}
	}
	
	private void readZnode() throws InterruptedException
	{
		try
		{
			Stat s = zkClient.exists(zNodePath, false);
			
			if (s == null)
			{
				// it seems transaction was aborted before we could even vote, we can just abort it locally and continue
				// create dummy data to force participant imp. to abort
				this.data = new TransactionData(null, null, emptyParticipantList);
			}
			else
			{
				// TODO revise this code portion : couldn't I use just getData and avoid getting znode stat?
				byte[] zdata = zkClient.getData(zNodePath, false, s);
				
				this.data = TransactionData.readByteArray(zdata);			
			}
			
			this.state = TransactionState.SET;
		}
		catch(KeeperException zkEx)
		{
			// TODO handle this
		}
	}
	
	/**
	 * Writes in my znode data string
	 * @param zNodePath
	 * @param data
	 * @throws InterruptedException
	 */
	private void setMyData(String data) throws InterruptedException
	{
		try
		{
			String myZnode = zNodePath + "/" + me.getName();
			zkClient.setData(myZnode, data.getBytes(), -1);
		} 
		catch (KeeperException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Get allowed participants. Null for anyone allowed.
	 * @return null or a list of allowed participants
	 */
	protected List<String> getParticipants()
	{
		return this.data.getParticipants();
	}
	
	/**
	 * Get the transaction query defined by the coordinator.
	 * @return query object
	 */
	protected Serializable getQuery()
	{
		return this.data.getQuery();
	}
	
	/**
	 * Get transaction coordinator's ID.
	 * @return
	 */
	protected String getCoordinatorID()
	{
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
	protected void commit(boolean preCommit) throws Exception
	{
		if (state == TransactionState.SET)
		{
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
	protected void rollback() throws Exception
	{
		if (state == TransactionState.SET)
		{
			state = TransactionState.ABORTED;
			setMyData(state.toString());
		}
		else
			throw new Exception("Cannot abort. Previous action was taken.");
	}
	
	/**
	 * ITransaction implementation
	 */
	public boolean getResult()
	{	
		// TODO todo, can't call before calling commit or rollback
		
		return false;
	}
	
	/**
	 * Parse transactionID from it's znode name
	 * @param transactionZnode a znode representing a transaction
	 * @return a long representing transactions ID
	 */
	static Long getTransactionID(String transactionZnode)
	{
		String tid;
		int index;
		
		index = transactionZnode.lastIndexOf('/');
		if (index == -1)
			tid = transactionZnode;
		else
			tid = transactionZnode.substring(index+1);
		
		return new Long(tid);
	}
	
	/**
	 * Sort a transaction list.
	 * @param transactionList a list of transaction znodes.
	 */
	static void sortTransactionList(List<String> transactionList)
	{
		// as simple as this ;)
		Collections.sort(transactionList);
	}
}