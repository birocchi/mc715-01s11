package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper.States;

// TODO set watcher on transaction node and call participate
// TODO make facade class for znode operations
// TODO use ACL nicely to improve nodes access
// TODO handler exceptions

/**
 * Transaction group
 * 
 */
public final class TransactionGroup
{
	static final String groupZnode = "g";
	static final String transactionZnode = "t";
	
	private String groupPath;
	private GroupMember me;
	private ZooKeeper zkClient;
	private TransactionGroupWatcher watcher;
	private Hashtable<String, GroupMember> members;
	private ITransactionHandler handler;
	private boolean disposed;
	private long lastTransactionID;
	private Object transactionSyncLock;
	private ExecutorService threadPool;
	
	private TransactionGroup(String groupPath, ZooKeeper zkClient, ITransactionHandler handler)
	{
		this.groupPath = groupPath;
		this.zkClient = zkClient;
		this.watcher = new TransactionGroupWatcher(this);
		this.me = new GroupMember(new Long(zkClient.getSessionId()).toString(), this);
		this.handler = handler;
		this.members = new Hashtable<String, GroupMember>();
		this.disposed = false;
		this.lastTransactionID = -1;
		this.transactionSyncLock = new Object();
		this.threadPool = java.util.concurrent.Executors.newCachedThreadPool();
	}
	
	/**
	 * Verifies if the group path exists and act according
	 * @throws InterruptedException 
	 */
	private void createGroup() throws InterruptedException
	{		
		// verifies if group path exists
		try 
		{
			// if group node does not exists, we create a new one
			if (zkClient.exists(groupPath, false) == null)
			{
				try
				{
					// we need a persistent node so it can have children
					zkClient.create(groupPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				catch(KeeperException zkEx)
				{
					// if someone created before us, just ignore and continue (Code.NODEEXISTS)
					// otherwise, something unexpected occurred
					if (zkEx.code() != Code.NODEEXISTS)
					{
						throw zkEx;
					}
				}
			}
			
			// now we create the group member node
			String groupMemberZnode = groupPath + "/" + TransactionGroup.groupZnode;
			// if group member node does not exists, we create a new one
			if (zkClient.exists(groupMemberZnode, false) == null)
			{
				try
				{
					// we need a persistent node so it can have children
					zkClient.create(groupMemberZnode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				catch(KeeperException zkEx)
				{
					// if someone created before us, just ignore and continue (Code.NODEEXISTS)
					// otherwise, something unexpected occurred
					if (zkEx.code() != Code.NODEEXISTS)
					{
						throw zkEx;
					}
				}
			}
			
			// now we create the transaction node
			String transactionZnode = groupPath + "/" + TransactionGroup.transactionZnode;
			// if transaction node does not exists, we create a new one
			if (zkClient.exists(transactionZnode, false) == null)
			{
				try
				{
					// we need a persistent node so it can have children
					zkClient.create(transactionZnode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				catch(KeeperException zkEx)
				{
					// if someone created before us, just ignore and continue (Code.NODEEXISTS)
					// otherwise, something unexpected occurred
					if (zkEx.code() != Code.NODEEXISTS)
					{
						throw zkEx;
					}
				}
			}
		}
		catch(KeeperException e)
		{
			// TODO: do something
		}
	}
	
	/**
	 * Join a group
	 * @throws GroupException if zkClient is already a member on this group
	 * @throws InterruptedException 
	 */
	private void join() throws GroupException, InterruptedException
	{
		createGroup();
		
		// now that znode exists, let's create our znode member		
		try
		{
			// create our znode under group
			zkClient.create(me.getZnodePath(), null,  ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}
		catch(KeeperException zkEx)
		{
			// if our node already exists, our user does not know how to code,
			// this is an error
			if (zkEx.code() == Code.NODEEXISTS)
				throw new GroupException("This zk client is already a member of group \""+groupPath+"\"");
			else
			{
				// TODO: do something
			}
		}
		
		// get group members
		updateMembers();
		
		// watch new transactions
		updateTransactions();
	}
	
	/**
	 * Update group member list
	 * @throws InterruptedException
	 */
	private void updateMembers() throws InterruptedException
	{
		List<String> membersName = null;
		
		// avoid updating members after we left group
		if (disposed)
			return;
		
		try 
		{
			membersName = zkClient.getChildren(groupPath + "/" + groupZnode, watcher);
		} 
		catch (KeeperException e) 
		{
			// TODO: handle this
			
			// actually we are ignoring member changes when we got an error
			return;
		}
		
		synchronized (this.members) 
		{
			// TODO make this not so stupiditly slow
					
			Enumeration<String> keys = members.keys();

			// remove those members who left the group	
			while (keys.hasMoreElements())
			{
				String m = keys.nextElement();
				
				// if we have some members who isn't on recent member list, we should remove it
				if (!membersName.contains(m))
					members.remove(m);
			}
			
			// add those members we don't have
			for (String m : membersName)
			{
				// if we don't have this member, we should add it
				if (!members.contains(m))
					members.put(m, new GroupMember(m, this));
			}
		}
	}
	
	/**
	 * Update transactions
	 * @throws InterruptedException
	 */
	private void updateTransactions() throws InterruptedException
	{
		// avoid updating transaction after we left group
		if (disposed)
			return;
		
		List<String> transactions = null;
		String fullPathPrefix = groupPath + "/" + transactionZnode;		
		
		try 
		{
			transactions = zkClient.getChildren(fullPathPrefix, watcher);
		} 
		catch (KeeperException e) 
		{
			// TODO: handle this
			
			// actually we are ignoring transactions when we got an error and those should be aborted by timeout (if we must participate)
			return;
		}
		
		// sort transactions
		BaseTransaction.sortTransactionList(transactions);
		fullPathPrefix += "/";
		
		synchronized (this.transactionSyncLock)
		{
			long tid = -1;
			
			// for each transaction
			for (String t : transactions)
			{
				tid = BaseTransaction.getTransactionID(t);
				
				// we try to avoid double reads
				if (tid > lastTransactionID)
				{
					// we can't stall here as we're holding a lock
					participate(fullPathPrefix + t);
				}
			}
			
			// avoid setting transactionID every loop iteration
			if (tid > lastTransactionID)
				lastTransactionID = tid;
		}		
	}
	
	/**
	 * Participate on some ongoing transaction.
	 * @throws InterruptedException 
	 */
	private void participate(String transactionZnode) throws InterruptedException
	{
		// TODO create waiting transaction for execution list to avoid stalling (see updateTransactions) 
		ParticipantTransaction pt = new ParticipantTransaction(transactionZnode, me, handler);		
		
		queueTransaction(pt);
	}
	
	/**
	 * Queue a transaction for execution as soon as possible
	 * @param transaction
	 */
	private void queueTransaction(BaseTransaction transaction)
	{
		synchronized (this.transactionSyncLock)
		{
			// TODO review this call - execute may run on this thread, we'd like to avoid that and queue calls
			threadPool.execute(transaction);
		}
	}
	
	/////////////////////////////
	//// PUBLIC MEMBERS  ////////
	/////////////////////////////

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null || !(obj instanceof TransactionGroup))
			return false;
		
		TransactionGroup tg = (TransactionGroup)obj;
		
		return tg.groupPath.equals(groupPath);
	}
	
	/**
	 * Get this group's znode path.
	 */
	public String getGroupPath()
	{
		return groupPath;
	}
	
	/**
	 * Package visibility zk client
	 * @return
	 */
	ZooKeeper getZkClient()
	{
		return zkClient;
	}
	
	/**
	 * Get a collection of member in this
	 */
	public List<GroupMember> getMembers()
	{
		ArrayList<GroupMember> mlist;
		
		// we have to copy data to avoid anachronical reads
		synchronized (members)
		{		
			mlist = new ArrayList<GroupMember>(members.size());
			
			Enumeration<GroupMember> m = members.elements();
			
			while (m.hasMoreElements())
				mlist.add(m.nextElement());
		}
		
		return mlist;
	}
	
	/**
	 * Causes the zk client to leave this group
	 * @throws InterruptedException 
	 */
	public void leaveGroup() throws InterruptedException
	{
		if (disposed)
			return;
		
		try 
		{
			zkClient.delete(me.getZnodePath(), -1);
		} 
		catch (KeeperException e) 
		{
			// TODO do something
		}
		
		disposed = true;
		zkClient = null;
		watcher.die();
		watcher = null;
		members = null;		
		me = null;
	}
	
	/**
	 * Call for a distributed transaction to be executed on peers - 
	 * you cannot vote as it's assumed you've 'pre-committed'
	 * @param query query object to be executed (may be as simple as a string)
	 * @param allowedParticipants group members allowed to participated the transaction. Use null for an everyone transaction.
	 * @return transaction object for manipulating the query
	 * @throws InterruptedException 
	 */
	public ITransaction BeginTransaction(Serializable query, List<GroupMember> allowedParticipants) throws InterruptedException
	{
		if (allowedParticipants == null)
			allowedParticipants = getMembers();
		
		CoordinatorTransaction ct = new CoordinatorTransaction(query, me, allowedParticipants);
		
		queueTransaction(ct);
				
		return ct;
	}
	
	/**
	 * Joins an existing transaction group or creates a new one.
	 * @param groupPath grouping pathname
	 * @param zkClient a earlier connected zk client
	 * @param handler a transaction handler for executing transactions
	 * @return a TransactionGroup object for managing transactions with
	 * @throws GroupException when zkClient is already a member of groupPath
	 * @throws InterruptedException 
	 */
	public static TransactionGroup joinGroup(String groupPath, 
			ZooKeeper zkClient, ITransactionHandler handler) throws GroupException, InterruptedException
	{	
		TransactionGroup g = new TransactionGroup(groupPath, zkClient, handler);
		
		if (zkClient.getState() != States.CONNECTED)
			throw new InvalidParameterException("zkClient is not connected.");
		
		g.join();
		
		return g;
	}
	
	
	/////////////////////////////
	//// WATCHER  ///////////////
	/////////////////////////////
		
	/**
	 * Watcher for group member's updates
	 *
	 */
	private class TransactionGroupWatcher implements Watcher 
	{
		private TransactionGroup group;
		private boolean die;		
		
		public TransactionGroupWatcher(TransactionGroup group)
		{
			this.group = group;
			this.die = false;
		}
		
		/***
		 * Process group membership events
		 * @param event
		 */
		private void processGroupMembers(WatchedEvent event)
		{
			if (event.getType() == EventType.NodeChildrenChanged)
			{
				try 
				{
					group.updateMembers();
				} 
				catch (InterruptedException e) 
				{
					// TODO: do something - this is critical, we need to reestablish the watcher
				}
			}
		}
		
		/**
		 * Process transaction's events
		 * @param event
		 */
		private void processTransactions(WatchedEvent event)
		{
			try 
			{
				group.updateTransactions();
			} 
			catch (InterruptedException e) 
			{
				// TODO: do something - this is critical, we need to reestablish the watcher
			}
		}
		
		/**
		 * Stop watching group's members changes.
		 */
		public void die()
		{
			this.die = true;
			this.group = null;
		}
		
		@Override
		public void process(WatchedEvent event) 
		{
			if (die || event.getType() == EventType.None)
				return;
			
			String fullPath = event.getPath();
			String path = fullPath.substring(fullPath.lastIndexOf('/') + 1);
			
			if (path.equals(TransactionGroup.groupZnode))
				processGroupMembers(event);
			else if (path.equals(TransactionGroup.transactionZnode))
				processTransactions(event);
			else
			{
				// TODO log programming error? lol
			}
		}

	}
}
