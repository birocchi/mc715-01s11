package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper.States;

// TODO set watcher on transaction node and call participate

/**
 * Transaction group
 * 
 */
public final class TransactionGroup
{
	static final String groupZnode = "g/";
	
	private Object syncLock; 
	private String groupPath;
	private GroupMember me;
	private ZooKeeper zkClient;
	private TransactionGroupWatcher watcher;
	private List<GroupMember> members;
	private ITransactionHandler handler;
	
	private TransactionGroup(String groupPath, ZooKeeper zkClient, ITransactionHandler handler)
	{
		this.groupPath = groupPath;
		this.zkClient = zkClient;
		this.watcher = new TransactionGroupWatcher(this);
		this.me = new GroupMember(new Long(zkClient.getSessionId()).toString(), this);
		this.syncLock = new Object();
		this.handler = handler;
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
					zkClient.create(groupPath, null, null, CreateMode.PERSISTENT);
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
			String groupMemberZnode = groupPath + groupZnode;
			// if group member node does not exists, we create a new one
			if (zkClient.exists(groupMemberZnode, false) == null)
			{
				try
				{
					// we need a persistent node so it can have children
					zkClient.create(groupMemberZnode, null, null, CreateMode.PERSISTENT);
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
			zkClient.create(me.getZnodePath(), null, null, CreateMode.EPHEMERAL);
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
	}
	
	/**
	 * Update group member list
	 * @throws InterruptedException
	 */
	private void updateMembers() throws InterruptedException
	{
		List<String> membersName = null;
		
		try 
		{
			membersName = zkClient.getChildren(groupPath, watcher);
		} 
		catch (KeeperException e) 
		{
			// TODO: handle this
			
			// actually we are ignoring member changes when we got an error
			return;
		}
		
		synchronized (this.syncLock) 
		{
			// TODO fazer lista de membros
		//	this.members = members;
		}
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if (obj == null || !(obj instanceof TransactionGroup))
			return false;
		
		TransactionGroup tg = (TransactionGroup)obj;
		
		return tg.groupPath.equals(groupPath);
	}
	
	/**
	 * Participate on some ongoing transaction.
	 */
	private void participate(String transactionZnode)
	{
		ParticipantTransaction pt = new ParticipantTransaction(transactionZnode, handler);		
		pt.participate();
	}
	
	/////////////////////////////
	//// PUBLIC MEMBERS  ////////
	/////////////////////////////
	
	/**
	 * Get this group's znode path.
	 */
	public String getGroupPath()
	{
		return groupPath;
	}
	
	/**
	 * Get a collection of member in this
	 */
	public Iterator<GroupMember> getMembers()
	{
		// TODO todo
		return null;
	}
	
	/**
	 * Causes the zk client to leave this group
	 * @throws InterruptedException 
	 */
	public void leaveGroup() throws InterruptedException
	{
		if (zkClient == null)
			return;
		
		try 
		{
			zkClient.delete(me.getZnodePath(), -1);
		} 
		catch (KeeperException e) 
		{
			// TODO do something
		}
		
		zkClient = null;
		watcher.die();
		watcher = null;
		members = null;		
		me = null;
	}
	
	/**
	 * Call for a distributed transaction to be executed on peers - 
	 * you cannot vote as it's assumed 'commit'
	 * @param query query object to be executed (may be as simple as a string)
	 * @param allowedParticipants group members allowed to participated the transaction. Use null for an everyone transaction.
	 * @return transaction object for manipulating the query
	 */
	public ITransaction BeginTransaction(Serializable query, List<GroupMember> allowedParticipants)
	{
		CoordinatorTransaction ct = new CoordinatorTransaction(query, me, allowedParticipants);
		
		ct.coordinate();
		
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
	public static TransactionGroup joinGroup(String groupPath, ZooKeeper zkClient, ITransactionHandler handler) throws GroupException, InterruptedException
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
			if (die)
				return;
			
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

	}
}
