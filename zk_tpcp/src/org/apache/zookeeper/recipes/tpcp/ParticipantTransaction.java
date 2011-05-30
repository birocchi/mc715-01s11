package org.apache.zookeeper.recipes.tpcp;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * Transaction viewed by a participant
 * 
 *
 */
class ParticipantTransaction extends BaseTransaction
{
	private ITransactionHandler handler;
	
	public ParticipantTransaction(String zNodeTransaction, GroupMember participant, ITransactionHandler handler) throws InterruptedException
	{
		super(zNodeTransaction, participant);
		this.handler = handler;
	}
	
	private boolean canParticipate()
	{
		List<String> pList = getParticipants();
		
		if (pList == null)
			return true;
		
		String myID = me.getName();
		
		for (String p : pList)
			if (p.equals(myID))
				return true;
		
		return false;
	}
	
	private void createParticipantNode() throws InterruptedException
	{
		try
		{
			String participantNodePath = zNodePath + "/" + me.getName();
		
			// creates our znode with no data
			zkClient.create(participantNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			
			this.state = TransactionState.SET;
		}
		catch(KeeperException zkEx)
		{
			// TODO handle this
		}
	}
	
	/**
	 * Participate on transaction
	 * @throws InterruptedException 
	 */
	private void participate() throws InterruptedException
	{
		if (!canParticipate())
			return;

		createParticipantNode();
			
		// if user commits
		if (handler.execute(getQuery()))
		{
			// pre-commits a transaction
			try
			{
				commit(true);
			}
			catch(Exception e)
			{
				// TODO handle
			}
			
			// TODO set timeout for coordinator (avoid coordinator "online crash") - this will be trick
			// because we'll have to consider last participant's process time - think carefully
		}
		else
		{
			try
			{
				rollback();
			}
			catch(Exception e)
			{
				// TODO handle
			}
			
			// when we abort, we can just ignore everything else
		}
	}

	@Override
	public void run()
	{
		try
		{
			participate();
		} 
		catch (InterruptedException e)
		{
			// TODO handle this
		}
	}
}
