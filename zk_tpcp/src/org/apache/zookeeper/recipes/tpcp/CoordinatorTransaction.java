package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;

/**
 * Transaction viewed by the coordinator
 *
 */
class CoordinatorTransaction extends BaseTransaction 
{
	private List<GroupMember> participants;
	
	public CoordinatorTransaction(Serializable query, GroupMember coordinator,
			List<GroupMember> participants) throws InterruptedException 
	{
		super(query, coordinator, participants);
		
		this.participants = participants;
	}
	
	/**
	 * Blocking call to coordinate transaction (begin to end)
	 */
	private void coordinate() throws InterruptedException
	{
		// TODO coordinate
	}
	
	@Override
	public void run()
	{
		try
		{
			coordinate();
		} 
		catch (InterruptedException e)
		{
			// TODO handle this
		}
	}

	@Override
	protected void nodeEvent(WatchedEvent event)
	{
		// TODO Auto-generated method stub
		
	}
}
