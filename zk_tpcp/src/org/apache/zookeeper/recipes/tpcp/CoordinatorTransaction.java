package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.List;

/**
 * Transaction viewed by the coordinator
 *
 */
class CoordinatorTransaction extends BaseTransaction 
{
	private List<GroupMember> allowedParticipants;
	
	public CoordinatorTransaction(Serializable query, GroupMember coordinator,
			List<GroupMember> allowedParticipants) 
	{
		super(query, coordinator, allowedParticipants);
		
		this.allowedParticipants = allowedParticipants;
	}
	
	/**
	 * Blocking call to coordinate transaction (begin to end)
	 */
	public void coordinate()
	{
		// TODO coordinate
	}
}
