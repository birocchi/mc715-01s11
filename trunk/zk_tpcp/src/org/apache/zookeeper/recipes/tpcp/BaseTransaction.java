package org.apache.zookeeper.recipes.tpcp;

import java.io.Serializable;
import java.util.List;

/**
 * Base class for defining common transaction operations
 *
 */
abstract class BaseTransaction implements ITransaction
{
	private GroupMember coordinator;
	private String zNodePath;
	
	/**
	 * Coordinator constructor.
	 * @param query
	 * @param coordinator
	 * @param allowedParticipants
	 */
	public BaseTransaction(Serializable query, GroupMember coordinator, 
			List<GroupMember> allowedParticipants)
	{
		this.coordinator = coordinator;
		
		createZnode(allowedParticipants, query);
	}
	
	/**
	 * Creates a transaction object from a znode (participant constructor)
	 * @param zNodePath path to transaction znode
	 */
	public BaseTransaction(String zNodePath)
	{
		readZnode();
	}
	
	private void createZnode(List<GroupMember> allowedParticipants, Serializable query)
	{
		// TODO: create znode from attributes, set znode path
	}
	
	private void readZnode()
	{
		// TODO: read znode and set coordinator
	}
	
	/**
	 * Get allowed participants. Null for anyone allowed.
	 * @return null or a list of allowed participants
	 */
	protected List<GroupMember> getAllowedParticipants()
	{
		// TODO allowed part from znode data (cache data).
		return null;
	}
	
	/**
	 * Get the transaction query defined by the coordinator.
	 * @return query object
	 */
	protected Serializable getQuery()
	{
		// TODO query obj from znode data (cache data).
		return null;
	}
	
	/**
	 * Get transaction coordinator.
	 * @return
	 */
	protected GroupMember getCoordinator()
	{
		return this.coordinator;
	}
	
	public boolean commit()
	{
		boolean committed;
		
		committed = false;
		
		// TODO todo
		
		return committed;
	}
	
	public void rollback()
	{
		// TODO todo
	}
	
	public boolean getResult()
	{
		// TODO todo, can't call before calling commit or rollback
		
		return false;
	}
}
