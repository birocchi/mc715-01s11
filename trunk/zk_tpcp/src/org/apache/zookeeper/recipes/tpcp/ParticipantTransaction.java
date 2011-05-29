package org.apache.zookeeper.recipes.tpcp;

/**
 * Transaction viewed by a participant
 * 
 *
 */
class ParticipantTransaction extends BaseTransaction
{
	private ITransactionHandler handler;
	
	public ParticipantTransaction(String zNodeTransaction, ITransactionHandler handler)
	{
		super(zNodeTransaction);
		this.handler = handler;
	}
	
	private boolean canParticipate()
	{
		// TODO todo
		
		return false;
	}
	
	/**
	 * Participate on transaction
	 */
	public void participate()
	{
		if (!canParticipate())
			return;
			
		if (handler.execute(getQuery()))
			commit();
		else
			rollback();
	}
}
