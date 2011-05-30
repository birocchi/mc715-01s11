package org.apache.zookeeper.recipes.tpcp;

/**
 * Contrat for a distribuited transaction.
 *
 */
public interface ITransaction 
{	
	/**
	 * Blocking call to get transaction result.
	 * @return true for committed, false for aborted
	 */
	public boolean getResult();
	
	/**
	 * Nonblocking call to get transaction result.
	 * @return true for committed, false for aborted
	 */
	// TODO nonblocking call
	//public boolean getResultAsync(Call);
}
