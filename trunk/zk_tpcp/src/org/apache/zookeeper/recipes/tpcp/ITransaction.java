package org.apache.zookeeper.recipes.tpcp;

/**
 * Contrat for a distribuited transaction.
 *
 */
public interface ITransaction 
{
	/**
	 * Commits an ongoing transaction.
	 * This is a blocking call.
	 * Committing a transaction does not mean that it will be committed on all peers.
	 * There may be some which will try to abort it, so committing a transaction is just
	 * setting a flag meaning you want to have it committed, thus it will be committed
	 * if everyone agrees on this. 
	 * @return true on commit success, false otherwise.
	 */
	public boolean commit();
	
	/**
	 * Aborts an ongoing transaction.
	 * If anyone aborts the transaction, it will be aborted disregarding the rest votes.
	 */
	public void rollback();
	
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
